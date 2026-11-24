-- M2 Stage and Publish Workflow
-- Implements Phase 4 / contracts/sql-history.md
-- Provides two-stage execution:
--   Stage A: TEMP tables, per-batch computation, fault isolation, recursive CTE identity assignment
--   Stage B: Single transaction atomic publish with 4 invariant guards, idempotency and conflict rejection

SET TimeZone = 'UTC';

-- Ensure base tables exist
.read validation/m2/sql/schema.sql

--------------------------------------------------------------------------------
-- 1. Compute canonical ranks for scenario nodes
--------------------------------------------------------------------------------

CREATE OR REPLACE TEMP TABLE stage_nodes_ranked AS
WITH unnested AS (
    SELECT
        c.ordinal,
        s.cluster_id::INTEGER AS cluster_id,
        s.member_count::INTEGER AS member_count,
        s.member_fraction::DOUBLE AS member_fraction,
        s.mean_features::DOUBLE[] AS mean_features,
        s.representative_member_id::VARCHAR AS representative_member_id,
        s.within_cluster_sse::DOUBLE AS within_cluster_sse
    FROM stage_cluster c, unnest(c.result.scenarios) AS u(s)
)
SELECT
    ordinal,
    cluster_id,
    (ROW_NUMBER() OVER (
        PARTITION BY ordinal
        ORDER BY mean_features ASC, cluster_id ASC
    ) - 1)::INTEGER AS canonical_rank,
    mean_features,
    member_count,
    member_fraction,
    representative_member_id,
    within_cluster_sse
FROM unnested;

--------------------------------------------------------------------------------
-- 2. Pair matching across adjacent 'ok' batches
--------------------------------------------------------------------------------

CREATE OR REPLACE TEMP TABLE stage_match_inputs AS
SELECT
    a.ordinal AS prev_ordinal,
    b.ordinal AS curr_ordinal,
    a.dispersion AS prev_dispersion,
    b.dispersion AS curr_dispersion,
    (
        SELECT list(struct_pack(cluster_id := cluster_id, mean_features := mean_features) ORDER BY cluster_id)
        FROM stage_nodes_ranked
        WHERE ordinal = a.ordinal
    ) AS prev_scenarios,
    (
        SELECT list(struct_pack(cluster_id := cluster_id, mean_features := mean_features) ORDER BY cluster_id)
        FROM stage_nodes_ranked
        WHERE ordinal = b.ordinal
    ) AS curr_scenarios
FROM stage_batches a
JOIN stage_batches b ON b.ordinal = a.ordinal + 1
WHERE a.status = 'ok' AND b.status = 'ok';

CREATE OR REPLACE TEMP TABLE stage_match_results AS
SELECT
    prev_ordinal,
    curr_ordinal,
    prev_dispersion,
    curr_dispersion,
    ens_match_scenarios(
        prev_scenarios,
        curr_scenarios,
        getvariable('weights'),
        struct_pack(
            threshold_coefficient := getvariable('threshold_coefficient'),
            prev_dispersion := prev_dispersion,
            curr_dispersion := curr_dispersion
        )
    ) AS match_result
FROM stage_match_inputs;

CREATE OR REPLACE TEMP TABLE stage_links_all AS
SELECT
    r.prev_ordinal,
    l.prev_cluster_id::INTEGER AS prev_cluster_id,
    r.curr_ordinal,
    l.curr_cluster_id::INTEGER AS curr_cluster_id,
    l.distance::DOUBLE AS distance,
    l.distance_ratio::DOUBLE AS distance_ratio,
    l.alternative_status::VARCHAR AS alternative_status,
    l.ambiguous::BOOLEAN AS ambiguous
FROM stage_match_results r, unnest(r.match_result.links) AS u(l);

--------------------------------------------------------------------------------
-- 3. Recursive CTE Identity Inheritance (branch_id)
--------------------------------------------------------------------------------

CREATE OR REPLACE TEMP TABLE stage_identity AS
WITH RECURSIVE root_nodes AS (
    SELECT
        n.ordinal,
        n.cluster_id,
        n.canonical_rank,
        CASE
            WHEN n.ordinal = 0 THEN 'first_batch'
            WHEN prev.status IS DISTINCT FROM 'ok' THEN 'previous_planned_batch_unavailable'
            WHEN EXISTS (
                SELECT 1 FROM stage_match_results r, unnest(r.match_result.unmatched_curr) AS t(u)
                WHERE r.curr_ordinal = n.ordinal AND u.cluster_id = n.cluster_id
                  AND u.reason = 'no_candidate_within_threshold'
            ) THEN 'no_candidate_within_threshold'
            ELSE 'not_selected_in_global_optimum'
        END AS assignment_reason
    FROM stage_nodes_ranked n
    LEFT JOIN stage_batches prev ON prev.ordinal = n.ordinal - 1
    WHERE NOT EXISTS (
        SELECT 1 FROM stage_links_all l
        WHERE l.curr_ordinal = n.ordinal AND l.curr_cluster_id = n.cluster_id
    )
),
chain(ordinal, cluster_id, canonical_rank, branch_id, assignment_reason) AS (
    SELECT
        ordinal,
        cluster_id,
        canonical_rank,
        substring(getvariable('run_key'), 1, 12) || ':b' || lpad(ordinal::VARCHAR, 2, '0') || ':k' || lpad(canonical_rank::VARCHAR, 2, '0') AS branch_id,
        assignment_reason
    FROM root_nodes
  UNION ALL
    SELECT
        l.curr_ordinal,
        l.curr_cluster_id,
        n.canonical_rank,
        c.branch_id,
        CASE WHEN l.ambiguous THEN 'inherited_ambiguous' ELSE 'inherited' END AS assignment_reason
    FROM chain c
    JOIN stage_links_all l
      ON l.prev_ordinal = c.ordinal AND l.prev_cluster_id = c.cluster_id
    JOIN stage_nodes_ranked n
      ON n.ordinal = l.curr_ordinal AND n.cluster_id = l.curr_cluster_id
)
SELECT * FROM chain;

--------------------------------------------------------------------------------
-- 4. Canonical Content Digest (content_sha256)
--------------------------------------------------------------------------------

CREATE OR REPLACE TEMP VIEW stage_content_lines AS
SELECT
    'b' AS kind,
    to_json({
        'o': ordinal,
        'init': init_time,
        'lead': lead_hours,
        'status': status,
        'reason': status_reason,
        'input': input_sha256,
        'om': om_sha256,
        'disp': dispersion,
        'res': cluster_result_sha256
    })::VARCHAR AS line,
    [ordinal, 0] AS k
FROM stage_batches
UNION ALL
SELECT
    'n' AS kind,
    to_json({
        'o': n.ordinal,
        'c': n.cluster_id,
        'rank': n.canonical_rank,
        'mean': n.mean_features,
        'cnt': n.member_count,
        'frac': n.member_fraction,
        'rep': n.representative_member_id,
        'sse': n.within_cluster_sse,
        'branch': i.branch_id,
        'why': i.assignment_reason
    })::VARCHAR,
    [n.ordinal, n.cluster_id]
FROM stage_nodes_ranked n
JOIN stage_identity i USING (ordinal, cluster_id)
UNION ALL
SELECT
    'p' AS kind,
    to_json({
        'p': prev_ordinal,
        'c': curr_ordinal,
        'scale': match_result.pair_scale,
        'thr': match_result.threshold,
        'res': match_result
    })::VARCHAR,
    [prev_ordinal, 0]
FROM stage_match_results
UNION ALL
SELECT
    'l' AS kind,
    to_json({
        'p': prev_ordinal,
        'pc': prev_cluster_id,
        'c': curr_ordinal,
        'cc': curr_cluster_id,
        'd': distance,
        'dr': distance_ratio,
        'st': alternative_status,
        'amb': ambiguous
    })::VARCHAR,
    [prev_ordinal, prev_cluster_id]
FROM stage_links_all;

SET VARIABLE content_sha256 = (
    SELECT sha256(string_agg(kind || ':' || line, chr(10) ORDER BY kind, k))
    FROM stage_content_lines
);

--------------------------------------------------------------------------------
-- 5. Stage B: Single-Transaction Atomic Publish
--------------------------------------------------------------------------------

BEGIN TRANSACTION;

-- Conflict check: same run_key but different content
SELECT CASE
    WHEN EXISTS (
        SELECT 1 FROM tracking_runs
        WHERE run_key = getvariable('run_key')
          AND content_sha256 <> getvariable('content_sha256')
    ) THEN error('RUN_VERSION_CONFLICT run_key=' || getvariable('run_key'))
END AS conflict_guard;

SET VARIABLE already_published = (
    SELECT count(*) > 0 FROM tracking_runs WHERE run_key = getvariable('run_key')
);

-- Insert into tracking_runs
INSERT INTO tracking_runs
SELECT
    getvariable('run_key'),
    getvariable('execution_binding'),
    getvariable('weights'),
    getvariable('planned_batch_count'),
    getvariable('content_sha256')
WHERE NOT getvariable('already_published');

-- Insert into replay_batches
INSERT INTO replay_batches
SELECT
    getvariable('run_key'),
    ordinal,
    init_time,
    lead_hours,
    status,
    status_reason,
    input_sha256,
    om_sha256,
    dispersion,
    cluster_result_sha256
FROM stage_batches
WHERE NOT getvariable('already_published');

-- Insert into scenario_nodes
INSERT INTO scenario_nodes
SELECT
    getvariable('run_key'),
    n.ordinal,
    n.cluster_id,
    n.canonical_rank,
    n.mean_features,
    n.member_count,
    n.member_fraction,
    n.representative_member_id,
    n.within_cluster_sse,
    i.branch_id,
    i.assignment_reason
FROM stage_nodes_ranked n
JOIN stage_identity i USING (ordinal, cluster_id)
WHERE NOT getvariable('already_published');

-- Optional fault injection for atomic rollback verification
SELECT CASE
    WHEN getvariable('inject_failure') THEN error('INJECTED_FAILURE after nodes')
END AS fault_injection_check;

-- Insert into pair_comparisons
INSERT INTO pair_comparisons
SELECT
    getvariable('run_key'),
    prev_ordinal,
    curr_ordinal,
    match_result.pair_scale,
    match_result.threshold,
    match_result
FROM stage_match_results
WHERE NOT getvariable('already_published');

-- Insert into scenario_links
INSERT INTO scenario_links
SELECT
    getvariable('run_key'),
    prev_ordinal,
    prev_cluster_id,
    curr_ordinal,
    curr_cluster_id,
    distance,
    distance_ratio,
    alternative_status,
    ambiguous
FROM stage_links_all
WHERE NOT getvariable('already_published');

--------------------------------------------------------------------------------
-- 6. Four Invariant Guards Before Commit
--------------------------------------------------------------------------------

-- G1: Exact node set (ordinal, cluster_id) matches stage nodes
SELECT CASE
    WHEN (SELECT count(*) FROM scenario_nodes WHERE run_key = getvariable('run_key'))
         <> (SELECT count(*) FROM stage_nodes_ranked)
      OR EXISTS (
          SELECT ordinal, cluster_id FROM stage_nodes_ranked
          EXCEPT
          SELECT ordinal, cluster_id FROM scenario_nodes WHERE run_key = getvariable('run_key')
      )
      OR EXISTS (
          SELECT ordinal, cluster_id FROM scenario_nodes WHERE run_key = getvariable('run_key')
          EXCEPT
          SELECT ordinal, cluster_id FROM stage_nodes_ranked
      )
    THEN error('INVARIANT_VIOLATION: G1 node set mismatch')
END AS guard_g1;

-- G2: No branch resurrection (branch_id must span contiguous ordinals)
SELECT CASE
    WHEN EXISTS (
        SELECT branch_id
        FROM scenario_nodes
        WHERE run_key = getvariable('run_key')
        GROUP BY branch_id
        HAVING max(ordinal) - min(ordinal) + 1 <> count(*)
    ) THEN error('INVARIANT_VIOLATION: G2 branch resurrection detected')
END AS guard_g2;

-- G3: Every adjacent pair of 'ok' batches has a comparison record, and no non-'ok' pair has one
SELECT CASE
    WHEN EXISTS (
        SELECT 1
        FROM replay_batches a
        JOIN replay_batches b
          ON a.run_key = b.run_key AND b.ordinal = a.ordinal + 1
        WHERE a.run_key = getvariable('run_key')
          AND a.status = 'ok' AND b.status = 'ok'
          AND NOT EXISTS (
              SELECT 1 FROM pair_comparisons p
              WHERE p.run_key = a.run_key AND p.prev_ordinal = a.ordinal
          )
    )
    OR EXISTS (
        SELECT 1
        FROM pair_comparisons p
        JOIN replay_batches a
          ON a.run_key = p.run_key AND a.ordinal = p.prev_ordinal
        JOIN replay_batches b
          ON b.run_key = p.run_key AND b.ordinal = p.curr_ordinal
        WHERE p.run_key = getvariable('run_key')
          AND (a.status <> 'ok' OR b.status <> 'ok')
    ) THEN error('INVARIANT_VIOLATION: G3 pair comparison mismatch')
END AS guard_g3;

-- G4: Links connect nodes with identical branch_id
SELECT CASE
    WHEN EXISTS (
        SELECT 1
        FROM scenario_links l
        JOIN scenario_nodes a
          ON a.run_key = l.run_key AND a.ordinal = l.prev_ordinal AND a.cluster_id = l.prev_cluster_id
        JOIN scenario_nodes b
          ON b.run_key = l.run_key AND b.ordinal = l.curr_ordinal AND b.cluster_id = l.curr_cluster_id
        WHERE l.run_key = getvariable('run_key')
          AND a.branch_id <> b.branch_id
    ) THEN error('INVARIANT_VIOLATION: G4 link does not carry identical branch_id')
END AS guard_g4;

COMMIT;
