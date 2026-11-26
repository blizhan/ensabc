-- M3 Two-Stage Staging and Atomic 11-Table Publish Workflow (T027)
-- Implements specs/004-m3-scenario-evolution/contracts/sql-history.md §3 & data-model.md §3.3:
--   Stage A (TEMP tables, autocommit):
--     - Optional raw-member per-batch fault isolation via `ens_diagnose_batch`
--       converting ONLY the 8 whitelisted single-batch algorithm failure codes into
--       `status = 'failed'` with exact `CoreErrorCode`, while rejecting metadata/NULL/
--       dimension/non-finite/numerical/config errors under `.bail on` before Stage B.
--     - Adjacent `ok` pair M2 matching (`ens_match_scenarios`) + recursive CTE `branch_id`
--       inheritance (`<tracking_run_key[:12]>:b<ord:02d>:k<rank:02d>`).
--     - Adjacent `ok` pair M3 evolution (`ens_evolve_scenarios`) with `BackboneAssertion`
--       verification against `match_result`.
--     - Full `0..B-2` transition materialization (`ok` vs `gap`), `gap_interrupted` /
--       `gap_restarted` events strictly disjoint from `disappearance` / `emergence`,
--       `ens_fsum` branch & chain summaries (0-ULP parity with `evolve_chain`), and
--       deterministic `m2_content_sha256` & `m3_content_sha256`.
--   Stage B (`BEGIN TRANSACTION ... COMMIT`):
--     - Idempotent M2 5-table publish with `RUN_VERSION_CONFLICT` guard + G1–G4 guards.
--     - Idempotent M3 6-table publish with `EVOLUTION_RUN_VERSION_CONFLICT` guard,
--       `inject_m3_failure` rollback test hooks, and `E1`–`E8` pre-commit invariant guards.

.bail on

SET TimeZone = 'UTC';

-- Ensure all 11 formal tables (5 M2 + 6 M3) exist
.read validation/m3/sql/schema.sql

-- Normalize optional fault-injection session variables
SET VARIABLE inject_failure = coalesce(getvariable('inject_failure'), false);
SET VARIABLE inject_m3_failure = coalesce(getvariable('inject_m3_failure')::VARCHAR, 'false');

-- ============================================================================
-- Stage A.0: Validate Global Parameters & Optional Raw-Batch Fault Isolation
-- ============================================================================

SELECT CASE
    WHEN getvariable('run_key') IS NULL OR length(getvariable('run_key')::VARCHAR) < 12
    THEN error('INVALID_EVOLUTION_CONFIG: run_key must be at least 12 characters')
    WHEN getvariable('evolution_run_id') IS NULL OR length(trim(getvariable('evolution_run_id')::VARCHAR)) = 0
    THEN error('INVALID_EVOLUTION_CONFIG: evolution_run_id must be non-empty')
    WHEN getvariable('planned_batch_count') IS NULL
      OR getvariable('planned_batch_count')::INTEGER NOT BETWEEN 4 AND 8
    THEN error('INVALID_EVOLUTION_CONFIG: planned_batch_count must be between 4 and 8')
    WHEN getvariable('threshold_coefficient') IS NULL
      OR NOT isfinite(getvariable('threshold_coefficient')::DOUBLE)
      OR getvariable('threshold_coefficient')::DOUBLE < 0.0
    THEN error('INVALID_THRESHOLD: threshold_coefficient must be finite and >= 0.0')
    WHEN getvariable('weights') IS NULL OR len(getvariable('weights')) = 0
    THEN error('EMPTY_FEATURES: spatial weights must not be empty')
    WHEN EXISTS (
        SELECT 1 FROM unnest(getvariable('weights')) AS uw(w)
        WHERE w IS NULL OR NOT isfinite(w::DOUBLE) OR w::DOUBLE < 0.0
    )
    THEN error('INVALID_WEIGHTS: spatial weights must be finite and >= 0.0')
    WHEN ens_fsum(getvariable('weights')) <= 0.0
    THEN error('INVALID_WEIGHTS: sum of spatial weights must be > 0.0')
    ELSE true
END AS global_params_gate;

-- Resolve and validate M3 EvolutionConfig upfront (catches bad config even on 0-ok-pair chains)
CREATE OR REPLACE TEMP TABLE stage_evolve_config_val AS
SELECT
    CASE
        WHEN getvariable('evolve_config') IS NOT NULL THEN (
            getvariable('evolve_config')::STRUCT(
                secondary_rule VARCHAR,
                flow_rule VARCHAR,
                fraction_sum_tolerance DOUBLE,
                high_uncertainty_threshold_ratio DOUBLE,
                high_uncertainty_optimality_ratio DOUBLE,
                high_uncertainty_secondary_ratio DOUBLE
            )
        )
        ELSE struct_pack(
            secondary_rule := coalesce(getvariable('evolution_binding').secondary_rule::VARCHAR, 'nearest_feasible_v1'),
            flow_rule := coalesce(getvariable('evolution_binding').flow_rule::VARCHAR, 'star_min_mass_proportional_v1'),
            fraction_sum_tolerance := coalesce(getvariable('evolution_binding').fraction_sum_tolerance::DOUBLE, 1e-9::DOUBLE),
            high_uncertainty_threshold_ratio := coalesce(getvariable('evolution_binding').high_uncertainty_threshold_ratio::DOUBLE, 0.10::DOUBLE),
            high_uncertainty_optimality_ratio := coalesce(getvariable('evolution_binding').high_uncertainty_optimality_ratio::DOUBLE, 0.10::DOUBLE),
            high_uncertainty_secondary_ratio := coalesce(getvariable('evolution_binding').high_uncertainty_secondary_ratio::DOUBLE, 0.10::DOUBLE)
        )
    END AS cfg;

SELECT CASE
    WHEN cfg.secondary_rule IS NULL OR cfg.flow_rule IS NULL
      OR cfg.fraction_sum_tolerance IS NULL
      OR cfg.high_uncertainty_threshold_ratio IS NULL
      OR cfg.high_uncertainty_optimality_ratio IS NULL
      OR cfg.high_uncertainty_secondary_ratio IS NULL
    THEN error('NULL_INPUT: evolve_config fields must not be NULL')
    WHEN cfg.secondary_rule <> 'nearest_feasible_v1'
    THEN error('INVALID_EVOLUTION_CONFIG: unsupported secondary_rule ' || cfg.secondary_rule)
    WHEN cfg.flow_rule <> 'star_min_mass_proportional_v1'
    THEN error('INVALID_EVOLUTION_CONFIG: unsupported flow_rule ' || cfg.flow_rule)
    WHEN NOT isfinite(cfg.fraction_sum_tolerance)
      OR cfg.fraction_sum_tolerance < 0.0
      OR cfg.fraction_sum_tolerance > 1e-3
    THEN error('INVALID_EVOLUTION_CONFIG: fraction_sum_tolerance out of [0.0, 1e-3]')
    WHEN NOT isfinite(cfg.high_uncertainty_threshold_ratio) OR cfg.high_uncertainty_threshold_ratio < 0.0
      OR NOT isfinite(cfg.high_uncertainty_optimality_ratio) OR cfg.high_uncertainty_optimality_ratio < 0.0
      OR NOT isfinite(cfg.high_uncertainty_secondary_ratio) OR cfg.high_uncertainty_secondary_ratio < 0.0
    THEN error('INVALID_EVOLUTION_CONFIG: high_uncertainty ratios must be finite and >= 0.0')
    ELSE true
END AS evolve_config_gate
FROM stage_evolve_config_val;

-- Optional raw-member batch staging table (used by real_8run_evolution.sql & raw-task tests)
CREATE TEMP TABLE IF NOT EXISTS stage_raw_batches (
    ordinal       INTEGER,
    init_time     TIMESTAMP,
    lead_hours    INTEGER,
    raw_status    VARCHAR,
    status_reason VARCHAR,
    input_sha256  VARCHAR,
    om_sha256     VARCHAR,
    members       STRUCT(member_id VARCHAR, features DOUBLE[])[]
);

SET VARIABLE has_raw_batches = (SELECT count(*) > 0 FROM stage_raw_batches);

-- Pre-flight non-whitelisted input/metadata/config gate on stage_raw_batches (runs under .bail on!)
SELECT CASE
    WHEN NOT getvariable('has_raw_batches') THEN true
    WHEN (SELECT count(*) FROM stage_raw_batches) <> getvariable('planned_batch_count')
      OR (SELECT count(DISTINCT ordinal) FROM stage_raw_batches) <> getvariable('planned_batch_count')
      OR (SELECT min(ordinal) FROM stage_raw_batches) <> 0
      OR (SELECT max(ordinal) FROM stage_raw_batches) <> getvariable('planned_batch_count') - 1
    THEN error('INVALID_BATCH_METADATA: stage_raw_batches ordinals must form 0..planned_batch_count-1')
    WHEN EXISTS (
        SELECT 1 FROM stage_raw_batches
        WHERE init_time IS NULL OR lead_hours IS NULL
           OR raw_status IS NULL OR raw_status NOT IN ('present', 'missing', 'failed')
    )
    THEN error('INVALID_BATCH_METADATA: NULL or invalid raw_status/init_time/lead_hours in stage_raw_batches')
    WHEN EXISTS (
        SELECT 1 FROM (
            SELECT init_time, lag(init_time) OVER (ORDER BY ordinal) AS prev_init
            FROM stage_raw_batches
        ) WHERE prev_init IS NOT NULL AND init_time <= prev_init
    )
    THEN error('INVALID_BATCH_METADATA: init_time must strictly increase with ordinal')
    WHEN (SELECT count(DISTINCT (init_time + to_hours(lead_hours))) FROM stage_raw_batches) > 1
    THEN error('INVALID_BATCH_METADATA: init_time + lead_hours valid_time mismatch across planned batches')
    WHEN EXISTS (
        SELECT 1 FROM stage_raw_batches
        WHERE raw_status = 'missing' AND (status_reason IS NULL OR length(trim(status_reason)) = 0)
    )
    THEN error('INVALID_BATCH_METADATA: missing batch requires non-empty status_reason')
    WHEN EXISTS (
        SELECT 1 FROM stage_raw_batches
        WHERE raw_status = 'failed'
          AND (status_reason IS NULL OR NOT regexp_matches(
              status_reason,
              '^(INSUFFICIENT_RANK|AMBIGUOUS_SUBSPACE|EMPTY_CLUSTER|NOT_CONVERGED|INSUFFICIENT_DISTINCT_VECTORS|INSUFFICIENT_PROJECTED_VECTORS|SVD_NOT_CONVERGED|ZERO_DISPERSION)([^A-Za-z0-9_]|$)'
          ))
    )
    THEN error('NON_WHITELISTED_BATCH_FAILURE: pre-marked failed batch carries non-whitelisted code')
    ELSE true
END AS raw_metadata_gate;

-- Validate cluster_config & member vectors on 'present' raw batches BEFORE .bail off
SET VARIABLE cluster_config_provided = (getvariable('cluster_config') IS NOT NULL);
SET VARIABLE cluster_config_check = CASE
    WHEN getvariable('cluster_config') IS NOT NULL THEN (
        struct_pack(
            projection_dim := getvariable('cluster_config').projection_dim::BIGINT,
            cluster_count := getvariable('cluster_config').cluster_count::BIGINT,
            initialization := getvariable('cluster_config').initialization::VARCHAR,
            seed := getvariable('cluster_config').seed::BIGINT,
            restarts := getvariable('cluster_config').restarts::BIGINT,
            initial_centers := CASE
                WHEN getvariable('cluster_config').initial_centers IS NULL THEN NULL::DOUBLE[][][]
                WHEN len(getvariable('cluster_config').initial_centers) = 0 THEN []::DOUBLE[][][]
                ELSE getvariable('cluster_config').initial_centers::DOUBLE[][][]
            END,
            stopping_rule := getvariable('cluster_config').stopping_rule::VARCHAR,
            tolerance := getvariable('cluster_config').tolerance::DOUBLE,
            max_iterations := getvariable('cluster_config').max_iterations::BIGINT,
            rank_threshold := getvariable('cluster_config').rank_threshold::DOUBLE
        )
    )
    ELSE {
        'projection_dim': 2::BIGINT,
        'cluster_count': 2::BIGINT,
        'initialization': 'kmeans++_chacha8_v1'::VARCHAR,
        'seed': 0::BIGINT,
        'restarts': 1::BIGINT,
        'initial_centers': []::DOUBLE[][][],
        'stopping_rule': 'sklearn_variance_scaled_v1'::VARCHAR,
        'tolerance': 1e-4::DOUBLE,
        'max_iterations': 10::BIGINT,
        'rank_threshold': 1e-10::DOUBLE
    }
END;

SELECT CASE
    WHEN NOT getvariable('has_raw_batches')
      OR NOT EXISTS (SELECT 1 FROM stage_raw_batches WHERE raw_status = 'present')
    THEN true
    WHEN NOT getvariable('cluster_config_provided')
      OR getvariable('cluster_config_check').projection_dim IS NULL
      OR getvariable('cluster_config_check').cluster_count IS NULL
      OR getvariable('cluster_config_check').initialization IS NULL
      OR getvariable('cluster_config_check').seed IS NULL
      OR getvariable('cluster_config_check').restarts IS NULL
      OR getvariable('cluster_config_check').initial_centers IS NULL
      OR getvariable('cluster_config_check').stopping_rule IS NULL
      OR getvariable('cluster_config_check').tolerance IS NULL
      OR getvariable('cluster_config_check').max_iterations IS NULL
      OR getvariable('cluster_config_check').rank_threshold IS NULL
    THEN error('NULL_INPUT: cluster_config or one of its fields is NULL')
    WHEN getvariable('cluster_config_check').projection_dim < 1
      OR getvariable('cluster_config_check').cluster_count < 2
      OR getvariable('cluster_config_check').restarts < 1
      OR getvariable('cluster_config_check').max_iterations < 1
      OR getvariable('cluster_config_check').seed < 0
      OR NOT isfinite(getvariable('cluster_config_check').tolerance::DOUBLE)
      OR getvariable('cluster_config_check').tolerance::DOUBLE < 0.0
      OR NOT isfinite(getvariable('cluster_config_check').rank_threshold::DOUBLE)
      OR getvariable('cluster_config_check').rank_threshold::DOUBLE <= 0.0
      OR getvariable('cluster_config_check').initialization NOT IN ('kmeans++_chacha8_v1', 'explicit_centers_v1')
      OR getvariable('cluster_config_check').stopping_rule <> 'sklearn_variance_scaled_v1'
      OR (getvariable('cluster_config_check').initialization = 'kmeans++_chacha8_v1'
          AND len(getvariable('cluster_config_check').initial_centers) <> 0)
      OR (getvariable('cluster_config_check').initialization = 'explicit_centers_v1'
          AND getvariable('cluster_config_check').seed <> 0)
    THEN error('INVALID_CONFIG: cluster_config failed pre-flight validation')
    WHEN getvariable('cluster_config_check').initialization = 'explicit_centers_v1'
      AND (
          len(getvariable('cluster_config_check').initial_centers) <> getvariable('cluster_config_check').restarts
          OR EXISTS (
              SELECT 1
              FROM unnest(coalesce(getvariable('cluster_config_check').initial_centers::DOUBLE[][][], []::DOUBLE[][][])) AS ur(rc)
              WHERE rc IS NULL OR len(rc) <> getvariable('cluster_config_check').cluster_count
          )
          OR EXISTS (
              SELECT 1
              FROM unnest(coalesce(getvariable('cluster_config_check').initial_centers::DOUBLE[][][], []::DOUBLE[][][])) AS ur(rc),
                   unnest(rc) AS uc(cc)
              WHERE cc IS NULL OR len(cc) <> getvariable('cluster_config_check').projection_dim
          )
          OR EXISTS (
              SELECT 1
              FROM unnest(coalesce(getvariable('cluster_config_check').initial_centers::DOUBLE[][][], []::DOUBLE[][][])) AS ur(rc),
                   unnest(rc) AS uc(cc),
                   unnest(cc) AS ucoord(v)
              WHERE v IS NULL OR NOT isfinite(v::DOUBLE)
          )
      )
    THEN error('INVALID_INITIAL_CENTERS: explicit_centers_v1 shape or coordinate invalid')
    WHEN EXISTS (
        SELECT 1 FROM stage_raw_batches WHERE raw_status = 'present' AND members IS NULL
    )
    THEN error('NULL_INPUT: members list is NULL on present batch')
    WHEN EXISTS (
        SELECT 1 FROM stage_raw_batches WHERE raw_status = 'present' AND len(members) = 0
    )
    THEN error('EMPTY_MEMBERS: members list is empty on present batch')
    WHEN EXISTS (
        SELECT 1 FROM stage_raw_batches
        WHERE raw_status = 'present' AND getvariable('cluster_config_check').cluster_count > len(members)
    )
    THEN error('INVALID_CONFIG: cluster_count exceeds member count on present batch')
    WHEN EXISTS (
        SELECT 1
        FROM stage_raw_batches r, unnest(r.members) AS um(m)
        WHERE r.raw_status = 'present'
          AND (m IS NULL OR m.member_id IS NULL OR m.features IS NULL)
    )
    THEN error('NULL_INPUT: nested member struct, member_id, or features is NULL')
    WHEN EXISTS (
        SELECT 1
        FROM stage_raw_batches r, unnest(r.members) AS um(m)
        WHERE r.raw_status = 'present' AND length(m.member_id) = 0
    )
    THEN error('EMPTY_MEMBER_ID: member_id is empty on present batch')
    WHEN EXISTS (
        SELECT 1
        FROM (
            SELECT r.ordinal, count(*) AS total_m, count(DISTINCT m.member_id) AS dist_m
            FROM stage_raw_batches r, unnest(r.members) AS um(m)
            WHERE r.raw_status = 'present'
            GROUP BY r.ordinal
        ) WHERE dist_m <> total_m
    )
    THEN error('DUPLICATE_MEMBER_ID: duplicate member_id on present batch')
    WHEN EXISTS (
        SELECT 1
        FROM stage_raw_batches r, unnest(r.members) AS um(m)
        WHERE r.raw_status = 'present' AND len(m.features) = 0
    )
    THEN error('EMPTY_FEATURES: member features list is empty on present batch')
    WHEN EXISTS (
        SELECT 1
        FROM (
            SELECT r.ordinal, min(len(m.features)) AS min_f, max(len(m.features)) AS max_f
            FROM stage_raw_batches r, unnest(r.members) AS um(m)
            WHERE r.raw_status = 'present'
            GROUP BY r.ordinal
        ) WHERE min_f <> max_f
    )
    THEN error('RAGGED_FEATURES: member feature lengths differ within batch')
    WHEN EXISTS (
        SELECT 1
        FROM stage_raw_batches r, unnest(r.members) AS um(m)
        WHERE r.raw_status = 'present' AND len(m.features) <> len(getvariable('weights'))
    )
    THEN error('DIMENSION_MISMATCH: member feature length does not match spatial weights length')
    WHEN EXISTS (
        SELECT 1
        FROM stage_raw_batches r, unnest(r.members) AS um(m), unnest(m.features) AS uf(val)
        WHERE r.raw_status = 'present' AND val IS NULL
    )
    THEN error('NULL_INPUT: nested feature coordinate is NULL')
    WHEN EXISTS (
        SELECT 1
        FROM stage_raw_batches r, unnest(r.members) AS um(m), unnest(m.features) AS uf(val)
        WHERE r.raw_status = 'present' AND NOT isfinite(val::DOUBLE)
    )
    THEN error('NON_FINITE_INPUT: non-finite feature value on present batch')
    WHEN EXISTS (
        SELECT 1
        FROM (
            SELECT r.ordinal, sum((val::DOUBLE) * (val::DOUBLE)) AS sq_sum
            FROM stage_raw_batches r, unnest(r.members) AS um(m), unnest(m.features) AS uf(val)
            WHERE r.raw_status = 'present'
            GROUP BY r.ordinal
        ) WHERE NOT isfinite(sq_sum)
    )
    THEN error('NUMERICAL_FAILURE: squared feature magnitude overflow on present batch')
    ELSE true
END AS raw_members_gate;

-- Execute per-batch diagnosis, dispersion, and clustering via `ens_diagnose_batch`
CREATE OR REPLACE TEMP TABLE stage_raw_diag (
    ordinal INTEGER,
    diag    VARCHAR
);

INSERT INTO stage_raw_diag
SELECT
    ordinal,
    ens_diagnose_batch(members, getvariable('weights'), getvariable('cluster_config_check')) AS diag
FROM stage_raw_batches
WHERE getvariable('has_raw_batches') AND raw_status = 'present'
ORDER BY ordinal ASC;

CREATE OR REPLACE TEMP TABLE stage_raw_disp_ok (
    ordinal    INTEGER PRIMARY KEY,
    dispersion DOUBLE NOT NULL
);

CREATE OR REPLACE TEMP TABLE stage_raw_cluster_ok AS
SELECT
    0::INTEGER AS ordinal,
    ens_cluster(
        [{'member_id': 'm0', 'features': [0.0, 1.0]},
         {'member_id': 'm1', 'features': [1.0, 0.0]},
         {'member_id': 'm2', 'features': [2.0, 3.0]}],
        {'projection_dim': 2, 'cluster_count': 2, 'initialization': 'kmeans++_chacha8_v1',
         'seed': 0, 'restarts': 1, 'initial_centers': []::DOUBLE[][][],
         'stopping_rule': 'sklearn_variance_scaled_v1', 'tolerance': 1e-4,
         'max_iterations': 10, 'rank_threshold': 1e-10}
    ) AS result
WHERE false;

INSERT INTO stage_raw_disp_ok
SELECT
    r.ordinal,
    CASE WHEN dg.diag = 'OK' THEN ens_dispersion(r.members, getvariable('weights')) END AS dispersion
FROM stage_raw_batches r
JOIN stage_raw_diag dg USING (ordinal)
WHERE getvariable('has_raw_batches') AND r.raw_status = 'present' AND dg.diag = 'OK'
ORDER BY r.ordinal ASC;

INSERT INTO stage_raw_cluster_ok
SELECT
    r.ordinal,
    CASE WHEN dg.diag = 'OK' THEN ens_cluster(r.members, getvariable('cluster_config_check')) END AS result
FROM stage_raw_batches r
JOIN stage_raw_diag dg USING (ordinal)
WHERE getvariable('has_raw_batches') AND r.raw_status = 'present' AND dg.diag = 'OK'
ORDER BY r.ordinal ASC;

-- Materialize stage_batches and stage_cluster from stage_raw_batches when has_raw_batches is true
CREATE TEMP TABLE IF NOT EXISTS stage_batches (
    ordinal               INTEGER,
    init_time             TIMESTAMP,
    lead_hours            INTEGER,
    status                VARCHAR,
    status_reason         VARCHAR,
    input_sha256          VARCHAR,
    om_sha256             VARCHAR,
    dispersion            DOUBLE,
    cluster_result_sha256 VARCHAR
);

CREATE TEMP TABLE IF NOT EXISTS stage_cluster AS
SELECT ordinal, result FROM stage_raw_cluster_ok WHERE false;

DELETE FROM stage_batches WHERE getvariable('has_raw_batches');
INSERT INTO stage_batches
SELECT
    r.ordinal,
    r.init_time,
    r.lead_hours,
    CASE
        WHEN r.raw_status = 'missing' THEN 'missing'
        WHEN r.raw_status = 'failed' THEN 'failed'
        WHEN dg.diag = 'OK' THEN 'ok'
        ELSE 'failed'
    END AS status,
    CASE
        WHEN r.raw_status = 'missing' THEN r.status_reason
        WHEN r.raw_status = 'failed' THEN r.status_reason
        WHEN dg.diag = 'OK' THEN NULL
        ELSE dg.diag
    END AS status_reason,
    CASE WHEN r.raw_status = 'missing' THEN NULL ELSE r.input_sha256 END AS input_sha256,
    CASE WHEN r.raw_status = 'missing' THEN NULL ELSE r.om_sha256 END AS om_sha256,
    CASE WHEN r.raw_status = 'present' AND dg.diag = 'OK' THEN d.dispersion ELSE NULL END AS dispersion,
    CASE WHEN r.raw_status = 'present' AND dg.diag = 'OK' THEN sha256(to_json(c.result)::VARCHAR) ELSE NULL END AS cluster_result_sha256
FROM stage_raw_batches r
LEFT JOIN stage_raw_diag dg USING (ordinal)
LEFT JOIN stage_raw_disp_ok d USING (ordinal)
LEFT JOIN stage_raw_cluster_ok c USING (ordinal)
WHERE getvariable('has_raw_batches')
ORDER BY r.ordinal;

DELETE FROM stage_cluster WHERE getvariable('has_raw_batches');
INSERT INTO stage_cluster
SELECT c.ordinal, c.result
FROM stage_raw_cluster_ok c
JOIN stage_raw_disp_ok d USING (ordinal)
WHERE getvariable('has_raw_batches')
ORDER BY c.ordinal;

-- ============================================================================
-- Stage A.1: Validate Staged Batches & Compute Canonical Scenario Ranks
-- ============================================================================

SELECT CASE
    WHEN (SELECT count(*) FROM stage_batches) <> getvariable('planned_batch_count')
      OR (SELECT count(DISTINCT ordinal) FROM stage_batches) <> getvariable('planned_batch_count')
      OR (SELECT min(ordinal) FROM stage_batches) <> 0
      OR (SELECT max(ordinal) FROM stage_batches) <> getvariable('planned_batch_count') - 1
    THEN error('INVALID_BATCH_METADATA: stage_batches must cover ordinals 0..planned_batch_count-1')
    WHEN EXISTS (
        SELECT 1 FROM stage_batches
        WHERE init_time IS NULL OR lead_hours IS NULL OR status NOT IN ('ok', 'missing', 'failed')
    )
    THEN error('INVALID_BATCH_METADATA: invalid status/init_time/lead_hours in stage_batches')
    WHEN EXISTS (
        SELECT 1 FROM (
            SELECT init_time, lag(init_time) OVER (ORDER BY ordinal) AS prev_init
            FROM stage_batches
        ) WHERE prev_init IS NOT NULL AND init_time <= prev_init
    )
    THEN error('INVALID_BATCH_METADATA: stage_batches init_time must strictly increase')
    WHEN EXISTS (
        SELECT 1 FROM stage_batches
        WHERE status = 'ok' AND (dispersion IS NULL OR NOT isfinite(dispersion) OR dispersion <= 0.0)
    )
    THEN error('ZERO_DISPERSION: ok batch must carry finite dispersion > 0.0')
    WHEN EXISTS (
        SELECT 1 FROM stage_batches
        WHERE status <> 'ok' AND (dispersion IS NOT NULL OR status_reason IS NULL OR length(trim(status_reason)) = 0)
    )
    THEN error('INVALID_BATCH_METADATA: non-ok batch must have NULL dispersion and non-empty status_reason')
    WHEN EXISTS (
        SELECT 1 FROM stage_batches
        WHERE status = 'failed'
          AND (status_reason IS NULL OR NOT regexp_matches(
              status_reason,
              '^(INSUFFICIENT_RANK|AMBIGUOUS_SUBSPACE|EMPTY_CLUSTER|NOT_CONVERGED|INSUFFICIENT_DISTINCT_VECTORS|INSUFFICIENT_PROJECTED_VECTORS|SVD_NOT_CONVERGED|ZERO_DISPERSION)([^A-Za-z0-9_]|$)'
          ))
    )
    THEN error('NON_WHITELISTED_BATCH_FAILURE: ' || coalesce((
        SELECT status_reason FROM stage_batches
        WHERE status = 'failed'
          AND (status_reason IS NULL OR NOT regexp_matches(
              status_reason,
              '^(INSUFFICIENT_RANK|AMBIGUOUS_SUBSPACE|EMPTY_CLUSTER|NOT_CONVERGED|INSUFFICIENT_DISTINCT_VECTORS|INSUFFICIENT_PROJECTED_VECTORS|SVD_NOT_CONVERGED|ZERO_DISPERSION)([^A-Za-z0-9_]|$)'
          ))
        ORDER BY ordinal ASC LIMIT 1
    ), 'NULL'))
    WHEN EXISTS (
        SELECT 1 FROM stage_batches b
        WHERE b.status = 'ok' AND NOT EXISTS (SELECT 1 FROM stage_cluster c WHERE c.ordinal = b.ordinal)
    )
      OR EXISTS (
        SELECT 1 FROM stage_cluster c
        LEFT JOIN stage_batches b USING (ordinal)
        WHERE b.status IS DISTINCT FROM 'ok'
    )
    THEN error('INVALID_BATCH_METADATA: stage_cluster ordinals must match ok batches in stage_batches')
    ELSE true
END AS staged_batches_gate;

CREATE OR REPLACE TEMP TABLE stage_nodes_ranked AS
WITH unnested AS (
    SELECT
        c.ordinal,
        s.cluster_id::INTEGER AS cluster_id,
        s.member_count::INTEGER AS member_count,
        CASE WHEN s.member_fraction::DOUBLE = 0.0 THEN 0.0::DOUBLE ELSE s.member_fraction::DOUBLE END AS member_fraction,
        list_transform(
            s.mean_features::DOUBLE[],
            lambda x: CASE WHEN x = 0.0 THEN 0.0::DOUBLE ELSE x END
        ) AS mean_features,
        s.representative_member_id::VARCHAR AS representative_member_id,
        CASE WHEN s.within_cluster_sse::DOUBLE = 0.0 THEN 0.0::DOUBLE ELSE s.within_cluster_sse::DOUBLE END AS within_cluster_sse
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

-- Standalone per-ok-batch scenario validation (enforces FR-012 even on isolated ok batches between gaps)
SELECT CASE
    WHEN EXISTS (
        SELECT 1 FROM stage_batches b
        WHERE b.status = 'ok'
          AND (SELECT count(*) FROM stage_nodes_ranked n WHERE n.ordinal = b.ordinal) = 0
    )
    THEN error('EMPTY_SCENARIOS: ok batch produced 0 scenarios')
    WHEN EXISTS (
        SELECT 1 FROM (
            SELECT ordinal, count(*) AS cnt FROM stage_nodes_ranked GROUP BY ordinal
        ) WHERE cnt > 12
    )
    THEN error('TOO_MANY_SCENARIOS: scenario count exceeds maximum supported limit of 12')
    WHEN EXISTS (
        SELECT 1 FROM stage_nodes_ranked WHERE cluster_id IS NULL OR cluster_id < 0
    )
    THEN error('INVALID_SCENARIO_ID: cluster_id must be non-negative')
    WHEN EXISTS (
        SELECT 1 FROM (
            SELECT ordinal, count(*) AS total_c, count(DISTINCT cluster_id) AS dist_c
            FROM stage_nodes_ranked GROUP BY ordinal
        ) WHERE dist_c <> total_c
    )
    THEN error('DUPLICATE_SCENARIO_ID: duplicate cluster_id within batch')
    WHEN EXISTS (
        SELECT 1 FROM stage_nodes_ranked WHERE mean_features IS NULL OR len(mean_features) = 0
    )
    THEN error('EMPTY_FEATURES: scenario mean_features must not be empty')
    WHEN EXISTS (
        SELECT 1 FROM stage_nodes_ranked n, unnest(n.mean_features) AS uf(v)
        WHERE v IS NULL OR NOT isfinite(v)
    )
    THEN error('NON_FINITE_INPUT: scenario mean_features contains NULL or non-finite value')
    WHEN EXISTS (
        SELECT 1 FROM stage_nodes_ranked WHERE len(mean_features) <> len(getvariable('weights'))
    )
    THEN error('DIMENSION_MISMATCH: scenario mean_features length does not match weights length')
    WHEN EXISTS (
        SELECT 1 FROM stage_nodes_ranked WHERE member_count IS NULL OR member_count < 1
    )
    THEN error('ZERO_MEMBER_SCENARIO: scenario member_count must be >= 1')
    WHEN EXISTS (
        SELECT 1 FROM stage_nodes_ranked
        WHERE member_fraction IS NULL OR NOT isfinite(member_fraction)
           OR member_fraction <= 0.0 OR member_fraction > 1.0
    )
    THEN error('INVALID_MEMBER_FRACTION: member_fraction must be finite and in (0.0, 1.0]')
    WHEN EXISTS (
        SELECT 1 FROM (
            SELECT
                ordinal,
                ens_fsum(list(member_fraction ORDER BY cluster_id ASC)) AS f_sum
            FROM stage_nodes_ranked
            GROUP BY ordinal
        ) t, stage_evolve_config_val c
        WHERE abs(t.f_sum - 1.0) > c.cfg.fraction_sum_tolerance
    )
    THEN error('INVALID_FRACTION_SUM: batch fsum(member_fraction) deviates from 1.0 beyond tolerance')
    ELSE true
END AS standalone_ok_batch_gate;

-- ============================================================================
-- Stage A.2: M2 Pair Matching & Recursive CTE Persistent Identity (`branch_id`)
-- ============================================================================

CREATE OR REPLACE TEMP TABLE stage_match_inputs AS
SELECT
    a.ordinal AS prev_ordinal,
    b.ordinal AS curr_ordinal,
    a.dispersion AS prev_dispersion,
    b.dispersion AS curr_dispersion,
    (
        SELECT list(struct_pack(cluster_id := cluster_id, mean_features := mean_features) ORDER BY cluster_id ASC)
        FROM stage_nodes_ranked
        WHERE ordinal = a.ordinal
    ) AS prev_scenarios,
    (
        SELECT list(struct_pack(cluster_id := cluster_id, mean_features := mean_features) ORDER BY cluster_id ASC)
        FROM stage_nodes_ranked
        WHERE ordinal = b.ordinal
    ) AS curr_scenarios
FROM stage_batches a
JOIN stage_batches b ON b.ordinal = a.ordinal + 1
WHERE a.status = 'ok' AND b.status = 'ok'
ORDER BY a.ordinal ASC;

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
            threshold_coefficient := getvariable('threshold_coefficient')::DOUBLE,
            prev_dispersion := prev_dispersion,
            curr_dispersion := curr_dispersion
        )
    ) AS match_result
FROM stage_match_inputs
ORDER BY prev_ordinal ASC;

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
FROM stage_match_results r, unnest(r.match_result.links) AS u(l)
ORDER BY r.prev_ordinal ASC, l.prev_cluster_id ASC;

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

-- M2-identical content_sha256 (guarantees cross-compatibility with M2 tracking_runs)
CREATE OR REPLACE TEMP VIEW stage_m2_content_lines AS
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
    FROM stage_m2_content_lines
);

-- ============================================================================
-- Stage A.3: M3 Multi-to-Multi Pair Evolution (`ens_evolve_scenarios`)
-- ============================================================================

CREATE OR REPLACE TEMP TABLE stage_evolve_inputs AS
SELECT
    m.prev_ordinal,
    m.curr_ordinal,
    (
        SELECT list(
            struct_pack(
                cluster_id := cluster_id::BIGINT,
                member_count := member_count::BIGINT,
                member_fraction := member_fraction::DOUBLE,
                mean_features := mean_features::DOUBLE[]
            )
            ORDER BY cluster_id ASC
        )
        FROM stage_nodes_ranked
        WHERE ordinal = m.prev_ordinal
    ) AS prev_scenarios,
    (
        SELECT list(
            struct_pack(
                cluster_id := cluster_id::BIGINT,
                member_count := member_count::BIGINT,
                member_fraction := member_fraction::DOUBLE,
                mean_features := mean_features::DOUBLE[]
            )
            ORDER BY cluster_id ASC
        )
        FROM stage_nodes_ranked
        WHERE ordinal = m.curr_ordinal
    ) AS curr_scenarios,
    m.match_result
FROM stage_match_results m
ORDER BY m.prev_ordinal ASC;

CREATE OR REPLACE TEMP TABLE stage_evolve_results AS
SELECT
    prev_ordinal,
    curr_ordinal,
    ens_evolve_scenarios(
        prev_scenarios,
        curr_scenarios,
        getvariable('weights'),
        match_result,
        (SELECT cfg FROM stage_evolve_config_val)
    ) AS evolve_result
FROM stage_evolve_inputs
ORDER BY prev_ordinal ASC;

-- ============================================================================
-- Stage A.4: Materialize M3 Nodes, Transitions, Edges, and Events
-- ============================================================================

CREATE OR REPLACE TEMP TABLE stage_curr_node_evo AS
SELECT
    er.curr_ordinal AS ordinal,
    cn.cluster_id::INTEGER AS cluster_id,
    cn.in_degree_primary::INTEGER AS in_degree_primary,
    cn.in_degree_secondary::INTEGER AS in_degree_secondary,
    cn.node_role::VARCHAR AS node_role,
    cn.has_primary_counterpart::BOOLEAN AS has_primary_counterpart,
    cn.primary_delta_fraction::DOUBLE AS primary_delta_fraction,
    cn.unattributed_fraction::DOUBLE AS unattributed_fraction,
    cn.high_uncertainty::BOOLEAN AS high_uncertainty
FROM stage_evolve_results er, unnest(er.evolve_result.curr_nodes) AS u(cn);

CREATE OR REPLACE TEMP TABLE stage_prev_node_evo AS
SELECT
    er.prev_ordinal AS ordinal,
    pn.cluster_id::INTEGER AS cluster_id,
    pn.out_degree_primary::INTEGER AS out_degree_primary,
    pn.out_degree_secondary::INTEGER AS out_degree_secondary,
    pn.node_role::VARCHAR AS node_role,
    pn.unattributed_fraction::DOUBLE AS unattributed_fraction,
    pn.high_uncertainty::BOOLEAN AS high_uncertainty
FROM stage_evolve_results er, unnest(er.evolve_result.prev_nodes) AS u(pn);

CREATE OR REPLACE TEMP TABLE stage_evolution_nodes AS
SELECT
    n.ordinal,
    n.cluster_id,
    n.canonical_rank,
    i.branch_id,
    n.mean_features,
    n.member_count,
    n.member_fraction,
    i.assignment_reason,
    CASE WHEN n.ordinal = 0 OR cn.cluster_id IS NULL THEN 0 ELSE cn.in_degree_primary END AS in_degree_primary,
    CASE WHEN n.ordinal = 0 OR cn.cluster_id IS NULL THEN 0 ELSE cn.in_degree_secondary END AS in_degree_secondary,
    CASE WHEN n.ordinal + 1 = getvariable('planned_batch_count') OR pn.cluster_id IS NULL THEN 0 ELSE pn.out_degree_primary END AS out_degree_primary,
    CASE WHEN n.ordinal + 1 = getvariable('planned_batch_count') OR pn.cluster_id IS NULL THEN 0 ELSE pn.out_degree_secondary END AS out_degree_secondary,
    CASE
        WHEN n.ordinal = 0 THEN 'first_batch'
        WHEN cn.cluster_id IS NOT NULL THEN cn.node_role
        ELSE 'gap_restarted'
    END AS incoming_role,
    CASE
        WHEN n.ordinal + 1 = getvariable('planned_batch_count') THEN 'final_batch'
        WHEN pn.cluster_id IS NOT NULL THEN pn.node_role
        ELSE 'gap_interrupted'
    END AS outgoing_role,
    CASE
        WHEN cn.cluster_id IS NOT NULL AND cn.has_primary_counterpart THEN cn.primary_delta_fraction
        ELSE 0.0::DOUBLE
    END AS primary_delta_fraction,
    CASE
        WHEN cn.cluster_id IS NOT NULL THEN cn.has_primary_counterpart
        ELSE false
    END AS has_primary_delta_fraction,
    CASE
        WHEN cn.cluster_id IS NOT NULL THEN cn.unattributed_fraction
        ELSE 0.0::DOUBLE
    END AS unattributed_inflow_fraction,
    CASE
        WHEN pn.cluster_id IS NOT NULL THEN pn.unattributed_fraction
        ELSE 0.0::DOUBLE
    END AS unattributed_outflow_fraction,
    (coalesce(cn.high_uncertainty, false) OR coalesce(pn.high_uncertainty, false)) AS high_uncertainty
FROM stage_nodes_ranked n
JOIN stage_identity i USING (ordinal, cluster_id)
LEFT JOIN stage_curr_node_evo cn USING (ordinal, cluster_id)
LEFT JOIN stage_prev_node_evo pn USING (ordinal, cluster_id)
ORDER BY n.ordinal ASC, n.cluster_id ASC;

CREATE OR REPLACE TEMP TABLE stage_evolution_transitions AS
SELECT
    a.ordinal AS prev_ordinal,
    b.ordinal AS curr_ordinal,
    CASE WHEN er.prev_ordinal IS NOT NULL THEN 'ok' ELSE 'gap' END AS transition_status,
    (er.prev_ordinal IS NULL) AS chain_broken,
    CASE
        WHEN er.prev_ordinal IS NOT NULL THEN NULL::VARCHAR
        WHEN a.status = 'ok' AND b.status <> 'ok' THEN 'curr_unavailable'
        WHEN a.status <> 'ok' AND b.status = 'ok' THEN 'prev_unavailable'
        ELSE 'both_unavailable'
    END AS gap_reason,
    CASE WHEN er.prev_ordinal IS NOT NULL THEN er.evolve_result.pair_scale ELSE NULL::DOUBLE END AS pair_scale,
    CASE WHEN er.prev_ordinal IS NOT NULL THEN er.evolve_result.threshold ELSE NULL::DOUBLE END AS threshold,
    CASE WHEN er.prev_ordinal IS NOT NULL THEN er.evolve_result.summary.primary_edge_count::INTEGER ELSE 0 END AS primary_edge_count,
    CASE WHEN er.prev_ordinal IS NOT NULL THEN er.evolve_result.summary.secondary_split_edge_count::INTEGER ELSE 0 END AS secondary_split_edge_count,
    CASE WHEN er.prev_ordinal IS NOT NULL THEN er.evolve_result.summary.secondary_merge_edge_count::INTEGER ELSE 0 END AS secondary_merge_edge_count,
    CASE WHEN er.prev_ordinal IS NOT NULL THEN er.evolve_result.summary.transition_tv_redistribution ELSE NULL::DOUBLE END AS transition_tv_redistribution,
    CASE WHEN er.prev_ordinal IS NOT NULL THEN er.evolve_result.summary.min_threshold_margin ELSE NULL::DOUBLE END AS min_threshold_margin,
    CASE WHEN er.prev_ordinal IS NOT NULL THEN er.evolve_result.summary.ambiguous_tie ELSE false END AS ambiguous_tie,
    CASE WHEN er.prev_ordinal IS NOT NULL THEN er.evolve_result.summary.high_uncertainty ELSE false END AS high_uncertainty,
    er.evolve_result AS evolve_result
FROM stage_batches a
JOIN stage_batches b ON b.ordinal = a.ordinal + 1
LEFT JOIN stage_evolve_results er ON er.prev_ordinal = a.ordinal
ORDER BY a.ordinal ASC;

CREATE OR REPLACE TEMP TABLE stage_evolution_edges AS
SELECT
    er.prev_ordinal,
    e.prev_cluster_id::INTEGER AS prev_cluster_id,
    er.curr_ordinal,
    e.curr_cluster_id::INTEGER AS curr_cluster_id,
    ip.branch_id AS prev_branch_id,
    ic.branch_id AS curr_branch_id,
    e.edge_role::VARCHAR AS edge_role,
    e.event_type::VARCHAR AS event_type,
    e.rule_basis::VARCHAR AS rule_basis,
    e.distance::DOUBLE AS distance,
    e.distance_over_scale::DOUBLE AS distance_over_scale,
    e.prev_member_fraction::DOUBLE AS prev_member_fraction,
    e.curr_member_fraction::DOUBLE AS curr_member_fraction,
    e.edge_delta_fraction::DOUBLE AS edge_delta_fraction,
    e.attributed_flow::DOUBLE AS attributed_flow,
    e.edge_strength::DOUBLE AS edge_strength,
    e.threshold_margin::DOUBLE AS threshold_margin,
    e.threshold_margin_ratio::DOUBLE AS threshold_margin_ratio,
    e.primary_alternative_status::VARCHAR AS primary_alternative_status,
    e.has_optimality_margin::BOOLEAN AS has_optimality_margin,
    e.optimality_margin::DOUBLE AS optimality_margin,
    e.optimality_margin_ratio::DOUBLE AS optimality_margin_ratio,
    e.has_secondary_margin::BOOLEAN AS has_secondary_margin,
    e.secondary_margin::DOUBLE AS secondary_margin,
    e.secondary_margin_ratio::DOUBLE AS secondary_margin_ratio,
    e.ambiguous_tie::BOOLEAN AS ambiguous_tie,
    e.high_uncertainty::BOOLEAN AS high_uncertainty,
    e.uncertainty_reasons::VARCHAR[] AS uncertainty_reasons,
    er.evolve_result.non_physical_disclaimer::VARCHAR AS non_physical_disclaimer
FROM stage_evolve_results er, unnest(er.evolve_result.edges) AS u(e)
JOIN stage_identity ip
  ON ip.ordinal = er.prev_ordinal AND ip.cluster_id = e.prev_cluster_id::INTEGER
JOIN stage_identity ic
  ON ic.ordinal = er.curr_ordinal AND ic.cluster_id = e.curr_cluster_id::INTEGER
ORDER BY er.prev_ordinal ASC, e.prev_cluster_id ASC, e.curr_cluster_id ASC;

CREATE OR REPLACE TEMP TABLE stage_evolution_events AS
SELECT * FROM (
    -- 1. Events on 'ok' transitions
    SELECT
        er.prev_ordinal,
        ev.event_index::INTEGER AS event_index,
        ev.event_type::VARCHAR AS event_type,
        list_transform(ev.prev_cluster_ids, lambda x: x::INTEGER) AS prev_cluster_ids,
        list_transform(ev.curr_cluster_ids, lambda x: x::INTEGER) AS curr_cluster_ids,
        coalesce((
            SELECT list(DISTINCT bid ORDER BY bid ASC)
            FROM (
                SELECT ip.branch_id AS bid
                FROM unnest(ev.prev_cluster_ids) AS up(pcid)
                JOIN stage_identity ip ON ip.ordinal = er.prev_ordinal AND ip.cluster_id = pcid::INTEGER
                UNION ALL
                SELECT ic.branch_id AS bid
                FROM unnest(ev.curr_cluster_ids) AS uc(ccid)
                JOIN stage_identity ic ON ic.ordinal = er.curr_ordinal AND ic.cluster_id = ccid::INTEGER
            )
        ), []::VARCHAR[]) AS branch_ids,
        CASE
            WHEN ev.has_primary_link THEN (
                SELECT ip.branch_id
                FROM stage_identity ip
                WHERE ip.ordinal = er.prev_ordinal AND ip.cluster_id = ev.primary_prev_cluster_id::INTEGER
            )
            ELSE NULL::VARCHAR
        END AS primary_branch_id,
        ev.rule_basis::VARCHAR AS rule_basis,
        ev.max_morphology_distance::DOUBLE AS max_morphology_distance,
        ev.max_morphology_over_scale::DOUBLE AS max_morphology_over_scale,
        ev.prev_fraction_sum::DOUBLE AS prev_fraction_sum,
        ev.curr_fraction_sum::DOUBLE AS curr_fraction_sum,
        ev.net_delta_fraction::DOUBLE AS net_delta_fraction,
        ev.attributed_flow_sum::DOUBLE AS attributed_flow_sum,
        ev.has_min_margin_ratio::BOOLEAN AS has_min_margin_ratio,
        ev.min_margin_ratio::DOUBLE AS min_margin_ratio,
        ev.ambiguous_tie::BOOLEAN AS ambiguous_tie,
        ev.high_uncertainty::BOOLEAN AS high_uncertainty,
        er.evolve_result.non_physical_disclaimer::VARCHAR AS non_physical_disclaimer
    FROM stage_evolve_results er, unnest(er.evolve_result.events) AS u(ev)

    UNION ALL

    -- 2. 'gap_interrupted' events when prev batch is 'ok' and curr batch is unavailable
    SELECT
        tr.prev_ordinal,
        n.canonical_rank AS event_index,
        'gap_interrupted'::VARCHAR AS event_type,
        [n.cluster_id]::INTEGER[] AS prev_cluster_ids,
        []::INTEGER[] AS curr_cluster_ids,
        [n.branch_id]::VARCHAR[] AS branch_ids,
        NULL::VARCHAR AS primary_branch_id,
        'next_planned_batch_unavailable'::VARCHAR AS rule_basis,
        0.0::DOUBLE AS max_morphology_distance,
        0.0::DOUBLE AS max_morphology_over_scale,
        n.member_fraction AS prev_fraction_sum,
        0.0::DOUBLE AS curr_fraction_sum,
        CASE WHEN n.member_fraction = 0.0 THEN 0.0::DOUBLE ELSE -n.member_fraction END AS net_delta_fraction,
        0.0::DOUBLE AS attributed_flow_sum,
        false AS has_min_margin_ratio,
        0.0::DOUBLE AS min_margin_ratio,
        false AS ambiguous_tie,
        false AS high_uncertainty,
        'algorithmic_attribution_only_not_physical_member_migration'::VARCHAR AS non_physical_disclaimer
    FROM stage_evolution_transitions tr
    JOIN stage_evolution_nodes n ON n.ordinal = tr.prev_ordinal
    WHERE tr.transition_status = 'gap' AND tr.gap_reason = 'curr_unavailable'

    UNION ALL

    -- 3. 'gap_restarted' events when prev batch is unavailable and curr batch is 'ok'
    SELECT
        tr.prev_ordinal,
        n.canonical_rank AS event_index,
        'gap_restarted'::VARCHAR AS event_type,
        []::INTEGER[] AS prev_cluster_ids,
        [n.cluster_id]::INTEGER[] AS curr_cluster_ids,
        [n.branch_id]::VARCHAR[] AS branch_ids,
        NULL::VARCHAR AS primary_branch_id,
        'previous_planned_batch_unavailable'::VARCHAR AS rule_basis,
        0.0::DOUBLE AS max_morphology_distance,
        0.0::DOUBLE AS max_morphology_over_scale,
        0.0::DOUBLE AS prev_fraction_sum,
        n.member_fraction AS curr_fraction_sum,
        n.member_fraction AS net_delta_fraction,
        0.0::DOUBLE AS attributed_flow_sum,
        false AS has_min_margin_ratio,
        0.0::DOUBLE AS min_margin_ratio,
        false AS ambiguous_tie,
        false AS high_uncertainty,
        'algorithmic_attribution_only_not_physical_member_migration'::VARCHAR AS non_physical_disclaimer
    FROM stage_evolution_transitions tr
    JOIN stage_evolution_nodes n ON n.ordinal = tr.curr_ordinal
    WHERE tr.transition_status = 'gap' AND tr.gap_reason = 'prev_unavailable'
)
ORDER BY prev_ordinal ASC, event_index ASC;

-- ============================================================================
-- Stage A.5: Materialize Branch Summaries & Chain Summary via `ens_fsum`
-- ============================================================================

CREATE OR REPLACE TEMP TABLE stage_evolution_branch_summaries AS
WITH branch_bounds AS (
    SELECT
        branch_id,
        min(ordinal)::INTEGER AS birth_ordinal,
        max(ordinal)::INTEGER AS end_ordinal,
        count(*)::INTEGER AS node_count,
        (count(*) - 1)::INTEGER AS step_count,
        min(member_fraction)::DOUBLE AS min_member_fraction,
        max(member_fraction)::DOUBLE AS max_member_fraction
    FROM stage_evolution_nodes
    GROUP BY branch_id
)
SELECT
    b.branch_id,
    b.birth_ordinal,
    b.end_ordinal,
    b.node_count,
    b.step_count,
    CASE nb.incoming_role
        WHEN 'first_batch' THEN 'first_batch'
        WHEN 'gap_restarted' THEN 'gap_restarted'
        WHEN 'split_secondary_child' THEN 'secondary_split'
        WHEN 'emerged' THEN 'emergence'
        ELSE nb.incoming_role
    END AS birth_reason,
    CASE ne.outgoing_role
        WHEN 'final_batch' THEN 'final_batch'
        WHEN 'gap_interrupted' THEN 'gap_interrupted'
        WHEN 'merge_secondary_parent' THEN 'secondary_merge'
        WHEN 'disappeared' THEN 'disappearance'
        ELSE ne.outgoing_role
    END AS termination_reason,
    ens_fsum(coalesce((
        SELECT list(e.distance ORDER BY e.prev_ordinal ASC)
        FROM stage_evolution_edges e
        WHERE e.edge_role = 'primary' AND e.prev_branch_id = b.branch_id
    ), []::DOUBLE[])) AS cumulative_morphology_distance,
    ens_fsum(coalesce((
        SELECT list(e.distance_over_scale ORDER BY e.prev_ordinal ASC)
        FROM stage_evolution_edges e
        WHERE e.edge_role = 'primary' AND e.prev_branch_id = b.branch_id
    ), []::DOUBLE[])) AS cumulative_morphology_over_scale,
    coalesce((
        SELECT max(e.distance)
        FROM stage_evolution_edges e
        WHERE e.edge_role = 'primary' AND e.prev_branch_id = b.branch_id
    ), 0.0::DOUBLE) AS max_step_morphology_distance,
    coalesce((
        SELECT max(e.distance_over_scale)
        FROM stage_evolution_edges e
        WHERE e.edge_role = 'primary' AND e.prev_branch_id = b.branch_id
    ), 0.0::DOUBLE) AS max_step_morphology_over_scale,
    CASE
        WHEN b.step_count = 0 THEN 0.0::DOUBLE
        ELSE 0.0::DOUBLE + sqrt(greatest(
            0.0::DOUBLE,
            ens_fsum(
                list_transform(
                    range(1, len(getvariable('weights')) + 1),
                    lambda g: (getvariable('weights')[g]) * ((nb.mean_features[g] - ne.mean_features[g]) * (nb.mean_features[g] - ne.mean_features[g]))
                )
            ) / ens_fsum(getvariable('weights'))
        ))
    END AS end_to_end_morphology_distance,
    nb.member_fraction AS initial_member_fraction,
    ne.member_fraction AS final_member_fraction,
    b.min_member_fraction,
    b.max_member_fraction,
    CASE
        WHEN ne.member_fraction - nb.member_fraction = 0.0 THEN 0.0::DOUBLE
        ELSE (ne.member_fraction - nb.member_fraction)::DOUBLE
    END AS net_delta_fraction,
    ens_fsum(coalesce((
        SELECT list(abs(n2.member_fraction - n1.member_fraction) ORDER BY n1.ordinal ASC)
        FROM stage_evolution_nodes n1
        JOIN stage_evolution_nodes n2
          ON n2.branch_id = n1.branch_id AND n2.ordinal = n1.ordinal + 1
        WHERE n1.branch_id = b.branch_id
    ), []::DOUBLE[])) AS cumulative_abs_delta_fraction,
    (
        SELECT count(*)::INTEGER
        FROM stage_evolution_events ev
        WHERE ev.event_type = 'split' AND ev.primary_branch_id = b.branch_id
    ) AS split_out_event_count,
    coalesce((
        SELECT list(e.curr_branch_id ORDER BY e.prev_ordinal ASC, e.curr_branch_id ASC)
        FROM stage_evolution_edges e
        WHERE e.edge_role = 'secondary_split' AND e.prev_branch_id = b.branch_id
    ), []::VARCHAR[]) AS spawned_secondary_branch_ids,
    (
        SELECT e.prev_branch_id
        FROM stage_evolution_edges e
        WHERE e.edge_role = 'secondary_split'
          AND e.curr_ordinal = b.birth_ordinal
          AND e.curr_branch_id = b.branch_id
        LIMIT 1
    ) AS split_from_parent_branch_id,
    (
        SELECT count(*)::INTEGER
        FROM stage_evolution_events ev
        WHERE ev.event_type = 'merge' AND ev.primary_branch_id = b.branch_id
    ) AS merge_in_event_count,
    coalesce((
        SELECT list(e.prev_branch_id ORDER BY e.prev_ordinal ASC, e.prev_branch_id ASC)
        FROM stage_evolution_edges e
        WHERE e.edge_role = 'secondary_merge' AND e.curr_branch_id = b.branch_id
    ), []::VARCHAR[]) AS absorbed_secondary_branch_ids,
    (
        SELECT e.curr_branch_id
        FROM stage_evolution_edges e
        WHERE e.edge_role = 'secondary_merge'
          AND e.prev_ordinal = b.end_ordinal
          AND e.prev_branch_id = b.branch_id
        LIMIT 1
    ) AS merged_into_branch_id,
    (
        SELECT count(*)::INTEGER
        FROM stage_evolution_edges e
        WHERE e.edge_role = 'primary' AND e.prev_branch_id = b.branch_id AND e.ambiguous_tie
    ) AS ambiguous_step_count,
    (
        SELECT count(*)::INTEGER
        FROM stage_evolution_edges e
        WHERE e.edge_role = 'primary' AND e.prev_branch_id = b.branch_id AND e.high_uncertainty
    ) AS high_uncertainty_step_count,
    CASE
        WHEN b.step_count > 0 THEN (
            (
                SELECT count(*)::DOUBLE
                FROM stage_evolution_edges e
                WHERE e.edge_role = 'primary' AND e.prev_branch_id = b.branch_id AND e.high_uncertainty
            ) / b.step_count::DOUBLE
        )
        ELSE 0.0::DOUBLE
    END AS high_uncertainty_step_ratio
FROM branch_bounds b
JOIN stage_evolution_nodes nb
  ON nb.branch_id = b.branch_id AND nb.ordinal = b.birth_ordinal
JOIN stage_evolution_nodes ne
  ON ne.branch_id = b.branch_id AND ne.ordinal = b.end_ordinal
ORDER BY b.branch_id ASC;

CREATE OR REPLACE TEMP TABLE stage_chain_summary AS
WITH counts AS (
    SELECT
        (SELECT count(*)::INTEGER FROM stage_batches WHERE status = 'ok') AS ok_batch_count,
        (SELECT count(*)::INTEGER FROM stage_batches WHERE status = 'missing') AS missing_batch_count,
        (SELECT count(*)::INTEGER FROM stage_batches WHERE status = 'failed') AS failed_batch_count,
        (SELECT count(*)::INTEGER FROM stage_evolution_transitions WHERE transition_status = 'ok') AS ok_transition_count,
        (SELECT count(*)::INTEGER FROM stage_evolution_transitions WHERE transition_status = 'gap') AS gap_transition_count,
        (SELECT count(*)::INTEGER FROM stage_evolution_branch_summaries) AS branch_count,
        (SELECT count(*)::INTEGER FROM stage_evolution_edges WHERE edge_role = 'primary') AS primary_edge_count,
        (SELECT count(*)::INTEGER FROM stage_evolution_edges WHERE edge_role = 'secondary_split') AS secondary_split_edge_count,
        (SELECT count(*)::INTEGER FROM stage_evolution_edges WHERE edge_role = 'secondary_merge') AS secondary_merge_edge_count,
        (SELECT count(*)::INTEGER FROM stage_evolution_edges) AS total_edge_count,
        (SELECT count(*)::INTEGER FROM stage_evolution_events WHERE event_type = 'continuation') AS continuation_count,
        (SELECT count(*)::INTEGER FROM stage_evolution_events WHERE event_type = 'split') AS split_count,
        (SELECT count(*)::INTEGER FROM stage_evolution_events WHERE event_type = 'merge') AS merge_count,
        (SELECT count(*)::INTEGER FROM stage_evolution_events WHERE event_type = 'emergence') AS emergence_count,
        (SELECT count(*)::INTEGER FROM stage_evolution_events WHERE event_type = 'disappearance') AS disappearance_count,
        (SELECT count(*)::INTEGER FROM stage_evolution_events WHERE event_type = 'gap_interrupted') AS gap_interrupted_count,
        (SELECT count(*)::INTEGER FROM stage_evolution_events WHERE event_type = 'gap_restarted') AS gap_restarted_count,
        (SELECT count(*)::INTEGER FROM stage_evolution_transitions WHERE transition_status = 'ok' AND ambiguous_tie) AS ambiguous_transition_count,
        (SELECT count(*)::INTEGER FROM stage_evolution_transitions WHERE transition_status = 'ok' AND high_uncertainty) AS high_uncertainty_transition_count,
        (SELECT count(*)::INTEGER FROM stage_evolution_edges WHERE high_uncertainty) AS high_uncertainty_edge_count
)
SELECT
    struct_pack(
        ok_batch_count := c.ok_batch_count,
        missing_batch_count := c.missing_batch_count,
        failed_batch_count := c.failed_batch_count,
        ok_transition_count := c.ok_transition_count,
        gap_transition_count := c.gap_transition_count,
        branch_count := c.branch_count,
        primary_edge_count := c.primary_edge_count,
        secondary_split_edge_count := c.secondary_split_edge_count,
        secondary_merge_edge_count := c.secondary_merge_edge_count,
        continuation_count := c.continuation_count,
        split_count := c.split_count,
        merge_count := c.merge_count,
        emergence_count := c.emergence_count,
        disappearance_count := c.disappearance_count,
        gap_interrupted_count := c.gap_interrupted_count,
        gap_restarted_count := c.gap_restarted_count,
        cumulative_primary_distance := ens_fsum(coalesce((
            SELECT list(e.distance ORDER BY e.prev_ordinal ASC, e.prev_cluster_id ASC, e.curr_cluster_id ASC)
            FROM stage_evolution_edges e
            WHERE e.edge_role = 'primary'
        ), []::DOUBLE[])),
        max_edge_distance := coalesce((SELECT max(e.distance) FROM stage_evolution_edges e), 0.0::DOUBLE),
        max_edge_distance_over_scale := coalesce((SELECT max(e.distance_over_scale) FROM stage_evolution_edges e), 0.0::DOUBLE),
        mean_transition_tv_redistribution := CASE
            WHEN c.ok_transition_count > 0 THEN (
                ens_fsum(coalesce((
                    SELECT list(t.transition_tv_redistribution ORDER BY t.prev_ordinal ASC)
                    FROM stage_evolution_transitions t
                    WHERE t.transition_status = 'ok'
                ), []::DOUBLE[])) / c.ok_transition_count::DOUBLE
            )
            ELSE 0.0::DOUBLE
        END,
        max_transition_tv_redistribution := coalesce((
            SELECT max(t.transition_tv_redistribution)
            FROM stage_evolution_transitions t
            WHERE t.transition_status = 'ok'
        ), 0.0::DOUBLE),
        ambiguous_transition_count := c.ambiguous_transition_count,
        high_uncertainty_transition_count := c.high_uncertainty_transition_count,
        high_uncertainty_transition_ratio := CASE
            WHEN c.ok_transition_count > 0 THEN (c.high_uncertainty_transition_count::DOUBLE / c.ok_transition_count::DOUBLE)
            ELSE 0.0::DOUBLE
        END,
        high_uncertainty_edge_count := c.high_uncertainty_edge_count,
        high_uncertainty_edge_ratio := CASE
            WHEN c.total_edge_count > 0 THEN (c.high_uncertainty_edge_count::DOUBLE / c.total_edge_count::DOUBLE)
            ELSE 0.0::DOUBLE
        END
    ) AS chain_summary
FROM counts c;

-- ============================================================================
-- Stage A.6: Canonical M3 Content Digest (`m3_content_sha256`)
-- ============================================================================

CREATE OR REPLACE TEMP VIEW stage_m3_content_lines AS
SELECT
    1 AS tbl_ord,
    [0, 0, 0] AS pk_int,
    '' AS pk_str,
    to_json({
        'evolution_run_id': getvariable('evolution_run_id'),
        'tracking_run_key': getvariable('run_key'),
        'evolution_binding': getvariable('evolution_binding'),
        'evolve_config': (SELECT cfg FROM stage_evolve_config_val),
        'weights': list_transform(getvariable('weights'), lambda x: CASE WHEN x = 0.0 THEN 0.0::DOUBLE ELSE x::DOUBLE END),
        'planned_batch_count': getvariable('planned_batch_count')::INTEGER,
        'chain_summary': cs.chain_summary,
        'non_physical_disclaimer': 'algorithmic_attribution_only_not_physical_member_migration'
    })::VARCHAR AS line
FROM stage_chain_summary cs
UNION ALL
SELECT
    2 AS tbl_ord,
    [n.ordinal, n.cluster_id, 0] AS pk_int,
    '' AS pk_str,
    to_json(n)::VARCHAR AS line
FROM stage_evolution_nodes n
UNION ALL
SELECT
    3 AS tbl_ord,
    [t.prev_ordinal, 0, 0] AS pk_int,
    '' AS pk_str,
    to_json(t)::VARCHAR AS line
FROM stage_evolution_transitions t
UNION ALL
SELECT
    4 AS tbl_ord,
    [e.prev_ordinal, e.prev_cluster_id, e.curr_cluster_id] AS pk_int,
    '' AS pk_str,
    to_json(e)::VARCHAR AS line
FROM stage_evolution_edges e
UNION ALL
SELECT
    5 AS tbl_ord,
    [ev.prev_ordinal, ev.event_index, 0] AS pk_int,
    '' AS pk_str,
    to_json(ev)::VARCHAR AS line
FROM stage_evolution_events ev
UNION ALL
SELECT
    6 AS tbl_ord,
    [0, 0, 0] AS pk_int,
    br.branch_id AS pk_str,
    to_json(br)::VARCHAR AS line
FROM stage_evolution_branch_summaries br;

SET VARIABLE m3_content_sha256 = (
    SELECT sha256(string_agg(tbl_ord::VARCHAR || ':' || line, chr(10) ORDER BY tbl_ord ASC, pk_int ASC, pk_str ASC))
    FROM stage_m3_content_lines
);

-- ============================================================================
-- Stage B: Single-Transaction Atomic 11-Table Publish + G1–G4 & E1–E8 Guards
-- ============================================================================

BEGIN TRANSACTION;

SELECT CASE
    WHEN getvariable('has_raw_batches')
      AND (SELECT count(*) FROM stage_raw_batches WHERE raw_status = 'present')
          <> (SELECT count(*) FROM stage_raw_diag)
    THEN error('RAW_BATCH_DIAGNOSE_INCOMPLETE')
    WHEN EXISTS (
        SELECT 1 FROM stage_batches
        WHERE status = 'failed'
          AND (status_reason IS NULL OR NOT regexp_matches(
              status_reason,
              '^(INSUFFICIENT_RANK|AMBIGUOUS_SUBSPACE|EMPTY_CLUSTER|NOT_CONVERGED|INSUFFICIENT_DISTINCT_VECTORS|INSUFFICIENT_PROJECTED_VECTORS|SVD_NOT_CONVERGED|ZERO_DISPERSION)([^A-Za-z0-9_]|$)'
          ))
    )
    THEN error('NON_WHITELISTED_BATCH_FAILURE: ' || coalesce((
        SELECT status_reason FROM stage_batches
        WHERE status = 'failed'
          AND (status_reason IS NULL OR NOT regexp_matches(
              status_reason,
              '^(INSUFFICIENT_RANK|AMBIGUOUS_SUBSPACE|EMPTY_CLUSTER|NOT_CONVERGED|INSUFFICIENT_DISTINCT_VECTORS|INSUFFICIENT_PROJECTED_VECTORS|SVD_NOT_CONVERGED|ZERO_DISPERSION)([^A-Za-z0-9_]|$)'
          ))
        ORDER BY ordinal ASC LIMIT 1
    ), 'NULL'))
END AS stage_b_batch_integrity_guard;

-- ----------------------------------------------------------------------------
-- B.1: M2 Idempotency / Conflict Check & Five-Table Publish
-- ----------------------------------------------------------------------------

SELECT CASE
    WHEN getvariable('execution_binding') IS NULL
    THEN error('RUN_VERSION_CONFLICT run_key=' || getvariable('run_key'))
    WHEN EXISTS (
        SELECT 1 FROM tracking_runs
        WHERE run_key = getvariable('run_key')
          AND (
              content_sha256 <> getvariable('content_sha256')
              OR list_transform(weights, lambda x: CASE WHEN x = 0.0 THEN 0.0::DOUBLE ELSE x::DOUBLE END)
                 <> list_transform(getvariable('weights'), lambda x: CASE WHEN x = 0.0 THEN 0.0::DOUBLE ELSE x::DOUBLE END)
              OR to_json(execution_binding)::VARCHAR <> to_json((
                  getvariable('execution_binding')::STRUCT(
                      run_id VARCHAR,
                      input_manifest_sha256 VARCHAR,
                      scenario_config_sha256 VARCHAR,
                      match_config_sha256 VARCHAR,
                      weights_version VARCHAR,
                      threshold_coefficient DOUBLE,
                      cluster_count INTEGER,
                      software STRUCT(
                          ensabc_extension_sha256 VARCHAR,
                          duckomo_version VARCHAR,
                          duckomo_sha256 VARCHAR,
                          duckdb_version VARCHAR,
                          cluster_algorithm_version VARCHAR,
                          match_algorithm_version VARCHAR
                      )
                  )
              ))::VARCHAR
              OR planned_batch_count <> getvariable('planned_batch_count')::INTEGER
          )
    ) THEN error('RUN_VERSION_CONFLICT run_key=' || getvariable('run_key'))
END AS m2_conflict_guard;

SET VARIABLE m2_already_published = (
    SELECT count(*) > 0 FROM tracking_runs WHERE run_key = getvariable('run_key')
);

INSERT INTO tracking_runs
SELECT
    getvariable('run_key'),
    getvariable('execution_binding'),
    getvariable('weights'),
    getvariable('planned_batch_count'),
    getvariable('content_sha256')
WHERE NOT getvariable('m2_already_published');

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
WHERE NOT getvariable('m2_already_published')
ORDER BY ordinal ASC;

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
WHERE NOT getvariable('m2_already_published')
ORDER BY n.ordinal ASC, n.cluster_id ASC;

SELECT CASE
    WHEN getvariable('inject_failure') THEN error('INJECTED_FAILURE after M2 scenario_nodes')
END AS m2_fault_injection_check;

INSERT INTO pair_comparisons
SELECT
    getvariable('run_key'),
    prev_ordinal,
    curr_ordinal,
    match_result.pair_scale,
    match_result.threshold,
    match_result
FROM stage_match_results
WHERE NOT getvariable('m2_already_published')
ORDER BY prev_ordinal ASC;

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
WHERE NOT getvariable('m2_already_published')
ORDER BY prev_ordinal ASC, prev_cluster_id ASC;

-- M2 Invariant Guards G1–G4
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

SELECT CASE
    WHEN EXISTS (
        SELECT branch_id
        FROM scenario_nodes
        WHERE run_key = getvariable('run_key')
        GROUP BY branch_id
        HAVING max(ordinal) - min(ordinal) + 1 <> count(*)
    ) THEN error('INVARIANT_VIOLATION: G2 branch resurrection detected')
END AS guard_g2;

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

-- ----------------------------------------------------------------------------
-- B.2: M3 Idempotency / Conflict Check (`EVOLUTION_RUN_VERSION_CONFLICT`)
-- ----------------------------------------------------------------------------

SELECT CASE
    WHEN getvariable('evolution_binding') IS NULL
      OR getvariable('evolution_binding').tracking_run_key IS DISTINCT FROM getvariable('run_key')
    THEN error('EVOLUTION_RUN_VERSION_CONFLICT: evolution_binding.tracking_run_key mismatch for evolution_run_id=' || getvariable('evolution_run_id'))
    WHEN EXISTS (
        SELECT 1 FROM evolution_runs
        WHERE evolution_run_id = getvariable('evolution_run_id')
          AND (
              content_sha256 <> getvariable('m3_content_sha256')
              OR tracking_run_key <> getvariable('run_key')
          )
    ) THEN error('EVOLUTION_RUN_VERSION_CONFLICT: evolution_run_id=' || getvariable('evolution_run_id'))
END AS m3_conflict_guard;

SET VARIABLE m3_already_published = (
    SELECT count(*) > 0 FROM evolution_runs WHERE evolution_run_id = getvariable('evolution_run_id')
);

-- ----------------------------------------------------------------------------
-- B.3: Insert into M3 Six Tables in Foreign-Key Order (with fault hooks)
-- ----------------------------------------------------------------------------

INSERT INTO evolution_runs
SELECT
    getvariable('evolution_run_id'),
    getvariable('run_key'),
    getvariable('evolution_binding'),
    getvariable('weights'),
    getvariable('planned_batch_count'),
    cs.chain_summary,
    'algorithmic_attribution_only_not_physical_member_migration',
    getvariable('m3_content_sha256')
FROM stage_chain_summary cs
WHERE NOT getvariable('m3_already_published');

SELECT CASE
    WHEN getvariable('inject_m3_failure') IN ('after_runs')
    THEN error('INJECTED_M3_FAILURE: after_runs')
END AS m3_fault_after_runs;

INSERT INTO evolution_nodes
SELECT
    getvariable('evolution_run_id'),
    ordinal,
    cluster_id,
    canonical_rank,
    branch_id,
    mean_features,
    member_count,
    member_fraction,
    assignment_reason,
    in_degree_primary,
    in_degree_secondary,
    out_degree_primary,
    out_degree_secondary,
    incoming_role,
    outgoing_role,
    primary_delta_fraction,
    has_primary_delta_fraction,
    unattributed_inflow_fraction,
    unattributed_outflow_fraction,
    high_uncertainty
FROM stage_evolution_nodes
WHERE NOT getvariable('m3_already_published')
ORDER BY ordinal ASC, cluster_id ASC;

SELECT CASE
    WHEN getvariable('inject_m3_failure') IN ('after_nodes')
    THEN error('INJECTED_M3_FAILURE: after_nodes')
END AS m3_fault_after_nodes;

INSERT INTO evolution_transitions
SELECT
    getvariable('evolution_run_id'),
    prev_ordinal,
    curr_ordinal,
    transition_status,
    chain_broken,
    gap_reason,
    pair_scale,
    threshold,
    primary_edge_count,
    secondary_split_edge_count,
    secondary_merge_edge_count,
    transition_tv_redistribution,
    min_threshold_margin,
    ambiguous_tie,
    high_uncertainty,
    evolve_result
FROM stage_evolution_transitions
WHERE NOT getvariable('m3_already_published')
ORDER BY prev_ordinal ASC;

SELECT CASE
    WHEN getvariable('inject_m3_failure') IN ('after_transitions')
    THEN error('INJECTED_M3_FAILURE: after_transitions')
END AS m3_fault_after_transitions;

INSERT INTO evolution_edges
SELECT
    getvariable('evolution_run_id'),
    prev_ordinal,
    prev_cluster_id,
    curr_ordinal,
    curr_cluster_id,
    prev_branch_id,
    curr_branch_id,
    edge_role,
    event_type,
    rule_basis,
    distance,
    distance_over_scale,
    prev_member_fraction,
    curr_member_fraction,
    edge_delta_fraction,
    attributed_flow,
    edge_strength,
    threshold_margin,
    threshold_margin_ratio,
    primary_alternative_status,
    has_optimality_margin,
    optimality_margin,
    optimality_margin_ratio,
    has_secondary_margin,
    secondary_margin,
    secondary_margin_ratio,
    ambiguous_tie,
    high_uncertainty,
    uncertainty_reasons,
    non_physical_disclaimer
FROM stage_evolution_edges
WHERE NOT getvariable('m3_already_published')
ORDER BY prev_ordinal ASC, prev_cluster_id ASC, curr_cluster_id ASC;

SELECT CASE
    WHEN getvariable('inject_m3_failure') IN ('after_edges', 'true', '1')
    THEN error('INJECTED_M3_FAILURE: after_edges')
END AS m3_fault_after_edges;

INSERT INTO evolution_events
SELECT
    getvariable('evolution_run_id'),
    prev_ordinal,
    event_index,
    event_type,
    prev_cluster_ids,
    curr_cluster_ids,
    branch_ids,
    primary_branch_id,
    rule_basis,
    max_morphology_distance,
    max_morphology_over_scale,
    prev_fraction_sum,
    curr_fraction_sum,
    net_delta_fraction,
    attributed_flow_sum,
    has_min_margin_ratio,
    min_margin_ratio,
    ambiguous_tie,
    high_uncertainty,
    non_physical_disclaimer
FROM stage_evolution_events
WHERE NOT getvariable('m3_already_published')
ORDER BY prev_ordinal ASC, event_index ASC;

SELECT CASE
    WHEN getvariable('inject_m3_failure') IN ('after_events')
    THEN error('INJECTED_M3_FAILURE: after_events')
END AS m3_fault_after_events;

INSERT INTO evolution_branch_summaries
SELECT
    getvariable('evolution_run_id'),
    branch_id,
    birth_ordinal,
    end_ordinal,
    node_count,
    step_count,
    birth_reason,
    termination_reason,
    cumulative_morphology_distance,
    cumulative_morphology_over_scale,
    max_step_morphology_distance,
    max_step_morphology_over_scale,
    end_to_end_morphology_distance,
    initial_member_fraction,
    final_member_fraction,
    min_member_fraction,
    max_member_fraction,
    net_delta_fraction,
    cumulative_abs_delta_fraction,
    split_out_event_count,
    spawned_secondary_branch_ids,
    split_from_parent_branch_id,
    merge_in_event_count,
    absorbed_secondary_branch_ids,
    merged_into_branch_id,
    ambiguous_step_count,
    high_uncertainty_step_count,
    high_uncertainty_step_ratio
FROM stage_evolution_branch_summaries
WHERE NOT getvariable('m3_already_published')
ORDER BY branch_id ASC;

SELECT CASE
    WHEN getvariable('inject_m3_failure') IN ('after_branches')
    THEN error('INJECTED_M3_FAILURE: after_branches')
END AS m3_fault_after_branches;

-- ----------------------------------------------------------------------------
-- B.4: Pre-Commit M3 Global Invariant Guards E1–E8 (data-model.md §3.3)
-- ----------------------------------------------------------------------------

-- E1: M2 scenario_nodes <-> M3 evolution_nodes bidirectional exact bijection
SELECT CASE
    WHEN (SELECT count(*) FROM evolution_nodes WHERE evolution_run_id = getvariable('evolution_run_id'))
         <> (SELECT count(*) FROM scenario_nodes WHERE run_key = getvariable('run_key'))
      OR EXISTS (
          SELECT ordinal, cluster_id, canonical_rank, branch_id, member_count, member_fraction, assignment_reason
          FROM evolution_nodes
          WHERE evolution_run_id = getvariable('evolution_run_id')
          EXCEPT
          SELECT ordinal, cluster_id, canonical_rank, branch_id, member_count, member_fraction, assignment_reason
          FROM scenario_nodes
          WHERE run_key = getvariable('run_key')
      )
      OR EXISTS (
          SELECT ordinal, cluster_id, canonical_rank, branch_id, member_count, member_fraction, assignment_reason
          FROM scenario_nodes
          WHERE run_key = getvariable('run_key')
          EXCEPT
          SELECT ordinal, cluster_id, canonical_rank, branch_id, member_count, member_fraction, assignment_reason
          FROM evolution_nodes
          WHERE evolution_run_id = getvariable('evolution_run_id')
      )
    THEN error('INVARIANT_VIOLATION: E1 evolution_nodes does not match M2 scenario_nodes verbatim')
END AS guard_e1;

-- E2: M2 scenario_links <-> M3 primary evolution_edges bijection & identical branch_id
SELECT CASE
    WHEN (SELECT count(*) FROM evolution_edges WHERE evolution_run_id = getvariable('evolution_run_id') AND edge_role = 'primary')
         <> (SELECT count(*) FROM scenario_links WHERE run_key = getvariable('run_key'))
      OR EXISTS (
          SELECT prev_ordinal, prev_cluster_id, curr_ordinal, curr_cluster_id, distance, ambiguous_tie
          FROM evolution_edges
          WHERE evolution_run_id = getvariable('evolution_run_id') AND edge_role = 'primary'
          EXCEPT
          SELECT prev_ordinal, prev_cluster_id, curr_ordinal, curr_cluster_id, distance, ambiguous
          FROM scenario_links
          WHERE run_key = getvariable('run_key')
      )
      OR EXISTS (
          SELECT prev_ordinal, prev_cluster_id, curr_ordinal, curr_cluster_id, distance, ambiguous
          FROM scenario_links
          WHERE run_key = getvariable('run_key')
          EXCEPT
          SELECT prev_ordinal, prev_cluster_id, curr_ordinal, curr_cluster_id, distance, ambiguous_tie
          FROM evolution_edges
          WHERE evolution_run_id = getvariable('evolution_run_id') AND edge_role = 'primary'
      )
      OR EXISTS (
          SELECT 1
          FROM evolution_edges e
          JOIN evolution_nodes pn
            ON pn.evolution_run_id = e.evolution_run_id
           AND pn.ordinal = e.prev_ordinal
           AND pn.cluster_id = e.prev_cluster_id
          JOIN evolution_nodes cn
            ON cn.evolution_run_id = e.evolution_run_id
           AND cn.ordinal = e.curr_ordinal
           AND cn.cluster_id = e.curr_cluster_id
          WHERE e.evolution_run_id = getvariable('evolution_run_id')
            AND (
                pn.branch_id <> e.prev_branch_id
                OR cn.branch_id <> e.curr_branch_id
                OR (e.edge_role = 'primary' AND e.prev_branch_id <> e.curr_branch_id)
            )
      )
    THEN error('INVARIANT_VIOLATION: E2 primary backbone mismatch or branch_id discontinuity')
END AS guard_e2;

-- E3: Secondary edge feasibility, role mutual exclusion, and zero edges on no_candidate nodes
SELECT CASE
    WHEN EXISTS (
        SELECT 1
        FROM evolution_edges e
        JOIN evolution_transitions t
          ON t.evolution_run_id = e.evolution_run_id AND t.prev_ordinal = e.prev_ordinal
        JOIN evolution_nodes pn
          ON pn.evolution_run_id = e.evolution_run_id AND pn.ordinal = e.prev_ordinal AND pn.cluster_id = e.prev_cluster_id
        JOIN evolution_nodes cn
          ON cn.evolution_run_id = e.evolution_run_id AND cn.ordinal = e.curr_ordinal AND cn.cluster_id = e.curr_cluster_id
        WHERE e.evolution_run_id = getvariable('evolution_run_id')
          AND (
              t.threshold IS NULL
              OR e.distance > t.threshold
              OR (e.edge_role = 'secondary_split' AND (
                  cn.in_degree_primary <> 0
                  OR cn.assignment_reason <> 'not_selected_in_global_optimum'
                  OR pn.out_degree_primary <> 1
              ))
              OR (e.edge_role = 'secondary_merge' AND (
                  pn.out_degree_primary <> 0
                  OR cn.in_degree_primary <> 1
              ))
          )
    )
    OR EXISTS (
        SELECT 1
        FROM evolution_nodes n
        WHERE n.evolution_run_id = getvariable('evolution_run_id')
          AND n.assignment_reason = 'no_candidate_within_threshold'
          AND (n.in_degree_primary <> 0 OR n.in_degree_secondary <> 0)
    )
    OR EXISTS (
        SELECT 1
        FROM evolution_nodes n
        WHERE n.evolution_run_id = getvariable('evolution_run_id')
          AND n.outgoing_role = 'disappeared'
          AND (n.out_degree_primary <> 0 OR n.out_degree_secondary <> 0)
    )
    OR EXISTS (
        -- Theorem 2: no primary edge simultaneously splits out of prev and merges into curr
        SELECT 1
        FROM evolution_edges ep
        WHERE ep.evolution_run_id = getvariable('evolution_run_id')
          AND ep.edge_role = 'primary'
          AND EXISTS (
              SELECT 1 FROM evolution_edges es
              WHERE es.evolution_run_id = ep.evolution_run_id
                AND es.prev_ordinal = ep.prev_ordinal
                AND es.prev_cluster_id = ep.prev_cluster_id
                AND es.edge_role = 'secondary_split'
          )
          AND EXISTS (
              SELECT 1 FROM evolution_edges em
              WHERE em.evolution_run_id = ep.evolution_run_id
                AND em.curr_ordinal = ep.curr_ordinal
                AND em.curr_cluster_id = ep.curr_cluster_id
                AND em.edge_role = 'secondary_merge'
          )
    )
    THEN error('INVARIANT_VIOLATION: E3 secondary edge feasibility or role mutual exclusion violated')
END AS guard_e3;

-- E4: Transition count == B - 1, zero cross-gap edges, and contiguous branch ordinals
SELECT CASE
    WHEN (SELECT count(*) FROM evolution_transitions WHERE evolution_run_id = getvariable('evolution_run_id'))
         <> getvariable('planned_batch_count') - 1
      OR EXISTS (
          SELECT 1
          FROM evolution_edges e
          JOIN evolution_transitions t
            ON t.evolution_run_id = e.evolution_run_id AND t.prev_ordinal = e.prev_ordinal
          WHERE e.evolution_run_id = getvariable('evolution_run_id')
            AND (t.chain_broken OR t.transition_status <> 'ok')
      )
      OR EXISTS (
          SELECT branch_id
          FROM evolution_nodes
          WHERE evolution_run_id = getvariable('evolution_run_id')
          GROUP BY branch_id
          HAVING max(ordinal) - min(ordinal) + 1 <> count(*)
      )
    THEN error('INVARIANT_VIOLATION: E4 gap chain break or contiguous branch ordinal violated')
END AS guard_e4;

-- E5: Strict disjointness between gap_interrupted/gap_restarted and ok transition events
SELECT CASE
    WHEN EXISTS (
        SELECT 1
        FROM evolution_events ev
        JOIN evolution_transitions t
          ON t.evolution_run_id = ev.evolution_run_id AND t.prev_ordinal = ev.prev_ordinal
        WHERE ev.evolution_run_id = getvariable('evolution_run_id')
          AND (
              (t.transition_status = 'gap' AND ev.event_type NOT IN ('gap_interrupted', 'gap_restarted'))
              OR (t.transition_status = 'ok' AND ev.event_type IN ('gap_interrupted', 'gap_restarted'))
          )
    )
    OR EXISTS (
        SELECT 1
        FROM evolution_transitions t
        WHERE t.evolution_run_id = getvariable('evolution_run_id')
          AND t.transition_status = 'gap'
          AND (
              (t.gap_reason = 'curr_unavailable' AND (
                  SELECT count(*) FROM evolution_events ev
                  WHERE ev.evolution_run_id = t.evolution_run_id
                    AND ev.prev_ordinal = t.prev_ordinal
                    AND ev.event_type = 'gap_interrupted'
              ) <> (
                  SELECT count(*) FROM evolution_nodes n
                  WHERE n.evolution_run_id = t.evolution_run_id AND n.ordinal = t.prev_ordinal
              ))
              OR (t.gap_reason = 'prev_unavailable' AND (
                  SELECT count(*) FROM evolution_events ev
                  WHERE ev.evolution_run_id = t.evolution_run_id
                    AND ev.prev_ordinal = t.prev_ordinal
                    AND ev.event_type = 'gap_restarted'
              ) <> (
                  SELECT count(*) FROM evolution_nodes n
                  WHERE n.evolution_run_id = t.evolution_run_id AND n.ordinal = t.curr_ordinal
              ))
              OR (t.gap_reason = 'both_unavailable' AND EXISTS (
                  SELECT 1 FROM evolution_events ev
                  WHERE ev.evolution_run_id = t.evolution_run_id AND ev.prev_ordinal = t.prev_ordinal
              ))
          )
    )
    THEN error('INVARIANT_VIOLATION: E5 gap event disjointness or completeness violated')
END AS guard_e5;

-- E6: Stored node in/out primary/secondary degrees equal actual incident edge counts
SELECT CASE
    WHEN EXISTS (
        SELECT 1
        FROM evolution_nodes n
        WHERE n.evolution_run_id = getvariable('evolution_run_id')
          AND (
              n.in_degree_primary <> (
                  SELECT count(*) FROM evolution_edges e
                  WHERE e.evolution_run_id = n.evolution_run_id
                    AND e.curr_ordinal = n.ordinal AND e.curr_cluster_id = n.cluster_id
                    AND e.edge_role = 'primary'
              )
              OR n.in_degree_secondary <> (
                  SELECT count(*) FROM evolution_edges e
                  WHERE e.evolution_run_id = n.evolution_run_id
                    AND e.curr_ordinal = n.ordinal AND e.curr_cluster_id = n.cluster_id
                    AND e.edge_role <> 'primary'
              )
              OR n.out_degree_primary <> (
                  SELECT count(*) FROM evolution_edges e
                  WHERE e.evolution_run_id = n.evolution_run_id
                    AND e.prev_ordinal = n.ordinal AND e.prev_cluster_id = n.cluster_id
                    AND e.edge_role = 'primary'
              )
              OR n.out_degree_secondary <> (
                  SELECT count(*) FROM evolution_edges e
                  WHERE e.evolution_run_id = n.evolution_run_id
                    AND e.prev_ordinal = n.ordinal AND e.prev_cluster_id = n.cluster_id
                    AND e.edge_role <> 'primary'
              )
          )
    )
    THEN error('INVARIANT_VIOLATION: E6 node degree conservation violated')
END AS guard_e6;

-- E7: Branch lifecycle conservation across evolution_branch_summaries and evolution_nodes
SELECT CASE
    WHEN EXISTS (
        SELECT DISTINCT branch_id FROM evolution_nodes WHERE evolution_run_id = getvariable('evolution_run_id')
        EXCEPT
        SELECT branch_id FROM evolution_branch_summaries WHERE evolution_run_id = getvariable('evolution_run_id')
    )
    OR EXISTS (
        SELECT branch_id FROM evolution_branch_summaries WHERE evolution_run_id = getvariable('evolution_run_id')
        EXCEPT
        SELECT DISTINCT branch_id FROM evolution_nodes WHERE evolution_run_id = getvariable('evolution_run_id')
    )
    OR coalesce((
        SELECT sum(node_count) FROM evolution_branch_summaries WHERE evolution_run_id = getvariable('evolution_run_id')
    ), 0) <> (
        SELECT count(*) FROM evolution_nodes WHERE evolution_run_id = getvariable('evolution_run_id')
    )
    OR EXISTS (
        SELECT 1
        FROM evolution_branch_summaries br
        JOIN (
            SELECT
                branch_id,
                min(ordinal) AS min_ord,
                max(ordinal) AS max_ord,
                count(*) AS cnt
            FROM evolution_nodes
            WHERE evolution_run_id = getvariable('evolution_run_id')
            GROUP BY branch_id
        ) actual USING (branch_id)
        WHERE br.evolution_run_id = getvariable('evolution_run_id')
          AND (
              br.birth_ordinal <> actual.min_ord
              OR br.end_ordinal <> actual.max_ord
              OR br.node_count <> actual.cnt
              OR br.node_count <> br.end_ordinal - br.birth_ordinal + 1
              OR br.step_count + 1 <> br.node_count
          )
    )
    THEN error('INVARIANT_VIOLATION: E7 branch lifecycle conservation violated')
END AS guard_e7;

-- E8: Finite numeric values across all 6 M3 tables & non-empty non-physical disclaimer
SELECT CASE
    WHEN EXISTS (
        SELECT 1 FROM evolution_runs r
        WHERE r.evolution_run_id = getvariable('evolution_run_id')
          AND (
              length(trim(r.non_physical_disclaimer)) = 0
              OR NOT isfinite(r.chain_summary.cumulative_primary_distance)
              OR NOT isfinite(r.chain_summary.max_edge_distance)
              OR NOT isfinite(r.chain_summary.max_edge_distance_over_scale)
              OR NOT isfinite(r.chain_summary.mean_transition_tv_redistribution)
              OR NOT isfinite(r.chain_summary.max_transition_tv_redistribution)
              OR NOT isfinite(r.chain_summary.high_uncertainty_transition_ratio)
              OR NOT isfinite(r.chain_summary.high_uncertainty_edge_ratio)
              OR EXISTS (SELECT 1 FROM unnest(r.weights) uw(w) WHERE w IS NULL OR NOT isfinite(w))
          )
    )
    OR EXISTS (
        SELECT 1 FROM evolution_nodes n
        WHERE n.evolution_run_id = getvariable('evolution_run_id')
          AND (
              NOT isfinite(n.member_fraction)
              OR NOT isfinite(n.primary_delta_fraction)
              OR NOT isfinite(n.unattributed_inflow_fraction)
              OR NOT isfinite(n.unattributed_outflow_fraction)
              OR EXISTS (SELECT 1 FROM unnest(n.mean_features) uf(v) WHERE v IS NULL OR NOT isfinite(v))
          )
    )
    OR EXISTS (
        SELECT 1 FROM evolution_edges e
        WHERE e.evolution_run_id = getvariable('evolution_run_id')
          AND (
              length(trim(e.non_physical_disclaimer)) = 0
              OR NOT isfinite(e.distance)
              OR NOT isfinite(e.distance_over_scale)
              OR NOT isfinite(e.prev_member_fraction)
              OR NOT isfinite(e.curr_member_fraction)
              OR NOT isfinite(e.edge_delta_fraction)
              OR NOT isfinite(e.attributed_flow)
              OR NOT isfinite(e.edge_strength)
              OR NOT isfinite(e.threshold_margin)
              OR NOT isfinite(e.threshold_margin_ratio)
              OR NOT isfinite(e.optimality_margin)
              OR NOT isfinite(e.optimality_margin_ratio)
              OR NOT isfinite(e.secondary_margin)
              OR NOT isfinite(e.secondary_margin_ratio)
          )
    )
    OR EXISTS (
        SELECT 1 FROM evolution_events ev
        WHERE ev.evolution_run_id = getvariable('evolution_run_id')
          AND (
              length(trim(ev.non_physical_disclaimer)) = 0
              OR NOT isfinite(ev.max_morphology_distance)
              OR NOT isfinite(ev.max_morphology_over_scale)
              OR NOT isfinite(ev.prev_fraction_sum)
              OR NOT isfinite(ev.curr_fraction_sum)
              OR NOT isfinite(ev.net_delta_fraction)
              OR NOT isfinite(ev.attributed_flow_sum)
              OR NOT isfinite(ev.min_margin_ratio)
          )
    )
    OR EXISTS (
        SELECT 1 FROM evolution_branch_summaries br
        WHERE br.evolution_run_id = getvariable('evolution_run_id')
          AND (
              NOT isfinite(br.cumulative_morphology_distance)
              OR NOT isfinite(br.cumulative_morphology_over_scale)
              OR NOT isfinite(br.max_step_morphology_distance)
              OR NOT isfinite(br.max_step_morphology_over_scale)
              OR NOT isfinite(br.end_to_end_morphology_distance)
              OR NOT isfinite(br.initial_member_fraction)
              OR NOT isfinite(br.final_member_fraction)
              OR NOT isfinite(br.min_member_fraction)
              OR NOT isfinite(br.max_member_fraction)
              OR NOT isfinite(br.net_delta_fraction)
              OR NOT isfinite(br.cumulative_abs_delta_fraction)
              OR NOT isfinite(br.high_uncertainty_step_ratio)
          )
    )
    THEN error('INVARIANT_VIOLATION: E8 non-finite metric or missing non_physical_disclaimer')
END AS guard_e8;

COMMIT;
