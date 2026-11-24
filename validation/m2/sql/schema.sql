-- M2 Five Formal Tracking Tables DDL
-- Implements contracts/sql-history.md schema with typed columns and integrity constraints.

CREATE TABLE IF NOT EXISTS tracking_runs (
    run_key             VARCHAR PRIMARY KEY,
    execution_binding   STRUCT(
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
                        ) NOT NULL,
    weights             DOUBLE[] NOT NULL,
    planned_batch_count INTEGER NOT NULL CHECK (planned_batch_count BETWEEN 4 AND 8),
    content_sha256      VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS replay_batches (
    run_key               VARCHAR NOT NULL REFERENCES tracking_runs(run_key),
    ordinal               INTEGER NOT NULL CHECK (ordinal >= 0),
    init_time             TIMESTAMP NOT NULL,
    lead_hours            INTEGER NOT NULL,
    status                VARCHAR NOT NULL CHECK (status IN ('ok', 'missing', 'failed')),
    status_reason         VARCHAR,
    input_sha256          VARCHAR,
    om_sha256             VARCHAR,
    dispersion            DOUBLE CHECK (dispersion IS NULL OR (isfinite(dispersion) AND dispersion > 0)),
    cluster_result_sha256 VARCHAR,
    PRIMARY KEY (run_key, ordinal),
    UNIQUE (run_key, init_time),
    CHECK ((status = 'ok') = (dispersion IS NOT NULL)),
    CHECK (status = 'ok' OR status_reason IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS scenario_nodes (
    run_key                  VARCHAR NOT NULL,
    ordinal                  INTEGER NOT NULL,
    cluster_id               INTEGER NOT NULL CHECK (cluster_id >= 0),
    canonical_rank           INTEGER NOT NULL CHECK (canonical_rank >= 0),
    mean_features            DOUBLE[] NOT NULL,
    member_count             INTEGER NOT NULL CHECK (member_count > 0),
    member_fraction          DOUBLE NOT NULL CHECK (member_fraction > 0 AND member_fraction <= 1),
    representative_member_id VARCHAR NOT NULL,
    within_cluster_sse       DOUBLE NOT NULL CHECK (isfinite(within_cluster_sse) AND within_cluster_sse >= 0),
    branch_id                VARCHAR NOT NULL,
    assignment_reason        VARCHAR NOT NULL CHECK (assignment_reason IN (
                               'first_batch', 'inherited', 'inherited_ambiguous',
                               'no_candidate_within_threshold', 'not_selected_in_global_optimum',
                               'previous_planned_batch_unavailable')),
    PRIMARY KEY (run_key, ordinal, cluster_id),
    UNIQUE (run_key, ordinal, branch_id),
    UNIQUE (run_key, ordinal, canonical_rank),
    FOREIGN KEY (run_key, ordinal) REFERENCES replay_batches(run_key, ordinal)
);

CREATE TABLE IF NOT EXISTS pair_comparisons (
    run_key        VARCHAR NOT NULL,
    prev_ordinal   INTEGER NOT NULL,
    curr_ordinal   INTEGER NOT NULL,
    pair_scale     DOUBLE NOT NULL CHECK (isfinite(pair_scale) AND pair_scale > 0),
    threshold      DOUBLE NOT NULL CHECK (isfinite(threshold) AND threshold >= 0),
    match_result   STRUCT(
                     algorithm_version VARCHAR,
                     prev_cluster_ids BIGINT[],
                     curr_cluster_ids BIGINT[],
                     prev_dispersion DOUBLE,
                     curr_dispersion DOUBLE,
                     pair_scale DOUBLE,
                     threshold_coefficient DOUBLE,
                     threshold DOUBLE,
                     distance_matrix DOUBLE[][],
                     distance_ratio_matrix DOUBLE[][],
                     eligible_matrix BOOLEAN[][],
                     edge_count BIGINT,
                     total_distance DOUBLE,
                     optimal_solution_count BIGINT,
                     ambiguous BOOLEAN,
                     threshold_margin DOUBLE,
                     optimality_margin DOUBLE,
                     has_optimality_margin BOOLEAN,
                     links STRUCT(
                       prev_cluster_id BIGINT, curr_cluster_id BIGINT,
                       distance DOUBLE, distance_ratio DOUBLE,
                       alternative_status VARCHAR, alternative_edge_count BIGINT,
                       alternative_total_distance DOUBLE, alternative_distance_increase DOUBLE,
                       alternative_distance_increase_ratio DOUBLE,
                       has_same_count_alternative BOOLEAN, ambiguous BOOLEAN
                     )[],
                     unmatched_prev STRUCT(
                       cluster_id BIGINT, reason VARCHAR,
                       min_candidate_distance DOUBLE, matched_in_some_optimum BOOLEAN
                     )[],
                     unmatched_curr STRUCT(
                       cluster_id BIGINT, reason VARCHAR,
                       min_candidate_distance DOUBLE, matched_in_some_optimum BOOLEAN
                     )[]
                   ) NOT NULL,
    PRIMARY KEY (run_key, prev_ordinal),
    UNIQUE (run_key, curr_ordinal),
    CHECK (curr_ordinal = prev_ordinal + 1),
    FOREIGN KEY (run_key, prev_ordinal) REFERENCES replay_batches(run_key, ordinal),
    FOREIGN KEY (run_key, curr_ordinal) REFERENCES replay_batches(run_key, ordinal)
);

CREATE TABLE IF NOT EXISTS scenario_links (
    run_key            VARCHAR NOT NULL,
    prev_ordinal       INTEGER NOT NULL,
    prev_cluster_id    INTEGER NOT NULL,
    curr_ordinal       INTEGER NOT NULL,
    curr_cluster_id    INTEGER NOT NULL,
    distance           DOUBLE NOT NULL CHECK (isfinite(distance) AND distance >= 0),
    distance_ratio     DOUBLE NOT NULL CHECK (isfinite(distance_ratio) AND distance_ratio >= 0),
    alternative_status VARCHAR NOT NULL CHECK (alternative_status IN ('tied', 'same_count', 'count_drop')),
    ambiguous          BOOLEAN NOT NULL,
    PRIMARY KEY (run_key, prev_ordinal, prev_cluster_id),
    UNIQUE (run_key, curr_ordinal, curr_cluster_id),
    CHECK (curr_ordinal = prev_ordinal + 1),
    FOREIGN KEY (run_key, prev_ordinal) REFERENCES pair_comparisons(run_key, prev_ordinal),
    FOREIGN KEY (run_key, prev_ordinal, prev_cluster_id) REFERENCES scenario_nodes(run_key, ordinal, cluster_id),
    FOREIGN KEY (run_key, curr_ordinal, curr_cluster_id) REFERENCES scenario_nodes(run_key, ordinal, cluster_id)
);
