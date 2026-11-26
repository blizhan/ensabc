-- Pure-SQL End-to-End 8-Run WeatherBench2 OM Scenario Evolution Script (T028)
-- Executed directly via DuckDB CLI with zero Python runtime dependency.
--
-- Standalone invocation example:
--   HOME=/tmp/isolated duckdb -unsigned -json -bail history.duckdb <<'SQL'
--   SET extension_directory = '/tmp/isolated/extensions';
--   SET autoinstall_known_extensions = false;
--   SET autoload_known_extensions = false;
--   LOAD '<path-to-duckomo-extension>';
--   LOAD '<path-to-ensabc-extension>';
--   SET VARIABLE om_dir = '/path/to/om_sample';
--   .read validation/m3/sql/real_8run_evolution.sql
--   SQL

SET TimeZone = 'UTC';
SET autoinstall_known_extensions = false;
SET autoload_known_extensions = false;

-- 1. Verify both DuckOMO and ensabc_m0_extension are loaded in this session
SELECT CASE
    WHEN (
        SELECT count(*)
        FROM duckdb_extensions()
        WHERE extension_name IN ('duckomo', 'ensabc_m0_extension') AND loaded
    ) = 2 THEN true
    ELSE error('MISSING_EXTENSION: both duckomo and ensabc_m0_extension must be loaded before real_8run_evolution.sql')
END AS extensions_loaded_gate;

SELECT CASE
    WHEN getvariable('om_dir') IS NULL OR length(trim(getvariable('om_dir')::VARCHAR)) = 0
    THEN error('INVALID_BATCH_METADATA: om_dir session variable is required')
    ELSE true
END AS om_dir_gate;

-- 2. Define read_om batch macro for the frozen WB2 2m_temperature (K) 50 x 3 x 3 EU sample
CREATE OR REPLACE MACRO m3_read_om_batch(om_path, ord, init_ts, lead_hr) AS TABLE
SELECT
    ord::INTEGER                      AS ordinal,
    init_ts::TIMESTAMP                AS init_time,
    lead_hr::INTEGER                  AS lead_hours,
    valid_time                        AS valid_time,
    member::BIGINT                    AS member_label,
    om_source.grid_id                 AS grid_id,
    om_source.parent_point_index::INT AS point_index,
    lat::DOUBLE                       AS latitude,
    lon::DOUBLE                       AS longitude,
    temperature_2m::DOUBLE            AS value_k
FROM read_om(
    om_path,
    variable       := 'temperature_2m',
    dimensions     := ['member', 'lat', 'lon'],
    grid           := {'nx': 3, 'ny': 3, 'lat0': 42.1875, 'lon0': 5.625,
                       'dlat': 5.625, 'dlon': 5.625, 'order': 'separate'},
    spatial_axes   := ['lat', 'lon'],
    axes           := {'member': {'axis': 'member', 'values': [
                         1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
                         21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,
                         41,42,43,44,45,46,47,48,49,50]}},
    include_source := true
);

-- 3. Read all 8 sequential forecast initialization batches from om_dir
CREATE OR REPLACE TEMP TABLE stage_real_cells AS
SELECT * FROM m3_read_om_batch(getvariable('om_dir') || '/wb2-ifs-ens-t2m-20200101T00Z.om', 0, '2020-01-01 00:00:00', 96)
UNION ALL
SELECT * FROM m3_read_om_batch(getvariable('om_dir') || '/wb2-ifs-ens-t2m-20200101T12Z.om', 1, '2020-01-01 12:00:00', 84)
UNION ALL
SELECT * FROM m3_read_om_batch(getvariable('om_dir') || '/wb2-ifs-ens-t2m-20200102T00Z.om', 2, '2020-01-02 00:00:00', 72)
UNION ALL
SELECT * FROM m3_read_om_batch(getvariable('om_dir') || '/wb2-ifs-ens-t2m-20200102T12Z.om', 3, '2020-01-02 12:00:00', 60)
UNION ALL
SELECT * FROM m3_read_om_batch(getvariable('om_dir') || '/wb2-ifs-ens-t2m-20200103T00Z.om', 4, '2020-01-03 00:00:00', 48)
UNION ALL
SELECT * FROM m3_read_om_batch(getvariable('om_dir') || '/wb2-ifs-ens-t2m-20200103T12Z.om', 5, '2020-01-03 12:00:00', 36)
UNION ALL
SELECT * FROM m3_read_om_batch(getvariable('om_dir') || '/wb2-ifs-ens-t2m-20200104T00Z.om', 6, '2020-01-04 00:00:00', 24)
UNION ALL
SELECT * FROM m3_read_om_batch(getvariable('om_dir') || '/wb2-ifs-ens-t2m-20200104T12Z.om', 7, '2020-01-04 12:00:00', 12);

-- 4. Preparation gate: 450 rows, 50 members x 9 ordered coordinates, Kelvin unit range,
--    common valid_time 2020-01-05T00:00:00Z, and init_time + lead_hours == valid_time
CREATE OR REPLACE TEMP TABLE stage_expected_points AS
SELECT * FROM (VALUES
    (0, 42.1875::DOUBLE,  5.625::DOUBLE),
    (1, 42.1875::DOUBLE, 11.25::DOUBLE),
    (2, 42.1875::DOUBLE, 16.875::DOUBLE),
    (3, 47.8125::DOUBLE,  5.625::DOUBLE),
    (4, 47.8125::DOUBLE, 11.25::DOUBLE),
    (5, 47.8125::DOUBLE, 16.875::DOUBLE),
    (6, 53.4375::DOUBLE,  5.625::DOUBLE),
    (7, 53.4375::DOUBLE, 11.25::DOUBLE),
    (8, 53.4375::DOUBLE, 16.875::DOUBLE)
) t(point_index, exp_lat, exp_lon);

CREATE OR REPLACE TEMP TABLE stage_batch_checks AS
SELECT
    c.ordinal,
    c.init_time,
    c.lead_hours,
    count(*) AS row_count,
    count(c.value_k) AS non_null_count,
    count(*) FILTER (WHERE NOT isfinite(c.value_k)) AS non_finite_count,
    count(DISTINCT (c.member_label, c.point_index)) AS distinct_member_points,
    count(DISTINCT c.member_label) AS member_count,
    min(c.member_label) AS min_member,
    max(c.member_label) AS max_member,
    count(DISTINCT c.point_index) AS point_count,
    count(DISTINCT c.grid_id) AS grid_count,
    min(c.grid_id) AS grid_id,
    bool_and(c.valid_time = TIMESTAMP '2020-01-05 00:00:00') AS valid_time_ok,
    bool_and(c.init_time + to_hours(c.lead_hours) = c.valid_time) AS init_lead_ok,
    bool_and(p.point_index IS NOT NULL AND c.latitude = p.exp_lat AND c.longitude = p.exp_lon) AS coords_ok,
    bool_and(c.value_k BETWEEN 180.0 AND 340.0) AS unit_kelvin_ok
FROM stage_real_cells c
LEFT JOIN stage_expected_points p ON p.point_index = c.point_index
GROUP BY c.ordinal, c.init_time, c.lead_hours
ORDER BY c.ordinal;

SELECT CASE
    WHEN count(*) = 0
     AND (SELECT count(*) FROM stage_batch_checks) = 8
     AND NOT EXISTS (
         SELECT 1 FROM (
             SELECT init_time, lag(init_time) OVER (ORDER BY ordinal) AS prev_init
             FROM stage_batch_checks
         ) WHERE prev_init IS NOT NULL AND init_time <> prev_init + INTERVAL '12 hours'
     )
    THEN true
    ELSE error('INVALID_BATCH_METADATA: preparation gate rejected batch')
END AS preparation_ok
FROM stage_batch_checks
WHERE row_count <> 450
   OR non_null_count <> 450
   OR non_finite_count <> 0
   OR distinct_member_points <> 450
   OR member_count <> 50
   OR min_member <> 1
   OR max_member <> 50
   OR point_count <> 9
   OR grid_count <> 1
   OR grid_id <> 'ca3212142cd36c9704cd0aa1f47c24c905611eca7bb911427279997de133ad26'
   OR NOT valid_time_ok
   OR NOT init_lead_ok
   OR NOT coords_ok
   OR NOT unit_kelvin_ok;

-- 5. Default frozen configuration & binding variables when running in standalone zero-Python CLI mode
SET VARIABLE weights = CASE
    WHEN getvariable('weights') IS NOT NULL THEN getvariable('weights')::DOUBLE[]
    ELSE [
        0.7409511253549591::DOUBLE,
        0.7409511253549591::DOUBLE,
        0.7409511253549591::DOUBLE,
        0.6715589548470183::DOUBLE,
        0.6715589548470183::DOUBLE,
        0.6715589548470183::DOUBLE,
        0.5956993044924334::DOUBLE,
        0.5956993044924334::DOUBLE,
        0.5956993044924334::DOUBLE
    ]::DOUBLE[]
END;

SET VARIABLE threshold_coefficient = coalesce(getvariable('threshold_coefficient')::DOUBLE, 1.0::DOUBLE);
SET VARIABLE planned_batch_count = coalesce(getvariable('planned_batch_count')::INTEGER, 8);

SET VARIABLE cluster_config = CASE
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
        'cluster_count': 3::BIGINT,
        'initialization': 'kmeans++_chacha8_v1'::VARCHAR,
        'seed': 3::BIGINT,
        'restarts': 10::BIGINT,
        'initial_centers': []::DOUBLE[][][],
        'stopping_rule': 'sklearn_variance_scaled_v1'::VARCHAR,
        'tolerance': 0.0001::DOUBLE,
        'max_iterations': 300::BIGINT,
        'rank_threshold': 1e-10::DOUBLE
    }
END;

SET VARIABLE om_hashes = CASE
    WHEN getvariable('om_hashes') IS NOT NULL THEN getvariable('om_hashes')::VARCHAR[]
    ELSE (
        SELECT list(batch_sha ORDER BY ordinal ASC)
        FROM (
            SELECT
                b.ordinal,
                sha256(to_json(list(c.value_k ORDER BY c.member_label, c.point_index))::VARCHAR) AS batch_sha
            FROM stage_batch_checks b
            JOIN stage_real_cells c USING (ordinal)
            GROUP BY b.ordinal
        )
    )
END;

SET VARIABLE standalone_input_sha = sha256(to_json(getvariable('om_hashes'))::VARCHAR);
SET VARIABLE standalone_duckomo_ver = coalesce(
    (SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'duckomo' AND loaded LIMIT 1),
    'unknown'
);

SET VARIABLE execution_binding = CASE
    WHEN getvariable('execution_binding') IS NOT NULL THEN (
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
    )
    ELSE {
        'run_id': sha256('m2:' || getvariable('standalone_input_sha') || ':' || to_json(getvariable('cluster_config'))::VARCHAR || ':' || getvariable('threshold_coefficient')::VARCHAR || ':' || getvariable('standalone_duckomo_ver')),
        'input_manifest_sha256': getvariable('standalone_input_sha'),
        'scenario_config_sha256': sha256(to_json(getvariable('cluster_config'))::VARCHAR),
        'match_config_sha256': sha256(to_json({'threshold_coefficient': getvariable('threshold_coefficient'), 'algorithm': 'm2.match.v1'})::VARCHAR),
        'weights_version': sha256(to_json(getvariable('weights'))::VARCHAR),
        'threshold_coefficient': getvariable('threshold_coefficient')::DOUBLE,
        'cluster_count': getvariable('cluster_config').cluster_count::INTEGER,
        'software': {
            'ensabc_extension_sha256': 'standalone_cli',
            'duckomo_version': getvariable('standalone_duckomo_ver'),
            'duckomo_sha256': 'standalone_cli',
            'duckdb_version': 'v1.5.5',
            'cluster_algorithm_version': 'm1.native.v1',
            'match_algorithm_version': 'm2.match.v1'
        }
    }
END;

SET VARIABLE run_key = coalesce(getvariable('run_key')::VARCHAR, getvariable('execution_binding').run_id);

SET VARIABLE evolution_binding = CASE
    WHEN getvariable('evolution_binding') IS NOT NULL THEN (
        getvariable('evolution_binding')::STRUCT(
            tracking_run_key VARCHAR,
            input_manifest_sha256 VARCHAR,
            scenario_config_sha256 VARCHAR,
            match_config_sha256 VARCHAR,
            evolve_config_sha256 VARCHAR,
            weights_version VARCHAR,
            secondary_rule VARCHAR,
            flow_rule VARCHAR,
            uncertainty_rule VARCHAR,
            fraction_sum_tolerance DOUBLE,
            high_uncertainty_threshold_ratio DOUBLE,
            high_uncertainty_optimality_ratio DOUBLE,
            high_uncertainty_secondary_ratio DOUBLE,
            software STRUCT(
                ensabc_extension_sha256 VARCHAR,
                duckomo_version VARCHAR,
                duckomo_sha256 VARCHAR,
                duckdb_version VARCHAR,
                cluster_algorithm_version VARCHAR,
                match_algorithm_version VARCHAR,
                evolution_algorithm_version VARCHAR
            )
        )
    )
    ELSE {
        'tracking_run_key': getvariable('run_key'),
        'input_manifest_sha256': getvariable('execution_binding').input_manifest_sha256,
        'scenario_config_sha256': getvariable('execution_binding').scenario_config_sha256,
        'match_config_sha256': getvariable('execution_binding').match_config_sha256,
        'evolve_config_sha256': sha256('nearest_feasible_v1:star_min_mass_proportional_v1:m3.uncertainty.v1:1e-9:0.1:0.1:0.1:m3.evolve.v1'),
        'weights_version': getvariable('execution_binding').weights_version,
        'secondary_rule': 'nearest_feasible_v1',
        'flow_rule': 'star_min_mass_proportional_v1',
        'uncertainty_rule': 'm3.uncertainty.v1',
        'fraction_sum_tolerance': 1e-9::DOUBLE,
        'high_uncertainty_threshold_ratio': 0.10::DOUBLE,
        'high_uncertainty_optimality_ratio': 0.10::DOUBLE,
        'high_uncertainty_secondary_ratio': 0.10::DOUBLE,
        'software': {
            'ensabc_extension_sha256': getvariable('execution_binding').software.ensabc_extension_sha256,
            'duckomo_version': getvariable('execution_binding').software.duckomo_version,
            'duckomo_sha256': getvariable('execution_binding').software.duckomo_sha256,
            'duckdb_version': getvariable('execution_binding').software.duckdb_version,
            'cluster_algorithm_version': 'm1.native.v1',
            'match_algorithm_version': 'm2.match.v1',
            'evolution_algorithm_version': 'm3.evolve.v1'
        }
    }
END;

SET VARIABLE evolution_run_id = coalesce(
    getvariable('evolution_run_id')::VARCHAR,
    sha256(to_json(getvariable('evolution_binding'))::VARCHAR)
);

-- 6. Aggregate 50 ensemble members x 9 spatial features per batch into stage_raw_batches
CREATE OR REPLACE TEMP TABLE stage_real_members AS
SELECT
    ordinal,
    init_time,
    lead_hours,
    member_label,
    printf('ifs_ens_%02d', member_label) AS member_id,
    list(value_k ORDER BY point_index ASC) AS features
FROM stage_real_cells
GROUP BY ordinal, init_time, lead_hours, member_label;

CREATE OR REPLACE TEMP TABLE stage_raw_batches AS
SELECT
    ordinal,
    init_time,
    lead_hours,
    'present'::VARCHAR AS raw_status,
    NULL::VARCHAR AS status_reason,
    getvariable('execution_binding').input_manifest_sha256::VARCHAR AS input_sha256,
    getvariable('om_hashes')[ordinal + 1]::VARCHAR AS om_sha256,
    list(struct_pack(member_id := member_id, features := features) ORDER BY member_label ASC) AS members
FROM stage_real_members
GROUP BY ordinal, init_time, lead_hours
ORDER BY ordinal ASC;

-- 7. Execute Two-Stage M2+M3 11-Table Atomic Publish Workflow
.read validation/m3/sql/stage_and_publish.sql

-- 8. Return published 11-table summary
SELECT
    er.evolution_run_id,
    er.tracking_run_key,
    er.content_sha256,
    (SELECT count(*) FROM replay_batches WHERE run_key = er.tracking_run_key) AS total_batches,
    (SELECT count(*) FROM scenario_nodes WHERE run_key = er.tracking_run_key) AS m2_nodes,
    (SELECT count(*) FROM pair_comparisons WHERE run_key = er.tracking_run_key) AS m2_pairs,
    (SELECT count(*) FROM scenario_links WHERE run_key = er.tracking_run_key) AS m2_links,
    (SELECT count(*) FROM evolution_nodes WHERE evolution_run_id = er.evolution_run_id) AS m3_nodes,
    (SELECT count(*) FROM evolution_transitions WHERE evolution_run_id = er.evolution_run_id) AS m3_transitions,
    (SELECT count(*) FROM evolution_edges WHERE evolution_run_id = er.evolution_run_id) AS m3_edges,
    (SELECT count(*) FROM evolution_events WHERE evolution_run_id = er.evolution_run_id) AS m3_events,
    (SELECT count(*) FROM evolution_branch_summaries WHERE evolution_run_id = er.evolution_run_id) AS m3_branches,
    er.chain_summary
FROM evolution_runs er
WHERE er.evolution_run_id = getvariable('evolution_run_id');
