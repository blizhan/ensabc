-- Full 8-run Real OM Replay Script
-- Executed via DuckDB CLI
-- Loads DuckOMO and ensabc_m0_extension, executes clustering, dispersion, matching,
-- and atomic publishing into the 5 tracking tables.

SET TimeZone = 'UTC';
SET autoinstall_known_extensions = false;
SET autoload_known_extensions = false;

-- Verify loaded extensions
SELECT extension_name, loaded, extension_version
FROM duckdb_extensions()
WHERE extension_name IN ('duckomo', 'ensabc_m0_extension')
ORDER BY extension_name;

-- Define read_om batch macro
CREATE OR REPLACE MACRO m2_read_om_batch(om_path, ord, init_ts, lead_hr) AS TABLE
SELECT
  ord::INTEGER                       AS ordinal,
  init_ts::TIMESTAMP                 AS init_time,
  lead_hr::INTEGER                   AS lead_hours,
  valid_time                         AS valid_time,
  member::BIGINT                     AS member_label,
  om_source.grid_id                  AS grid_id,
  om_source.parent_point_index::INT  AS point_index,
  lat::DOUBLE                        AS latitude,
  lon::DOUBLE                        AS longitude,
  temperature_2m::DOUBLE             AS value_k
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

-- Read all 8 batches from om_dir
CREATE OR REPLACE TEMP TABLE stage_real_cells AS
SELECT * FROM m2_read_om_batch(getvariable('om_dir') || '/wb2-ifs-ens-t2m-20200101T00Z.om', 0, '2020-01-01 00:00:00', 96)
UNION ALL
SELECT * FROM m2_read_om_batch(getvariable('om_dir') || '/wb2-ifs-ens-t2m-20200101T12Z.om', 1, '2020-01-01 12:00:00', 84)
UNION ALL
SELECT * FROM m2_read_om_batch(getvariable('om_dir') || '/wb2-ifs-ens-t2m-20200102T00Z.om', 2, '2020-01-02 00:00:00', 72)
UNION ALL
SELECT * FROM m2_read_om_batch(getvariable('om_dir') || '/wb2-ifs-ens-t2m-20200102T12Z.om', 3, '2020-01-02 12:00:00', 60)
UNION ALL
SELECT * FROM m2_read_om_batch(getvariable('om_dir') || '/wb2-ifs-ens-t2m-20200103T00Z.om', 4, '2020-01-03 00:00:00', 48)
UNION ALL
SELECT * FROM m2_read_om_batch(getvariable('om_dir') || '/wb2-ifs-ens-t2m-20200103T12Z.om', 5, '2020-01-03 12:00:00', 36)
UNION ALL
SELECT * FROM m2_read_om_batch(getvariable('om_dir') || '/wb2-ifs-ens-t2m-20200104T00Z.om', 6, '2020-01-04 00:00:00', 24)
UNION ALL
SELECT * FROM m2_read_om_batch(getvariable('om_dir') || '/wb2-ifs-ens-t2m-20200104T12Z.om', 7, '2020-01-04 12:00:00', 12);

-- Verification assertions on read cells (contracts/om-duckomo.md §3)
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
  count(DISTINCT c.point_index) AS point_count,
  count(DISTINCT c.grid_id) AS grid_count,
  min(c.grid_id) AS grid_id,
  bool_and(c.valid_time = TIMESTAMP '2020-01-05 00:00:00') AS valid_time_ok,
  bool_and(c.init_time + to_hours(c.lead_hours) = c.valid_time) AS init_lead_ok,
  bool_and(p.point_index IS NOT NULL AND c.latitude = p.exp_lat AND c.longitude = p.exp_lon) AS coords_ok
FROM stage_real_cells c
LEFT JOIN stage_expected_points p ON p.point_index = c.point_index
GROUP BY c.ordinal, c.init_time, c.lead_hours
ORDER BY c.ordinal;

-- Assert all 8 batches satisfy preparation gate
SELECT CASE WHEN count(*) = 0 AND (SELECT count(*) FROM stage_batch_checks) = 8
THEN true ELSE error('INVALID_BATCH_METADATA: preparation gate rejected batch') END AS preparation_ok
FROM stage_batch_checks
WHERE row_count != 450
   OR non_null_count != 450
   OR non_finite_count != 0
   OR distinct_member_points != 450
   OR member_count != 50
   OR point_count != 9
   OR grid_count != 1
   OR grid_id != 'ca3212142cd36c9704cd0aa1f47c24c905611eca7bb911427279997de133ad26'
   OR NOT valid_time_ok
   OR NOT init_lead_ok
   OR NOT coords_ok;

-- Aggregate features by member
CREATE OR REPLACE TEMP TABLE stage_real_members AS
SELECT
  ordinal,
  init_time,
  lead_hours,
  member_label,
  printf('ifs_ens_%02d', member_label) AS member_id,
  list(value_k ORDER BY point_index) AS features
FROM stage_real_cells
GROUP BY ordinal, init_time, lead_hours, member_label;

-- Create tasks for clustering and dispersion
CREATE OR REPLACE TEMP TABLE stage_real_tasks AS
SELECT
  ordinal,
  init_time,
  lead_hours,
  list(struct_pack(member_id := member_id, features := features) ORDER BY member_label) AS members
FROM stage_real_members
GROUP BY ordinal, init_time, lead_hours;

-- Execute clustering and dispersion
CREATE OR REPLACE TEMP TABLE stage_cluster AS
SELECT
  ordinal,
  ens_cluster(members, getvariable('cluster_config')) AS result
FROM stage_real_tasks;

CREATE OR REPLACE TEMP TABLE stage_real_dispersions AS
SELECT
  ordinal,
  ens_dispersion(members, getvariable('weights')) AS dispersion
FROM stage_real_tasks;

-- Stage batches metadata
CREATE OR REPLACE TEMP TABLE stage_batches AS
SELECT
  t.ordinal,
  t.init_time,
  t.lead_hours,
  'ok' AS status,
  NULL::VARCHAR AS status_reason,
  getvariable('execution_binding').input_manifest_sha256 AS input_sha256,
  getvariable('om_hashes')[t.ordinal + 1] AS om_sha256,
  d.dispersion,
  sha256(to_json(c.result)::VARCHAR) AS cluster_result_sha256
FROM stage_real_tasks t
JOIN stage_real_dispersions d USING (ordinal)
JOIN stage_cluster c USING (ordinal);

-- Execute Stage A and Stage B publish workflow
.read validation/m2/sql/stage_and_publish.sql

-- Return summary of published results
SELECT
  r.run_key,
  (SELECT count(*) FROM replay_batches WHERE run_key = r.run_key) AS total_batches,
  (SELECT count(*) FROM scenario_nodes WHERE run_key = r.run_key) AS total_nodes,
  (SELECT count(*) FROM pair_comparisons WHERE run_key = r.run_key) AS total_pairs,
  (SELECT count(*) FROM scenario_links WHERE run_key = r.run_key) AS total_links
FROM tracking_runs r
WHERE r.run_key = getvariable('run_key');
