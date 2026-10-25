-- M1 real sample SQL query template
-- Computes scenarios using ens_cluster, saves typed result table, and unnests scenarios and members.

LOAD '{{EXTENSION_PATH}}';

CREATE TABLE raw_task AS SELECT * FROM read_json_auto('{{TASK_JSON_PATH}}');

CREATE TABLE task_input AS
WITH member_rows AS (
  SELECT unnest(members) AS m FROM raw_task
)
SELECT
  'real-ifs-ens-20200101-t2m' AS task_id,
  (
    SELECT list(struct_pack(member_id := m.member_id, features := m.features) ORDER BY m.member_index)
    FROM member_rows
  ) AS members,
  struct_pack(
    projection_dim := raw_task.config.projection_dim,
    cluster_count := raw_task.config.cluster_count,
    initialization := raw_task.config.initialization,
    seed := raw_task.config.seed,
    restarts := raw_task.config.restarts,
    initial_centers := CAST([] AS DOUBLE[][][]),
    stopping_rule := raw_task.config.stopping_rule,
    tolerance := raw_task.config.tolerance,
    max_iterations := raw_task.config.max_iterations,
    rank_threshold := raw_task.config.rank_threshold
  ) AS config
FROM raw_task;

CREATE TABLE cluster_results AS
SELECT task_id, ens_cluster(members, config) AS result
FROM task_input;

CREATE TABLE result_members AS
SELECT task_id, unnest(result.members) AS member
FROM cluster_results;

CREATE TABLE result_scenarios AS
SELECT task_id, unnest(result.scenarios) AS scenario
FROM cluster_results;
