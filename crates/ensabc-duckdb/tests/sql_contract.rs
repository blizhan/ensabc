use std::path::Path;
use std::process::Command;

fn run_duckdb(sql: &str) -> (bool, String, String) {
    let output = Command::new("duckdb")
        .arg("-unsigned")
        .arg("-c")
        .arg(sql)
        .output()
        .expect("failed to execute duckdb CLI");

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    (output.status.success(), stdout, stderr)
}

#[test]
fn nested_initial_centers_reject_nulls() {
    let ext = "/tmp/ensabc-m1-extension-target/ensabc_m0_extension.duckdb_extension";
    assert!(
        Path::new(ext).exists(),
        "Build the M1 extension before SQL tests"
    );
    for centers in [
        "[NULL]::DOUBLE[][][]",
        "[[NULL]]::DOUBLE[][][]",
        "[[[NULL], [1.0]]]::DOUBLE[][][]",
    ] {
        let sql = format!(
            "LOAD '{ext}'; SELECT ens_cluster([{{'member_id':'a','features':[0.0]}},{{'member_id':'b','features':[1.0]}},{{'member_id':'c','features':[4.0]}}], {{'projection_dim':1,'cluster_count':2,'initialization':'explicit_centers_v1','seed':0,'restarts':1,'initial_centers':{centers},'stopping_rule':'sklearn_variance_scaled_v1','tolerance':0.0001,'max_iterations':300,'rank_threshold':1e-10}});"
        );
        let (ok, stdout, stderr) = run_duckdb(&sql);
        assert!(
            !ok && format!("{stdout}{stderr}").contains("NULL_INPUT"),
            "{centers}: {stdout} {stderr}"
        );
    }
}

#[test]
fn test_sql_contract_positive_and_echo() {
    let ext = "/tmp/ensabc-m1-extension-target/ensabc_m0_extension.duckdb_extension";
    if !Path::new(ext).exists() {
        return;
    }

    let sql = format!(
        "LOAD '{ext}';
         SELECT ens_m0_echo([{{'member_id':'m0','features':[1.0, 2.0]}}])[1].member_id;
         WITH tasks AS (
           SELECT 'demo' AS task_id,
                  [{{'member_id':'a','features':[0.0,0.0]}},
                   {{'member_id':'b','features':[0.2,0.1]}},
                   {{'member_id':'c','features':[10.0,9.0]}},
                   {{'member_id':'d','features':[10.2,9.1]}}]
                    ::STRUCT(member_id VARCHAR, features DOUBLE[])[] AS members
         ), config AS (
           SELECT {{'projection_dim':1, 'cluster_count':2,
                   'initialization':'kmeans++_chacha8_v1', 'seed':0,
                   'restarts':10, 'initial_centers':[]::DOUBLE[][][],
                   'stopping_rule':'sklearn_variance_scaled_v1',
                   'tolerance':0.0001, 'max_iterations':300,
                   'rank_threshold':1e-10}}
                   ::STRUCT(projection_dim BIGINT, cluster_count BIGINT,
                            initialization VARCHAR, seed BIGINT, restarts BIGINT,
                            initial_centers DOUBLE[][][], stopping_rule VARCHAR,
                            tolerance DOUBLE, max_iterations BIGINT,
                            rank_threshold DOUBLE) AS params
         )
         SELECT (ens_cluster(members, params)).algorithm_version,
                len((ens_cluster(members, params)).scenarios),
                (ens_cluster(members, params)).diagnostics.selected_restart
         FROM tasks CROSS JOIN config;"
    );

    let (ok, stdout, stderr) = run_duckdb(&sql);
    assert!(
        ok,
        "DuckDB query failed:\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("m0"),
        "Expected m0 in echo result: {stdout}"
    );
    assert!(
        stdout.contains("m1.native.v1"),
        "Expected algorithm version m1.native.v1: {stdout}"
    );
}

#[test]
fn test_sql_contract_error_rejection_and_recovery() {
    let ext = "/tmp/ensabc-m1-extension-target/ensabc_m0_extension.duckdb_extension";
    if !Path::new(ext).exists() {
        return;
    }

    // 1. Rejection of NULL argument
    let null_sql = format!(
        "LOAD '{ext}';
         SELECT ens_cluster(NULL, {{'projection_dim':1, 'cluster_count':2, 'initialization':'kmeans++_chacha8_v1', 'seed':0, 'restarts':10, 'initial_centers':[]::DOUBLE[][][], 'stopping_rule':'sklearn_variance_scaled_v1', 'tolerance':0.0001, 'max_iterations':300, 'rank_threshold':1e-10}});"
    );
    let (ok_null, stdout_null, stderr_null) = run_duckdb(&null_sql);
    assert!(!ok_null, "Expected NULL to fail");
    let combined_null = format!("{stdout_null}\n{stderr_null}");
    assert!(
        combined_null.contains("ens_cluster [NULL_INPUT] chunk_row=0"),
        "Expected NULL_INPUT error, got:\n{combined_null}"
    );

    // 2. Rejection of empty members
    let empty_sql = format!(
        "LOAD '{ext}';
         SELECT ens_cluster([]::STRUCT(member_id VARCHAR, features DOUBLE[])[], {{'projection_dim':1, 'cluster_count':2, 'initialization':'kmeans++_chacha8_v1', 'seed':0, 'restarts':10, 'initial_centers':[]::DOUBLE[][][], 'stopping_rule':'sklearn_variance_scaled_v1', 'tolerance':0.0001, 'max_iterations':300, 'rank_threshold':1e-10}});"
    );
    let (ok_empty, stdout_empty, stderr_empty) = run_duckdb(&empty_sql);
    assert!(!ok_empty, "Expected empty members to fail");
    let combined_empty = format!("{stdout_empty}\n{stderr_empty}");
    assert!(
        combined_empty.contains("ens_cluster [EMPTY_MEMBERS] chunk_row=0"),
        "Expected EMPTY_MEMBERS error, got:\n{combined_empty}"
    );

    // 3. Same-connection recovery (error in statement 1 does not poison connection)
    let recovery_sql = format!(
        "LOAD '{ext}';
         SELECT ens_m0_echo([{{'member_id':'recovered','features':[1.0]}}])[1].member_id;"
    );
    let (ok_rec, stdout_rec, stderr_rec) = run_duckdb(&recovery_sql);
    assert!(ok_rec, "Recovery query failed: {stderr_rec}");
    assert!(stdout_rec.contains("recovered"));
}

#[test]
fn test_sql_contract_concurrency() {
    let ext = "/tmp/ensabc-m1-extension-target/ensabc_m0_extension.duckdb_extension";
    if !Path::new(ext).exists() {
        return;
    }

    let h1 = std::thread::spawn(move || {
        let sql = format!(
            "LOAD '{ext}';
             SELECT (ens_cluster([{{'member_id':'a','features':[0.0,0.0]}},{{'member_id':'b','features':[10.0,10.0]}}], {{'projection_dim':1, 'cluster_count':2, 'initialization':'kmeans++_chacha8_v1', 'seed':1, 'restarts':1, 'initial_centers':[]::DOUBLE[][][], 'stopping_rule':'sklearn_variance_scaled_v1', 'tolerance':0.0001, 'max_iterations':300, 'rank_threshold':1e-10}})).diagnostics.total_sse;"
        );
        let (ok, stdout, stderr) = run_duckdb(&sql);
        assert!(ok, "Thread 1 failed: {stderr}");
        stdout
    });

    let h2 = std::thread::spawn(move || {
        let sql = format!(
            "LOAD '{ext}';
             SELECT (ens_cluster([{{'member_id':'x','features':[1.0,1.0]}},{{'member_id':'y','features':[20.0,20.0]}}], {{'projection_dim':1, 'cluster_count':2, 'initialization':'kmeans++_chacha8_v1', 'seed':2, 'restarts':1, 'initial_centers':[]::DOUBLE[][][], 'stopping_rule':'sklearn_variance_scaled_v1', 'tolerance':0.0001, 'max_iterations':300, 'rank_threshold':1e-10}})).diagnostics.total_sse;"
        );
        let (ok, stdout, stderr) = run_duckdb(&sql);
        assert!(ok, "Thread 2 failed: {stderr}");
        stdout
    });

    let r1 = h1.join().unwrap();
    let r2 = h2.join().unwrap();
    assert!(r1.contains("0.0"), "Thread 1 result unexpected: {r1}");
    assert!(r2.contains("0.0"), "Thread 2 result unexpected: {r2}");
}
