//! M0/M1 DuckDB extension: exposes `ens_m0_echo` and `ens_cluster`.

#![allow(unsafe_op_in_unsafe_fn)]

use std::{
    ffi::{CString, c_char},
    panic::{AssertUnwindSafe, catch_unwind},
    ptr,
};

use duckdb::ffi::{
    DUCKDB_TYPE_DUCKDB_TYPE_BIGINT, DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN,
    DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE, DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR, DuckDBSuccess, duckdb_connect,
    duckdb_create_list_type, duckdb_create_logical_type, duckdb_create_scalar_function,
    duckdb_create_struct_type, duckdb_data_chunk, duckdb_data_chunk_get_size,
    duckdb_data_chunk_get_vector, duckdb_destroy_logical_type, duckdb_destroy_scalar_function,
    duckdb_function_info, duckdb_list_entry, duckdb_list_vector_get_child,
    duckdb_list_vector_get_size, duckdb_list_vector_reserve, duckdb_list_vector_set_size,
    duckdb_logical_type, duckdb_register_scalar_function, duckdb_scalar_function_add_parameter,
    duckdb_scalar_function_set_error, duckdb_scalar_function_set_function,
    duckdb_scalar_function_set_name, duckdb_scalar_function_set_return_type,
    duckdb_scalar_function_set_special_handling, duckdb_string_t, duckdb_string_t_data,
    duckdb_string_t_length, duckdb_struct_vector_get_child, duckdb_validity_row_is_valid,
    duckdb_vector, duckdb_vector_assign_string_element_len, duckdb_vector_ensure_validity_writable,
    duckdb_vector_get_data, duckdb_vector_get_validity, duckdb_vector_reference_vector,
};

use ensabc_core::{
    CoreError, InitMethod, MemberInput, ScenarioConfig, ScenarioResult, StopRule, TaskInput,
};

const MIN_DUCKDB_VERSION: &str = "v1.5.5";
const ECHO_FUNCTION_NAME: &str = "ens_m0_echo";
const CLUSTER_FUNCTION_NAME: &str = "ens_cluster";

// ---------------------------------------------------------------------------
// Error Handling
// ---------------------------------------------------------------------------

struct SqlError {
    code: &'static str,
    row: u64,
    message: String,
}

impl SqlError {
    fn new(code: &'static str, row: u64, message: impl Into<String>) -> Self {
        Self {
            code,
            row,
            message: message.into(),
        }
    }

    fn from_core(err: CoreError, row: u64) -> Self {
        let code = err.code.as_str();
        let mut msg = err.message;
        if let Some(m) = err.member_index {
            msg = format!("{msg} member_index={m}");
        }
        if let Some(f) = err.feature_index {
            msg = format!("{msg} feature_index={f}");
        }
        if let Some(r) = err.restart_index {
            msg = format!("{msg} restart_index={r}");
        }
        if let Some(it) = err.iteration {
            msg = format!("{msg} iteration={it}");
        }
        Self {
            code,
            row,
            message: msg,
        }
    }

    fn to_c_string(&self) -> CString {
        let text = format!(
            "ens_cluster [{}] chunk_row={}: {}",
            self.code, self.row, self.message
        );
        // Clean out any embedded NUL bytes
        let clean: String = text
            .chars()
            .map(|c| if c == '\0' { ' ' } else { c })
            .collect();
        CString::new(clean)
            .unwrap_or_else(|_| CString::new("ens_cluster [INTERNAL_ERROR]").unwrap())
    }
}

fn set_error_raw(info: duckdb_function_info, c_str: &CString) {
    unsafe {
        duckdb_scalar_function_set_error(info, c_str.as_ptr());
    }
}

fn row_is_valid(validity: *mut u64, row: u64) -> bool {
    if validity.is_null() {
        return true;
    }
    unsafe { duckdb_validity_row_is_valid(validity, row) }
}

unsafe fn read_string(vector: duckdb_vector, row: u64) -> String {
    unsafe {
        let data = duckdb_vector_get_data(vector) as *const duckdb_string_t;
        let string = *data.add(row as usize);
        let length = duckdb_string_t_length(string) as usize;
        let bytes = duckdb_string_t_data(ptr::from_ref(&string).cast_mut());
        let slice = std::slice::from_raw_parts(bytes.cast::<u8>(), length);
        String::from_utf8_lossy(slice).into_owned()
    }
}

// ---------------------------------------------------------------------------
// Type Builders
// ---------------------------------------------------------------------------

fn build_members_type() -> duckdb_logical_type {
    unsafe {
        let mut varchar = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR);
        let mut double = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE);
        let mut feature_list = duckdb_create_list_type(double);
        duckdb_destroy_logical_type(&mut double);

        let mut member_types = [varchar, feature_list];
        let member_id_name = CString::new("member_id").unwrap();
        let features_name = CString::new("features").unwrap();
        let mut member_names = [member_id_name.as_ptr(), features_name.as_ptr()];

        let mut member_struct = duckdb_create_struct_type(
            member_types.as_mut_ptr(),
            member_names.as_mut_ptr(),
            member_types.len() as u64,
        );

        duckdb_destroy_logical_type(&mut varchar);
        duckdb_destroy_logical_type(&mut feature_list);
        let members = duckdb_create_list_type(member_struct);
        duckdb_destroy_logical_type(&mut member_struct);
        members
    }
}

fn build_config_type() -> duckdb_logical_type {
    unsafe {
        let mut bigint_t = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_BIGINT);
        let mut double_t = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE);
        let mut varchar_t = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR);

        let mut d1_list = duckdb_create_list_type(double_t);
        let mut d2_list = duckdb_create_list_type(d1_list);
        let mut d3_list = duckdb_create_list_type(d2_list);

        let field_names = [
            CString::new("projection_dim").unwrap(),
            CString::new("cluster_count").unwrap(),
            CString::new("initialization").unwrap(),
            CString::new("seed").unwrap(),
            CString::new("restarts").unwrap(),
            CString::new("initial_centers").unwrap(),
            CString::new("stopping_rule").unwrap(),
            CString::new("tolerance").unwrap(),
            CString::new("max_iterations").unwrap(),
            CString::new("rank_threshold").unwrap(),
        ];
        let mut name_ptrs: Vec<*const c_char> = field_names.iter().map(|s| s.as_ptr()).collect();
        let mut types = [
            bigint_t, bigint_t, varchar_t, bigint_t, bigint_t, d3_list, varchar_t, double_t,
            bigint_t, double_t,
        ];

        let config_struct = duckdb_create_struct_type(
            types.as_mut_ptr(),
            name_ptrs.as_mut_ptr(),
            types.len() as u64,
        );

        duckdb_destroy_logical_type(&mut bigint_t);
        duckdb_destroy_logical_type(&mut double_t);
        duckdb_destroy_logical_type(&mut varchar_t);
        duckdb_destroy_logical_type(&mut d1_list);
        duckdb_destroy_logical_type(&mut d2_list);
        duckdb_destroy_logical_type(&mut d3_list);

        config_struct
    }
}

fn build_cluster_return_type() -> duckdb_logical_type {
    unsafe {
        let mut bigint_t = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_BIGINT);
        let mut double_t = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE);
        let mut bool_t = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN);
        let mut varchar_t = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR);

        let mut double_list = duckdb_create_list_type(double_t);
        let mut double_2d_list = duckdb_create_list_type(double_list);
        let mut bigint_list = duckdb_create_list_type(bigint_t);

        // 1. Members list: STRUCT(member_id VARCHAR, cluster_id BIGINT)[]
        let mem_names = [
            CString::new("member_id").unwrap(),
            CString::new("cluster_id").unwrap(),
        ];
        let mut mem_name_ptrs: Vec<*const c_char> = mem_names.iter().map(|s| s.as_ptr()).collect();
        let mut mem_types = [varchar_t, bigint_t];
        let mut mem_struct = duckdb_create_struct_type(
            mem_types.as_mut_ptr(),
            mem_name_ptrs.as_mut_ptr(),
            mem_types.len() as u64,
        );
        let mut ret_members_list = duckdb_create_list_type(mem_struct);
        duckdb_destroy_logical_type(&mut mem_struct);

        // 2. Scenarios list: STRUCT(cluster_id BIGINT, member_count BIGINT, member_fraction DOUBLE, mean_features DOUBLE[], representative_member_id VARCHAR, within_cluster_sse DOUBLE)[]
        let sc_names = [
            CString::new("cluster_id").unwrap(),
            CString::new("member_count").unwrap(),
            CString::new("member_fraction").unwrap(),
            CString::new("mean_features").unwrap(),
            CString::new("representative_member_id").unwrap(),
            CString::new("within_cluster_sse").unwrap(),
        ];
        let mut sc_name_ptrs: Vec<*const c_char> = sc_names.iter().map(|s| s.as_ptr()).collect();
        let mut sc_types = [
            bigint_t,
            bigint_t,
            double_t,
            double_list,
            varchar_t,
            double_t,
        ];
        let mut sc_struct = duckdb_create_struct_type(
            sc_types.as_mut_ptr(),
            sc_name_ptrs.as_mut_ptr(),
            sc_types.len() as u64,
        );
        let mut ret_scenarios_list = duckdb_create_list_type(sc_struct);
        duckdb_destroy_logical_type(&mut sc_struct);

        // 3. Restart summaries list
        let rs_names = [
            CString::new("restart_index").unwrap(),
            CString::new("initial_centers").unwrap(),
            CString::new("initial_member_indices").unwrap(),
            CString::new("iterations").unwrap(),
            CString::new("total_sse").unwrap(),
            CString::new("stop_reason").unwrap(),
        ];
        let mut rs_name_ptrs: Vec<*const c_char> = rs_names.iter().map(|s| s.as_ptr()).collect();
        let mut rs_types = [
            bigint_t,
            double_2d_list,
            bigint_list,
            bigint_t,
            double_t,
            varchar_t,
        ];
        let mut rs_struct = duckdb_create_struct_type(
            rs_types.as_mut_ptr(),
            rs_name_ptrs.as_mut_ptr(),
            rs_types.len() as u64,
        );
        let mut ret_rs_list = duckdb_create_list_type(rs_struct);
        duckdb_destroy_logical_type(&mut rs_struct);

        // 4. Diagnostics STRUCT
        let diag_names = [
            CString::new("input_member_count").unwrap(),
            CString::new("feature_count").unwrap(),
            CString::new("projection_dim").unwrap(),
            CString::new("cluster_count").unwrap(),
            CString::new("effective_rank").unwrap(),
            CString::new("normalization_mean").unwrap(),
            CString::new("normalization_scale").unwrap(),
            CString::new("pca_mean").unwrap(),
            CString::new("scaled").unwrap(),
            CString::new("singular_values").unwrap(),
            CString::new("components").unwrap(),
            CString::new("explained_variance").unwrap(),
            CString::new("explained_variance_ratio").unwrap(),
            CString::new("scores").unwrap(),
            CString::new("centers").unwrap(),
            CString::new("total_sse").unwrap(),
            CString::new("iterations").unwrap(),
            CString::new("converged").unwrap(),
            CString::new("stop_reason").unwrap(),
            CString::new("final_assignment_changed").unwrap(),
            CString::new("selected_restart").unwrap(),
            CString::new("restart_summaries").unwrap(),
            CString::new("distance_space").unwrap(),
        ];
        let mut diag_name_ptrs: Vec<*const c_char> =
            diag_names.iter().map(|s| s.as_ptr()).collect();
        let mut diag_types = [
            bigint_t,
            bigint_t,
            bigint_t,
            bigint_t,
            bigint_t,
            double_list,
            double_list,
            double_list,
            double_2d_list,
            double_list,
            double_2d_list,
            double_list,
            double_list,
            double_2d_list,
            double_2d_list,
            double_t,
            bigint_t,
            bool_t,
            varchar_t,
            bool_t,
            bigint_t,
            ret_rs_list,
            varchar_t,
        ];
        let mut ret_diag_struct = duckdb_create_struct_type(
            diag_types.as_mut_ptr(),
            diag_name_ptrs.as_mut_ptr(),
            diag_types.len() as u64,
        );

        // 5. Outer result STRUCT
        let mut cfg_type = build_config_type();
        let outer_names = [
            CString::new("algorithm_version").unwrap(),
            CString::new("config").unwrap(),
            CString::new("members").unwrap(),
            CString::new("scenarios").unwrap(),
            CString::new("diagnostics").unwrap(),
        ];
        let mut outer_name_ptrs: Vec<*const c_char> =
            outer_names.iter().map(|s| s.as_ptr()).collect();
        let mut outer_types = [
            varchar_t,
            cfg_type,
            ret_members_list,
            ret_scenarios_list,
            ret_diag_struct,
        ];

        let res_struct = duckdb_create_struct_type(
            outer_types.as_mut_ptr(),
            outer_name_ptrs.as_mut_ptr(),
            outer_types.len() as u64,
        );

        duckdb_destroy_logical_type(&mut bigint_t);
        duckdb_destroy_logical_type(&mut double_t);
        duckdb_destroy_logical_type(&mut bool_t);
        duckdb_destroy_logical_type(&mut varchar_t);
        duckdb_destroy_logical_type(&mut double_list);
        duckdb_destroy_logical_type(&mut double_2d_list);
        duckdb_destroy_logical_type(&mut bigint_list);
        duckdb_destroy_logical_type(&mut ret_members_list);
        duckdb_destroy_logical_type(&mut ret_scenarios_list);
        duckdb_destroy_logical_type(&mut ret_rs_list);
        duckdb_destroy_logical_type(&mut ret_diag_struct);
        duckdb_destroy_logical_type(&mut cfg_type);

        res_struct
    }
}

// ---------------------------------------------------------------------------
// Extraction from DuckDB input chunk
// ---------------------------------------------------------------------------

unsafe fn checked_list_entry(
    vector: duckdb_vector,
    index: u64,
    row: u64,
) -> Result<duckdb_list_entry, SqlError> {
    if !row_is_valid(duckdb_vector_get_validity(vector), index) {
        return Err(SqlError::new("NULL_INPUT", row, "nested list is NULL"));
    }
    let entry = *(duckdb_vector_get_data(vector) as *const duckdb_list_entry).add(index as usize);
    let end = entry
        .offset
        .checked_add(entry.length)
        .ok_or_else(|| SqlError::new("INVALID_VECTOR", row, "LIST range overflow"))?;
    if end > duckdb_list_vector_get_size(vector) || usize::try_from(end).is_err() {
        return Err(SqlError::new(
            "INVALID_VECTOR",
            row,
            "LIST range exceeds child size",
        ));
    }
    Ok(entry)
}

unsafe fn reserve_list(vector: duckdb_vector, size: usize) -> Result<(), &'static str> {
    let size = u64::try_from(size).map_err(|_| "LIST size exceeds u64")?;
    if duckdb_list_vector_reserve(vector, size) != DuckDBSuccess {
        return Err("failed to reserve LIST child");
    }
    if duckdb_list_vector_set_size(vector, size) != DuckDBSuccess {
        return Err("failed to set LIST child size");
    }
    Ok(())
}

unsafe fn extract_task(
    members_vec: duckdb_vector,
    config_vec: duckdb_vector,
    row: u64,
) -> Result<TaskInput, SqlError> {
    // 1. Validate top-level NULL
    if !row_is_valid(duckdb_vector_get_validity(members_vec), row) {
        return Err(SqlError::new("NULL_INPUT", row, "members list is NULL"));
    }
    if !row_is_valid(duckdb_vector_get_validity(config_vec), row) {
        return Err(SqlError::new("NULL_INPUT", row, "config is NULL"));
    }

    // 2. Extract members
    let entry = checked_list_entry(members_vec, row, row)?;
    let offset = entry.offset;
    let length = entry.length;

    if length == 0 {
        return Err(SqlError::new("EMPTY_MEMBERS", row, "members list is empty"));
    }

    let member_structs = duckdb_list_vector_get_child(members_vec);
    let ids_vec = duckdb_struct_vector_get_child(member_structs, 0);
    let feats_vec = duckdb_struct_vector_get_child(member_structs, 1);

    let m_validity = duckdb_vector_get_validity(member_structs);
    let id_validity = duckdb_vector_get_validity(ids_vec);
    let feat_validity = duckdb_vector_get_validity(feats_vec);

    let mut members = Vec::with_capacity(length as usize);
    let mut expected_features: Option<u64> = None;

    for m_idx in 0..length {
        let m_row = offset + m_idx;
        if !row_is_valid(m_validity, m_row) {
            return Err(SqlError::new(
                "NULL_INPUT",
                row,
                format!("member struct at index {m_idx} is NULL"),
            ));
        }
        if !row_is_valid(id_validity, m_row) {
            return Err(SqlError::new(
                "NULL_INPUT",
                row,
                format!("member_id at index {m_idx} is NULL"),
            ));
        }
        let member_id = read_string(ids_vec, m_row);
        if member_id.is_empty() {
            return Err(SqlError::new(
                "EMPTY_MEMBER_ID",
                row,
                format!("member_id at index {m_idx} is empty"),
            ));
        }

        if !row_is_valid(feat_validity, m_row) {
            return Err(SqlError::new(
                "NULL_INPUT",
                row,
                format!("features list at index {m_idx} is NULL"),
            ));
        }
        let f_entry = checked_list_entry(feats_vec, m_row, row)?;
        let f_len = f_entry.length;
        let f_offset = f_entry.offset;

        if f_len == 0 {
            return Err(SqlError::new(
                "EMPTY_FEATURES",
                row,
                format!("features list at index {m_idx} is empty"),
            ));
        }
        match expected_features {
            None => expected_features = Some(f_len),
            Some(exp) if exp != f_len => {
                return Err(SqlError::new(
                    "RAGGED_FEATURES",
                    row,
                    format!("member {m_idx} feature count {f_len} differs from {exp}"),
                ));
            }
            Some(_) => {}
        }

        let f_vals_vec = duckdb_list_vector_get_child(feats_vec);
        let f_vals_validity = duckdb_vector_get_validity(f_vals_vec);
        let f_vals_ptr = duckdb_vector_get_data(f_vals_vec) as *const f64;

        let mut features = Vec::with_capacity(f_len as usize);
        for f_idx in 0..f_len {
            let val_row = f_offset + f_idx;
            if !row_is_valid(f_vals_validity, val_row) {
                return Err(SqlError::new(
                    "NULL_INPUT",
                    row,
                    format!("feature value at member {m_idx}, feature {f_idx} is NULL"),
                ));
            }
            let val = *f_vals_ptr.add(val_row as usize);
            if !val.is_finite() {
                return Err(SqlError::new(
                    "NON_FINITE_INPUT",
                    row,
                    format!("non-finite feature value at member {m_idx}, feature {f_idx}: {val}"),
                ));
            }
            features.push(val);
        }

        members.push(MemberInput {
            member_index: m_idx as usize,
            member_id,
            features,
        });
    }

    // 3. Extract config
    let c_pdim_vec = duckdb_struct_vector_get_child(config_vec, 0);
    let c_k_vec = duckdb_struct_vector_get_child(config_vec, 1);
    let c_init_vec = duckdb_struct_vector_get_child(config_vec, 2);
    let c_seed_vec = duckdb_struct_vector_get_child(config_vec, 3);
    let c_restarts_vec = duckdb_struct_vector_get_child(config_vec, 4);
    let c_inits_vec = duckdb_struct_vector_get_child(config_vec, 5);
    let c_stop_vec = duckdb_struct_vector_get_child(config_vec, 6);
    let c_tol_vec = duckdb_struct_vector_get_child(config_vec, 7);
    let c_max_iter_vec = duckdb_struct_vector_get_child(config_vec, 8);
    let c_rank_thresh_vec = duckdb_struct_vector_get_child(config_vec, 9);

    for (c_idx, &child_vec) in [
        c_pdim_vec,
        c_k_vec,
        c_init_vec,
        c_seed_vec,
        c_restarts_vec,
        c_inits_vec,
        c_stop_vec,
        c_tol_vec,
        c_max_iter_vec,
        c_rank_thresh_vec,
    ]
    .iter()
    .enumerate()
    {
        if !row_is_valid(duckdb_vector_get_validity(child_vec), row) {
            return Err(SqlError::new(
                "NULL_INPUT",
                row,
                format!("config field {c_idx} is NULL"),
            ));
        }
    }

    let pdim = *(duckdb_vector_get_data(c_pdim_vec) as *const i64).add(row as usize);
    let k = *(duckdb_vector_get_data(c_k_vec) as *const i64).add(row as usize);
    let init_str = read_string(c_init_vec, row);
    let seed = *(duckdb_vector_get_data(c_seed_vec) as *const i64).add(row as usize);
    let restarts = *(duckdb_vector_get_data(c_restarts_vec) as *const i64).add(row as usize);
    let stop_str = read_string(c_stop_vec, row);
    let tol = *(duckdb_vector_get_data(c_tol_vec) as *const f64).add(row as usize);
    let max_iter = *(duckdb_vector_get_data(c_max_iter_vec) as *const i64).add(row as usize);
    let rank_thresh = *(duckdb_vector_get_data(c_rank_thresh_vec) as *const f64).add(row as usize);

    if pdim <= 0 || k <= 0 || restarts <= 0 || max_iter <= 0 || seed < 0 {
        return Err(SqlError::new(
            "INVALID_CONFIG",
            row,
            "numeric config parameters must be positive (seed >= 0)",
        ));
    }

    let init_method = match init_str.as_str() {
        "kmeans++_chacha8_v1" => InitMethod::KMeansPlusPlusChaCha8V1,
        "explicit_centers_v1" => InitMethod::ExplicitCentersV1,
        other => {
            return Err(SqlError::new(
                "INVALID_CONFIG",
                row,
                format!("unsupported initialization: {other}"),
            ));
        }
    };

    let stop_rule = match stop_str.as_str() {
        "sklearn_variance_scaled_v1" => StopRule::SklearnVarianceScaledV1,
        other => {
            return Err(SqlError::new(
                "INVALID_CONFIG",
                row,
                format!("unsupported stopping_rule: {other}"),
            ));
        }
    };

    // Extract initial_centers 3D list: DOUBLE[][][]
    let inits_entry = checked_list_entry(c_inits_vec, row, row)?;
    let mut initial_centers_3d = Vec::new();
    if inits_entry.length > 0 {
        let l2_vec = duckdb_list_vector_get_child(c_inits_vec);
        let l1_vec = duckdb_list_vector_get_child(l2_vec);
        let val_vec = duckdb_list_vector_get_child(l1_vec);
        let val_ptr = duckdb_vector_get_data(val_vec) as *const f64;

        for r_idx in 0..inits_entry.length {
            let l2_entry = checked_list_entry(l2_vec, inits_entry.offset + r_idx, row)?;
            let mut restart_centers = Vec::with_capacity(l2_entry.length as usize);
            for c_idx in 0..l2_entry.length {
                let l1_entry = checked_list_entry(l1_vec, l2_entry.offset + c_idx, row)?;
                let mut center = Vec::with_capacity(l1_entry.length as usize);
                for d_idx in 0..l1_entry.length {
                    let value_index = l1_entry.offset + d_idx;
                    if !row_is_valid(duckdb_vector_get_validity(val_vec), value_index) {
                        return Err(SqlError::new(
                            "NULL_INPUT",
                            row,
                            "initial center coordinate is NULL",
                        ));
                    }
                    let v = *val_ptr.add(value_index as usize);
                    center.push(v);
                }
                restart_centers.push(center);
            }
            initial_centers_3d.push(restart_centers);
        }
    }

    let config = ScenarioConfig {
        projection_dim: pdim as usize,
        cluster_count: k as usize,
        initialization: init_method,
        seed: seed as u64,
        restarts: restarts as usize,
        initial_centers: initial_centers_3d,
        stopping_rule: stop_rule,
        tolerance: tol,
        max_iterations: max_iter as usize,
        rank_threshold: rank_thresh,
    };

    Ok(TaskInput { config, members })
}

// ---------------------------------------------------------------------------
// Writing results into DuckDB output vectors
// ---------------------------------------------------------------------------

unsafe fn write_f64_list(
    list_vec: duckdb_vector,
    values: &[f64],
    base_offset: &mut u64,
) -> duckdb_list_entry {
    let offset = *base_offset;
    let length = values.len() as u64;
    let child_vec = duckdb_list_vector_get_child(list_vec);
    let child_data = duckdb_vector_get_data(child_vec) as *mut f64;
    for (i, &v) in values.iter().enumerate() {
        *child_data.add((offset as usize) + i) = v;
    }
    *base_offset += length;
    duckdb_list_entry { offset, length }
}

unsafe fn write_f64_2d_list(
    list_2d_vec: duckdb_vector,
    matrix: &[Vec<f64>],
    outer_offset: &mut u64,
    inner_offset: &mut u64,
) -> duckdb_list_entry {
    let offset = *outer_offset;
    let length = matrix.len() as u64;
    let inner_list_vec = duckdb_list_vector_get_child(list_2d_vec);
    let inner_entries = duckdb_vector_get_data(inner_list_vec) as *mut duckdb_list_entry;

    for (row_idx, row) in matrix.iter().enumerate() {
        let entry = write_f64_list(inner_list_vec, row, inner_offset);
        *inner_entries.add((offset as usize) + row_idx) = entry;
    }
    *outer_offset += length;
    duckdb_list_entry { offset, length }
}

unsafe fn write_f64_3d_list(
    list_3d_vec: duckdb_vector,
    tensor: &[Vec<Vec<f64>>],
    l3_offset: &mut u64,
    l2_offset: &mut u64,
    l1_offset: &mut u64,
) -> duckdb_list_entry {
    let offset = *l3_offset;
    let length = tensor.len() as u64;
    let l2_vec = duckdb_list_vector_get_child(list_3d_vec);
    let l2_entries = duckdb_vector_get_data(l2_vec) as *mut duckdb_list_entry;

    for (idx, mat) in tensor.iter().enumerate() {
        let entry = write_f64_2d_list(l2_vec, mat, l2_offset, l1_offset);
        *l2_entries.add((offset as usize) + idx) = entry;
    }
    *l3_offset += length;
    duckdb_list_entry { offset, length }
}

unsafe fn write_results_to_output(
    results: &[ScenarioResult],
    output: duckdb_vector,
) -> Result<(), &'static str> {
    let row_count = results.len();

    // Children of outer STRUCT:
    // 0: algorithm_version (VARCHAR)
    // 1: config (STRUCT)
    // 2: members (LIST)
    // 3: scenarios (LIST)
    // 4: diagnostics (STRUCT)
    let v_algo = duckdb_struct_vector_get_child(output, 0);
    let v_cfg = duckdb_struct_vector_get_child(output, 1);
    let v_members = duckdb_struct_vector_get_child(output, 2);
    let v_scenarios = duckdb_struct_vector_get_child(output, 3);
    let v_diag = duckdb_struct_vector_get_child(output, 4);

    // Make validity writable
    duckdb_vector_ensure_validity_writable(output);

    // 1. Precalculate capacities and reserve LIST vectors
    let mut total_members = 0usize;
    let mut total_scenarios = 0usize;
    let mut total_scenario_mean_features = 0usize;
    let mut total_norm_mean = 0usize;
    let mut total_scaled_rows = 0usize;
    let mut total_scaled_cells = 0usize;
    let mut total_components_rows = 0usize;
    let mut total_components_cells = 0usize;
    let mut total_scores_rows = 0usize;
    let mut total_scores_cells = 0usize;
    let mut total_centers_rows = 0usize;
    let mut total_centers_cells = 0usize;
    let mut total_restarts = 0usize;
    let mut total_restart_centers_rows = 0usize;
    let mut total_restart_centers_cells = 0usize;
    let mut total_restart_member_indices = 0usize;

    for res in results {
        let n = res.members.len();
        let k = res.scenarios.len();
        let d = res.config.projection_dim;
        let p = res.diagnostics.feature_count;
        total_members = total_members.checked_add(n).ok_or("LIST size overflow")?;
        total_scenarios = total_scenarios.checked_add(k).ok_or("LIST size overflow")?;
        total_scenario_mean_features = total_scenario_mean_features
            .checked_add(k.checked_mul(p).ok_or("LIST size overflow")?)
            .ok_or("LIST size overflow")?;
        total_norm_mean = total_norm_mean.checked_add(p).ok_or("LIST size overflow")?;
        total_scaled_rows = total_scaled_rows
            .checked_add(n)
            .ok_or("LIST size overflow")?;
        total_scaled_cells = total_scaled_cells
            .checked_add(n.checked_mul(p).ok_or("LIST size overflow")?)
            .ok_or("LIST size overflow")?;
        total_components_rows = total_components_rows
            .checked_add(d)
            .ok_or("LIST size overflow")?;
        total_components_cells = total_components_cells
            .checked_add(d.checked_mul(p).ok_or("LIST size overflow")?)
            .ok_or("LIST size overflow")?;
        total_scores_rows = total_scores_rows
            .checked_add(n)
            .ok_or("LIST size overflow")?;
        total_scores_cells = total_scores_cells
            .checked_add(n.checked_mul(d).ok_or("LIST size overflow")?)
            .ok_or("LIST size overflow")?;
        total_centers_rows = total_centers_rows
            .checked_add(k)
            .ok_or("LIST size overflow")?;
        total_centers_cells = total_centers_cells
            .checked_add(k.checked_mul(d).ok_or("LIST size overflow")?)
            .ok_or("LIST size overflow")?;
        let r_count = res.diagnostics.restarts.len();
        total_restarts = total_restarts
            .checked_add(r_count)
            .ok_or("LIST size overflow")?;
        for r in &res.diagnostics.restarts {
            total_restart_centers_rows = total_restart_centers_rows
                .checked_add(r.initial_centers.len())
                .ok_or("LIST size overflow")?;
            for c in &r.initial_centers {
                total_restart_centers_cells = total_restart_centers_cells
                    .checked_add(c.len())
                    .ok_or("LIST size overflow")?;
            }
            total_restart_member_indices = total_restart_member_indices
                .checked_add(r.initial_member_indices.len())
                .ok_or("LIST size overflow")?;
        }
    }

    // Reserve members LIST
    reserve_list(v_members, total_members)?;

    // Reserve scenarios LIST
    reserve_list(v_scenarios, total_scenarios)?;
    let sc_child = duckdb_list_vector_get_child(v_scenarios);
    let sc_mean_feats_vec = duckdb_struct_vector_get_child(sc_child, 3);
    reserve_list(sc_mean_feats_vec, total_scenario_mean_features)?;

    // Diagnostics LIST children:
    let d_norm_mean = duckdb_struct_vector_get_child(v_diag, 5);
    let d_norm_scale = duckdb_struct_vector_get_child(v_diag, 6);
    let d_pca_mean = duckdb_struct_vector_get_child(v_diag, 7);
    let d_scaled = duckdb_struct_vector_get_child(v_diag, 8);
    let d_s = duckdb_struct_vector_get_child(v_diag, 9);
    let d_comp = duckdb_struct_vector_get_child(v_diag, 10);
    let d_ev = duckdb_struct_vector_get_child(v_diag, 11);
    let d_evr = duckdb_struct_vector_get_child(v_diag, 12);
    let d_scores = duckdb_struct_vector_get_child(v_diag, 13);
    let d_centers = duckdb_struct_vector_get_child(v_diag, 14);
    let d_rs_list = duckdb_struct_vector_get_child(v_diag, 21);

    for list_vec in [d_norm_mean, d_norm_scale, d_pca_mean] {
        reserve_list(list_vec, total_norm_mean)?;
    }
    for list_vec in [d_s, d_ev, d_evr] {
        let total_d = results
            .iter()
            .try_fold(0usize, |sum, r| sum.checked_add(r.config.projection_dim))
            .ok_or("LIST size overflow")?;
        reserve_list(list_vec, total_d)?;
    }

    // 2D lists in diagnostics: scaled, components, scores, centers
    let setup_2d =
        |vec_2d: duckdb_vector, outer_cap: usize, inner_cap: usize| -> Result<(), &'static str> {
            reserve_list(vec_2d, outer_cap)?;
            let inner_l = duckdb_list_vector_get_child(vec_2d);
            reserve_list(inner_l, inner_cap)?;
            Ok(())
        };
    setup_2d(d_scaled, total_scaled_rows, total_scaled_cells)?;
    setup_2d(d_comp, total_components_rows, total_components_cells)?;
    setup_2d(d_scores, total_scores_rows, total_scores_cells)?;
    setup_2d(d_centers, total_centers_rows, total_centers_cells)?;

    // Restart summaries
    reserve_list(d_rs_list, total_restarts)?;
    let rs_struct = duckdb_list_vector_get_child(d_rs_list);
    let rs_centers_vec = duckdb_struct_vector_get_child(rs_struct, 1);
    let rs_indices_vec = duckdb_struct_vector_get_child(rs_struct, 2);
    setup_2d(
        rs_centers_vec,
        total_restart_centers_rows,
        total_restart_centers_cells,
    )?;
    reserve_list(rs_indices_vec, total_restart_member_indices)?;

    // Track write offsets
    let mut off_members = 0u64;
    let mut off_scenarios = 0u64;
    let mut off_sc_mean = 0u64;
    let mut off_norm_mean = 0u64;
    let mut off_norm_scale = 0u64;
    let mut off_pca_mean = 0u64;
    let mut off_s = 0u64;
    let mut off_ev = 0u64;
    let mut off_evr = 0u64;
    let mut off_scaled_outer = 0u64;
    let mut off_scaled_inner = 0u64;
    let mut off_comp_outer = 0u64;
    let mut off_comp_inner = 0u64;
    let mut off_scores_outer = 0u64;
    let mut off_scores_inner = 0u64;
    let mut off_centers_outer = 0u64;
    let mut off_centers_inner = 0u64;
    let mut off_rs = 0u64;
    let mut off_rs_centers_outer = 0u64;
    let mut off_rs_centers_inner = 0u64;
    let mut off_rs_indices = 0u64;

    // Outer config struct children:
    let c_pdim = duckdb_struct_vector_get_child(v_cfg, 0);
    let c_k = duckdb_struct_vector_get_child(v_cfg, 1);
    let c_init = duckdb_struct_vector_get_child(v_cfg, 2);
    let c_seed = duckdb_struct_vector_get_child(v_cfg, 3);
    let c_restarts = duckdb_struct_vector_get_child(v_cfg, 4);
    let c_inits = duckdb_struct_vector_get_child(v_cfg, 5);
    let c_stop = duckdb_struct_vector_get_child(v_cfg, 6);
    let c_tol = duckdb_struct_vector_get_child(v_cfg, 7);
    let c_max_iter = duckdb_struct_vector_get_child(v_cfg, 8);
    let c_rank_thresh = duckdb_struct_vector_get_child(v_cfg, 9);

    let mut config_outer = 0usize;
    let mut config_rows = 0usize;
    let mut config_cells = 0usize;
    for result in results {
        config_outer = config_outer
            .checked_add(result.config.initial_centers.len())
            .ok_or("LIST size overflow")?;
        for matrix in &result.config.initial_centers {
            config_rows = config_rows
                .checked_add(matrix.len())
                .ok_or("LIST size overflow")?;
            for values in matrix {
                config_cells = config_cells
                    .checked_add(values.len())
                    .ok_or("LIST size overflow")?;
            }
        }
    }
    reserve_list(c_inits, config_outer)?;
    let config_inner = duckdb_list_vector_get_child(c_inits);
    reserve_list(config_inner, config_rows)?;
    reserve_list(duckdb_list_vector_get_child(config_inner), config_cells)?;

    let mut off_cfg_inits_3 = 0u64;
    let mut off_cfg_inits_2 = 0u64;
    let mut off_cfg_inits_1 = 0u64;

    // Diagnostics scalar children:
    let d_n_members = duckdb_struct_vector_get_child(v_diag, 0);
    let d_n_feats = duckdb_struct_vector_get_child(v_diag, 1);
    let d_pdim = duckdb_struct_vector_get_child(v_diag, 2);
    let d_k = duckdb_struct_vector_get_child(v_diag, 3);
    let d_eff_rank = duckdb_struct_vector_get_child(v_diag, 4);
    let d_tot_sse = duckdb_struct_vector_get_child(v_diag, 15);
    let d_iters = duckdb_struct_vector_get_child(v_diag, 16);
    let d_conv = duckdb_struct_vector_get_child(v_diag, 17);
    let d_stop = duckdb_struct_vector_get_child(v_diag, 18);
    let d_changed = duckdb_struct_vector_get_child(v_diag, 19);
    let d_sel_restart = duckdb_struct_vector_get_child(v_diag, 20);
    let d_space = duckdb_struct_vector_get_child(v_diag, 22);

    for row in 0..row_count {
        let res = &results[row];

        // 1. algorithm_version
        duckdb_vector_assign_string_element_len(
            v_algo,
            row as u64,
            res.algorithm_version.as_ptr().cast(),
            res.algorithm_version.len() as u64,
        );

        // 2. config struct
        *(duckdb_vector_get_data(c_pdim) as *mut i64).add(row) = res.config.projection_dim as i64;
        *(duckdb_vector_get_data(c_k) as *mut i64).add(row) = res.config.cluster_count as i64;
        let init_name = match res.config.initialization {
            InitMethod::KMeansPlusPlusChaCha8V1 => "kmeans++_chacha8_v1",
            InitMethod::ExplicitCentersV1 => "explicit_centers_v1",
        };
        duckdb_vector_assign_string_element_len(
            c_init,
            row as u64,
            init_name.as_ptr().cast(),
            init_name.len() as u64,
        );
        *(duckdb_vector_get_data(c_seed) as *mut i64).add(row) = res.config.seed as i64;
        *(duckdb_vector_get_data(c_restarts) as *mut i64).add(row) = res.config.restarts as i64;
        let stop_name = match res.config.stopping_rule {
            StopRule::SklearnVarianceScaledV1 => "sklearn_variance_scaled_v1",
        };
        duckdb_vector_assign_string_element_len(
            c_stop,
            row as u64,
            stop_name.as_ptr().cast(),
            stop_name.len() as u64,
        );
        *(duckdb_vector_get_data(c_tol) as *mut f64).add(row) = res.config.tolerance;
        *(duckdb_vector_get_data(c_max_iter) as *mut i64).add(row) =
            res.config.max_iterations as i64;
        *(duckdb_vector_get_data(c_rank_thresh) as *mut f64).add(row) = res.config.rank_threshold;
        let c_inits_entry = write_f64_3d_list(
            c_inits,
            &res.config.initial_centers,
            &mut off_cfg_inits_3,
            &mut off_cfg_inits_2,
            &mut off_cfg_inits_1,
        );
        *(duckdb_vector_get_data(c_inits) as *mut duckdb_list_entry).add(row) = c_inits_entry;

        // 3. members list
        let m_start = off_members;
        let m_len = res.members.len() as u64;
        let m_child = duckdb_list_vector_get_child(v_members);
        let m_id_vec = duckdb_struct_vector_get_child(m_child, 0);
        let m_cid_vec = duckdb_struct_vector_get_child(m_child, 1);
        for (i, m) in res.members.iter().enumerate() {
            let m_row = m_start + (i as u64);
            duckdb_vector_assign_string_element_len(
                m_id_vec,
                m_row,
                m.member_id.as_ptr().cast(),
                m.member_id.len() as u64,
            );
            *(duckdb_vector_get_data(m_cid_vec) as *mut i64).add(m_row as usize) =
                m.cluster_id as i64;
        }
        off_members += m_len;
        *(duckdb_vector_get_data(v_members) as *mut duckdb_list_entry).add(row) =
            duckdb_list_entry {
                offset: m_start,
                length: m_len,
            };

        // 4. scenarios list
        let sc_start = off_scenarios;
        let sc_len = res.scenarios.len() as u64;
        let sc_cid_vec = duckdb_struct_vector_get_child(sc_child, 0);
        let sc_count_vec = duckdb_struct_vector_get_child(sc_child, 1);
        let sc_frac_vec = duckdb_struct_vector_get_child(sc_child, 2);
        let sc_rep_vec = duckdb_struct_vector_get_child(sc_child, 4);
        let sc_sse_vec = duckdb_struct_vector_get_child(sc_child, 5);

        for (i, sc) in res.scenarios.iter().enumerate() {
            let sc_row = sc_start + (i as u64);
            *(duckdb_vector_get_data(sc_cid_vec) as *mut i64).add(sc_row as usize) =
                sc.cluster_id as i64;
            *(duckdb_vector_get_data(sc_count_vec) as *mut i64).add(sc_row as usize) =
                sc.member_count as i64;
            *(duckdb_vector_get_data(sc_frac_vec) as *mut f64).add(sc_row as usize) =
                sc.member_fraction;
            let mean_entry = write_f64_list(sc_mean_feats_vec, &sc.mean_features, &mut off_sc_mean);
            *(duckdb_vector_get_data(sc_mean_feats_vec) as *mut duckdb_list_entry)
                .add(sc_row as usize) = mean_entry;
            duckdb_vector_assign_string_element_len(
                sc_rep_vec,
                sc_row,
                sc.representative_member_id.as_ptr().cast(),
                sc.representative_member_id.len() as u64,
            );
            *(duckdb_vector_get_data(sc_sse_vec) as *mut f64).add(sc_row as usize) = sc.cluster_sse;
        }
        off_scenarios += sc_len;
        *(duckdb_vector_get_data(v_scenarios) as *mut duckdb_list_entry).add(row) =
            duckdb_list_entry {
                offset: sc_start,
                length: sc_len,
            };

        // 5. diagnostics struct
        let diag = &res.diagnostics;
        *(duckdb_vector_get_data(d_n_members) as *mut i64).add(row) =
            diag.input_member_count as i64;
        *(duckdb_vector_get_data(d_n_feats) as *mut i64).add(row) = diag.feature_count as i64;
        *(duckdb_vector_get_data(d_pdim) as *mut i64).add(row) = diag.projection_dim as i64;
        *(duckdb_vector_get_data(d_k) as *mut i64).add(row) = diag.cluster_count as i64;
        *(duckdb_vector_get_data(d_eff_rank) as *mut i64).add(row) = diag.effective_rank as i64;

        let e_nm = write_f64_list(d_norm_mean, &diag.normalization_mean, &mut off_norm_mean);
        *(duckdb_vector_get_data(d_norm_mean) as *mut duckdb_list_entry).add(row) = e_nm;
        let e_ns = write_f64_list(d_norm_scale, &diag.normalization_scale, &mut off_norm_scale);
        *(duckdb_vector_get_data(d_norm_scale) as *mut duckdb_list_entry).add(row) = e_ns;
        let e_pm = write_f64_list(d_pca_mean, &diag.pca_mean, &mut off_pca_mean);
        *(duckdb_vector_get_data(d_pca_mean) as *mut duckdb_list_entry).add(row) = e_pm;

        let e_scaled = write_f64_2d_list(
            d_scaled,
            &diag.scaled,
            &mut off_scaled_outer,
            &mut off_scaled_inner,
        );
        *(duckdb_vector_get_data(d_scaled) as *mut duckdb_list_entry).add(row) = e_scaled;

        let e_s = write_f64_list(d_s, &diag.singular_values, &mut off_s);
        *(duckdb_vector_get_data(d_s) as *mut duckdb_list_entry).add(row) = e_s;

        let e_comp = write_f64_2d_list(
            d_comp,
            &diag.components,
            &mut off_comp_outer,
            &mut off_comp_inner,
        );
        *(duckdb_vector_get_data(d_comp) as *mut duckdb_list_entry).add(row) = e_comp;

        let e_ev = write_f64_list(d_ev, &diag.explained_variance, &mut off_ev);
        *(duckdb_vector_get_data(d_ev) as *mut duckdb_list_entry).add(row) = e_ev;

        let e_evr = write_f64_list(d_evr, &diag.explained_variance_ratio, &mut off_evr);
        *(duckdb_vector_get_data(d_evr) as *mut duckdb_list_entry).add(row) = e_evr;

        let e_scores = write_f64_2d_list(
            d_scores,
            &diag.scores,
            &mut off_scores_outer,
            &mut off_scores_inner,
        );
        *(duckdb_vector_get_data(d_scores) as *mut duckdb_list_entry).add(row) = e_scores;

        let e_centers = write_f64_2d_list(
            d_centers,
            &diag.centers,
            &mut off_centers_outer,
            &mut off_centers_inner,
        );
        *(duckdb_vector_get_data(d_centers) as *mut duckdb_list_entry).add(row) = e_centers;

        *(duckdb_vector_get_data(d_tot_sse) as *mut f64).add(row) = diag.total_sse;
        *(duckdb_vector_get_data(d_iters) as *mut i64).add(row) = diag.iterations as i64;
        *(duckdb_vector_get_data(d_conv) as *mut bool).add(row) = diag.converged;
        duckdb_vector_assign_string_element_len(
            d_stop,
            row as u64,
            diag.stop_reason.as_ptr().cast(),
            diag.stop_reason.len() as u64,
        );
        *(duckdb_vector_get_data(d_changed) as *mut bool).add(row) = diag.final_assignment_changed;
        *(duckdb_vector_get_data(d_sel_restart) as *mut i64).add(row) =
            diag.selected_restart as i64;

        // Restart summaries
        let rs_start = off_rs;
        let rs_len = diag.restarts.len() as u64;
        let rs_idx_vec = duckdb_struct_vector_get_child(rs_struct, 0);
        let rs_iter_vec = duckdb_struct_vector_get_child(rs_struct, 3);
        let rs_sse_vec = duckdb_struct_vector_get_child(rs_struct, 4);
        let rs_stop_vec = duckdb_struct_vector_get_child(rs_struct, 5);

        for (i, r) in diag.restarts.iter().enumerate() {
            let r_row = rs_start + (i as u64);
            *(duckdb_vector_get_data(rs_idx_vec) as *mut i64).add(r_row as usize) =
                r.restart_index as i64;
            let c_entry = write_f64_2d_list(
                rs_centers_vec,
                &r.initial_centers,
                &mut off_rs_centers_outer,
                &mut off_rs_centers_inner,
            );
            *(duckdb_vector_get_data(rs_centers_vec) as *mut duckdb_list_entry)
                .add(r_row as usize) = c_entry;

            let idx_off = off_rs_indices;
            let idx_len = r.initial_member_indices.len() as u64;
            let idx_child = duckdb_list_vector_get_child(rs_indices_vec);
            let idx_data = duckdb_vector_get_data(idx_child) as *mut i64;
            for (j, &m_idx) in r.initial_member_indices.iter().enumerate() {
                *idx_data.add((idx_off as usize) + j) = m_idx as i64;
            }
            off_rs_indices += idx_len;
            *(duckdb_vector_get_data(rs_indices_vec) as *mut duckdb_list_entry)
                .add(r_row as usize) = duckdb_list_entry {
                offset: idx_off,
                length: idx_len,
            };

            *(duckdb_vector_get_data(rs_iter_vec) as *mut i64).add(r_row as usize) =
                r.iterations as i64;
            *(duckdb_vector_get_data(rs_sse_vec) as *mut f64).add(r_row as usize) = r.total_sse;
            duckdb_vector_assign_string_element_len(
                rs_stop_vec,
                r_row,
                r.stop_reason.as_ptr().cast(),
                r.stop_reason.len() as u64,
            );
        }
        off_rs += rs_len;
        *(duckdb_vector_get_data(d_rs_list) as *mut duckdb_list_entry).add(row) =
            duckdb_list_entry {
                offset: rs_start,
                length: rs_len,
            };

        duckdb_vector_assign_string_element_len(
            d_space,
            row as u64,
            diag.distance_space.as_ptr().cast(),
            diag.distance_space.len() as u64,
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Scalar Function Callbacks
// ---------------------------------------------------------------------------

unsafe extern "C" fn cluster_callback(
    info: duckdb_function_info,
    input: duckdb_data_chunk,
    output: duckdb_vector,
) {
    let outcome = catch_unwind(AssertUnwindSafe(|| unsafe {
        let row_count = duckdb_data_chunk_get_size(input);
        if row_count == 0 {
            return Ok(());
        }

        let members_vec = duckdb_data_chunk_get_vector(input, 0);
        let config_vec = duckdb_data_chunk_get_vector(input, 1);

        if members_vec.is_null() {
            return Err(SqlError::new(
                "NULL_INPUT",
                0,
                "members argument vector is NULL",
            ));
        }
        if config_vec.is_null() {
            return Err(SqlError::new(
                "NULL_INPUT",
                0,
                "config argument vector is NULL",
            ));
        }

        let mut results = Vec::with_capacity(row_count as usize);

        for row in 0..row_count {
            let task = extract_task(members_vec, config_vec, row)?;
            let res = ensabc_core::cluster(&task).map_err(|e| SqlError::from_core(e, row))?;
            results.push(res);
        }

        write_results_to_output(&results, output)
            .map_err(|msg| SqlError::new("OUTPUT_ALLOCATION", 0, msg))?;

        Ok(())
    }));

    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            let c_str = err.to_c_string();
            set_error_raw(info, &c_str);
        }
        Err(_) => {
            let c_str =
                CString::new("ens_cluster [INTERNAL_PANIC] chunk_row=0: execution panicked")
                    .unwrap();
            set_error_raw(info, &c_str);
        }
    }
}

// ---------------------------------------------------------------------------
// Legacy ens_m0_echo implementation (retained for backward compatibility)
// ---------------------------------------------------------------------------

struct EchoValidationError {
    message: String,
}

impl EchoValidationError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

fn set_echo_error(info: duckdb_function_info, message: &str) {
    let text = format!("ens_m0_echo: {message}");
    let owned = match CString::new(text) {
        Ok(value) => value,
        Err(_) => CString::new("ens_m0_echo: error").unwrap(),
    };
    unsafe {
        duckdb_scalar_function_set_error(info, owned.as_ptr());
    }
}

fn validate_echo_task(members: duckdb_vector, row: u64) -> Result<(), EchoValidationError> {
    unsafe {
        let members_validity = duckdb_vector_get_validity(members);
        if !row_is_valid(members_validity, row) {
            return Err(EchoValidationError::new(format!(
                "task input layer: the members list of task row {row} is NULL"
            )));
        }

        let entries = duckdb_vector_get_data(members) as *const duckdb_list_entry;
        let entry = *entries.add(row as usize);
        let offset = entry.offset;
        let length = entry.length;

        if length == 0 {
            return Err(EchoValidationError::new(format!(
                "task input layer: task row {row} has an empty member list"
            )));
        }

        let member_structs = duckdb_list_vector_get_child(members);
        let ids = duckdb_struct_vector_get_child(member_structs, 0);
        let features = duckdb_struct_vector_get_child(member_structs, 1);

        let member_validity = duckdb_vector_get_validity(member_structs);
        let ids_validity = duckdb_vector_get_validity(ids);
        let features_validity = duckdb_vector_get_validity(features);

        let mut expected_feature_count: Option<u64> = None;
        let mut seen_ids: Vec<String> = Vec::new();

        for member_index in 0..length {
            let member_row = offset + member_index;
            if !row_is_valid(member_validity, member_row) {
                return Err(EchoValidationError::new(format!(
                    "task row {row} member {member_index}: the member item is NULL"
                )));
            }
            if !row_is_valid(ids_validity, member_row) {
                return Err(EchoValidationError::new(format!(
                    "task row {row} member {member_index}: member_id is NULL"
                )));
            }
            let member_id = read_string(ids, member_row);
            if member_id.is_empty() {
                return Err(EchoValidationError::new(format!(
                    "task row {row} member {member_index}: member_id is empty"
                )));
            }
            if seen_ids.iter().any(|seen| seen == &member_id) {
                return Err(EchoValidationError::new(format!(
                    "task row {row} member {member_index}: duplicate member_id {member_id:?}"
                )));
            }
            seen_ids.push(member_id);

            if !row_is_valid(features_validity, member_row) {
                return Err(EchoValidationError::new(format!(
                    "task row {row} member {member_index}: features is NULL"
                )));
            }
            let feature_entries = duckdb_vector_get_data(features) as *const duckdb_list_entry;
            let feature_entry = *feature_entries.add(member_row as usize);
            let feature_offset = feature_entry.offset;
            let feature_length = feature_entry.length;

            if feature_length == 0 {
                return Err(EchoValidationError::new(format!(
                    "task row {row} member {member_index}: features is empty"
                )));
            }

            match expected_feature_count {
                None => expected_feature_count = Some(feature_length),
                Some(expected) if expected != feature_length => {
                    return Err(EchoValidationError::new(format!(
                        "task row {row} member {member_index}: feature length {feature_length} differs from the task's {expected}"
                    )));
                }
                Some(_) => {}
            }

            let feature_values = duckdb_list_vector_get_child(features);
            let feature_values_validity = duckdb_vector_get_validity(feature_values);
            let values = duckdb_vector_get_data(feature_values) as *const f64;
            for feature_index in 0..feature_length {
                let value_row = feature_offset + feature_index;
                if !row_is_valid(feature_values_validity, value_row) {
                    return Err(EchoValidationError::new(format!(
                        "task row {row} member {member_index} feature {feature_index}: the feature value is NULL"
                    )));
                }
                let value = *values.add(value_row as usize);
                if !value.is_finite() {
                    return Err(EchoValidationError::new(format!(
                        "task row {row} member {member_index} feature {feature_index}: the feature value is non-finite ({value})"
                    )));
                }
            }
        }
        Ok(())
    }
}

unsafe extern "C" fn echo_callback(
    info: duckdb_function_info,
    input: duckdb_data_chunk,
    output: duckdb_vector,
) {
    let outcome = catch_unwind(AssertUnwindSafe(|| unsafe {
        let row_count = duckdb_data_chunk_get_size(input);
        if row_count == 0 {
            return Ok(());
        }

        let members = duckdb_data_chunk_get_vector(input, 0);
        if members.is_null() {
            return Err(EchoValidationError::new(
                "task input layer: the members argument vector is missing",
            ));
        }

        for row in 0..row_count {
            validate_echo_task(members, row)?;
        }

        duckdb_vector_reference_vector(output, members);
        Ok(())
    }));

    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(error)) => set_echo_error(info, &error.message),
        Err(_) => set_echo_error(
            info,
            "the internal validation panicked; the input was rejected rather than propagated",
        ),
    }
}

// ---------------------------------------------------------------------------
// Registration & C API Entry Point
// ---------------------------------------------------------------------------

unsafe fn register(connection: duckdb::ffi::duckdb_connection) -> Result<(), String> {
    unsafe {
        // 1. Register ens_m0_echo
        let echo_name = CString::new(ECHO_FUNCTION_NAME).unwrap();
        let mut echo_fn = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(echo_fn, echo_name.as_ptr());
        let mut echo_m_type = build_members_type();
        duckdb_scalar_function_add_parameter(echo_fn, echo_m_type);
        duckdb_scalar_function_set_return_type(echo_fn, echo_m_type);
        duckdb_destroy_logical_type(&mut echo_m_type);
        duckdb_scalar_function_set_special_handling(echo_fn);
        duckdb_scalar_function_set_function(echo_fn, Some(echo_callback));

        let state = duckdb_register_scalar_function(connection, echo_fn);
        duckdb_destroy_scalar_function(&mut echo_fn);
        if state != DuckDBSuccess {
            return Err(format!("failed to register {ECHO_FUNCTION_NAME}"));
        }

        // 2. Register ens_cluster
        let cluster_name = CString::new(CLUSTER_FUNCTION_NAME).unwrap();
        let mut cluster_fn = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(cluster_fn, cluster_name.as_ptr());

        let mut cluster_m_type = build_members_type();
        let mut cluster_cfg_type = build_config_type();
        let mut cluster_ret_type = build_cluster_return_type();

        duckdb_scalar_function_add_parameter(cluster_fn, cluster_m_type);
        duckdb_scalar_function_add_parameter(cluster_fn, cluster_cfg_type);
        duckdb_scalar_function_set_return_type(cluster_fn, cluster_ret_type);

        duckdb_destroy_logical_type(&mut cluster_m_type);
        duckdb_destroy_logical_type(&mut cluster_cfg_type);
        duckdb_destroy_logical_type(&mut cluster_ret_type);

        duckdb_scalar_function_set_special_handling(cluster_fn);
        duckdb_scalar_function_set_function(cluster_fn, Some(cluster_callback));

        let state2 = duckdb_register_scalar_function(connection, cluster_fn);
        duckdb_destroy_scalar_function(&mut cluster_fn);
        if state2 != DuckDBSuccess {
            return Err(format!("failed to register {CLUSTER_FUNCTION_NAME}"));
        }

        Ok(())
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn ensabc_m0_extension_init_c_api(
    info: duckdb::ffi::duckdb_extension_info,
    access: *const duckdb::ffi::duckdb_extension_access,
) -> bool {
    let init = || -> Result<(), String> {
        let have_api =
            unsafe { duckdb::ffi::duckdb_rs_extension_api_init(info, access, MIN_DUCKDB_VERSION) }
                .map_err(|message| message.to_string())?;
        if !have_api {
            return Err(
                "the DuckDB C API could not be initialized for this extension version".to_string(),
            );
        }

        let get_database = unsafe { (*access).get_database }
            .ok_or("the extension access struct has no get_database function")?;
        let database_ptr = unsafe { get_database(info) };
        if database_ptr.is_null() {
            return Err("DuckDB returned a null database handle for the extension".to_string());
        }
        let database: duckdb::ffi::duckdb_database = unsafe { *database_ptr };

        let mut connection: duckdb::ffi::duckdb_connection = ptr::null_mut();
        let state = unsafe { duckdb_connect(database, &mut connection) };
        if state != DuckDBSuccess {
            return Err("failed to open a connection for the extension registration".to_string());
        }

        let registration = unsafe { register(connection) };
        unsafe {
            duckdb::ffi::duckdb_disconnect(&mut connection);
        }
        registration
    };

    match init() {
        Ok(()) => true,
        Err(message) => {
            if let Some(set_error) = unsafe { (*access).set_error } {
                let text = CString::new(message).unwrap_or_else(|_| {
                    CString::new("the extension failed to initialize").unwrap()
                });
                unsafe {
                    set_error(info, text.as_ptr());
                }
            }
            false
        }
    }
}
