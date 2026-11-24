//! M2 DuckDB extension: exposes `ens_dispersion` and `ens_match_scenarios`.

#![allow(unsafe_op_in_unsafe_fn)]

use std::{
    ffi::{CString, c_char},
    panic::{AssertUnwindSafe, catch_unwind},
    ptr,
};

use duckdb::ffi::{
    DUCKDB_TYPE_DUCKDB_TYPE_BIGINT, DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN,
    DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE, DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR, DuckDBSuccess,
    duckdb_connection, duckdb_create_list_type, duckdb_create_logical_type,
    duckdb_create_scalar_function, duckdb_create_struct_type, duckdb_data_chunk,
    duckdb_data_chunk_get_size, duckdb_data_chunk_get_vector, duckdb_destroy_logical_type,
    duckdb_destroy_scalar_function, duckdb_function_info, duckdb_list_entry,
    duckdb_list_vector_get_child, duckdb_list_vector_get_size, duckdb_list_vector_reserve,
    duckdb_list_vector_set_size, duckdb_logical_type, duckdb_register_scalar_function,
    duckdb_scalar_function_add_parameter, duckdb_scalar_function_set_error,
    duckdb_scalar_function_set_function, duckdb_scalar_function_set_name,
    duckdb_scalar_function_set_return_type, duckdb_scalar_function_set_special_handling,
    duckdb_string_t, duckdb_string_t_data, duckdb_string_t_length, duckdb_struct_vector_get_child,
    duckdb_validity_row_is_valid, duckdb_vector, duckdb_vector_assign_string_element_len,
    duckdb_vector_ensure_validity_writable, duckdb_vector_get_data, duckdb_vector_get_validity,
};

use ensabc_core::{
    CoreError, MatchResult, MemberInput, ScaledMatchInput, ScenarioNodeInput, Substitutability,
    ensemble_dispersion, match_scenarios_scaled,
};

const DISPERSION_FUNCTION_NAME: &str = "ens_dispersion";
const MATCH_SCENARIOS_FUNCTION_NAME: &str = "ens_match_scenarios";
const MATCH_ALGORITHM_VERSION: &str = "m2.match.v1";

// ---------------------------------------------------------------------------
// Error Handling
// ---------------------------------------------------------------------------

struct SqlMatchError {
    func: &'static str,
    code: &'static str,
    row: u64,
    message: String,
}

impl SqlMatchError {
    fn new(func: &'static str, code: &'static str, row: u64, message: impl Into<String>) -> Self {
        Self {
            func,
            code,
            row,
            message: message.into(),
        }
    }

    fn from_core(func: &'static str, err: CoreError, row: u64) -> Self {
        let code = err.code.as_str();
        let mut msg = err.message;
        if let Some(m) = err.member_index {
            msg = format!("{msg} member_index={m}");
        }
        if let Some(p) = err.prev_index {
            msg = format!("{msg} prev_index={p}");
        }
        if let Some(c) = err.curr_index {
            msg = format!("{msg} curr_index={c}");
        }
        if let Some(f) = err.feature_index {
            msg = format!("{msg} feature_index={f}");
        }
        Self {
            func,
            code,
            row,
            message: msg,
        }
    }

    fn to_c_string(&self) -> CString {
        let text = format!(
            "{} [{}] chunk_row={}: {}",
            self.func, self.code, self.row, self.message
        );
        let clean: String = text
            .chars()
            .map(|c| if c == '\0' { ' ' } else { c })
            .collect();
        CString::new(clean).unwrap_or_else(|_| CString::new("ens_m2 [INTERNAL_ERROR]").unwrap())
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
    let data = duckdb_vector_get_data(vector) as *const duckdb_string_t;
    let string = *data.add(row as usize);
    let length = duckdb_string_t_length(string) as usize;
    let bytes = duckdb_string_t_data(ptr::from_ref(&string).cast_mut());
    let slice = std::slice::from_raw_parts(bytes.cast::<u8>(), length);
    String::from_utf8_lossy(slice).into_owned()
}

unsafe fn checked_list_entry(
    func: &'static str,
    vector: duckdb_vector,
    index: u64,
    row: u64,
) -> Result<duckdb_list_entry, SqlMatchError> {
    if !row_is_valid(duckdb_vector_get_validity(vector), index) {
        return Err(SqlMatchError::new(
            func,
            "NULL_INPUT",
            row,
            "nested list is NULL",
        ));
    }
    let entry = *(duckdb_vector_get_data(vector) as *const duckdb_list_entry).add(index as usize);
    let end = entry
        .offset
        .checked_add(entry.length)
        .ok_or_else(|| SqlMatchError::new(func, "INVALID_VECTOR", row, "LIST range overflow"))?;
    if end > duckdb_list_vector_get_size(vector) || usize::try_from(end).is_err() {
        return Err(SqlMatchError::new(
            func,
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

// ---------------------------------------------------------------------------
// Logical Type Builders
// ---------------------------------------------------------------------------

fn build_double_list_type() -> duckdb_logical_type {
    unsafe {
        let mut double_t = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE);
        let list_t = duckdb_create_list_type(double_t);
        duckdb_destroy_logical_type(&mut double_t);
        list_t
    }
}

fn build_dispersion_members_type() -> duckdb_logical_type {
    unsafe {
        let mut varchar_t = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR);
        let mut feats_t = build_double_list_type();

        let member_id_name = CString::new("member_id").unwrap();
        let features_name = CString::new("features").unwrap();
        let mut names = [member_id_name.as_ptr(), features_name.as_ptr()];
        let mut types = [varchar_t, feats_t];

        let mut struct_t = duckdb_create_struct_type(types.as_mut_ptr(), names.as_mut_ptr(), 2);
        duckdb_destroy_logical_type(&mut varchar_t);
        duckdb_destroy_logical_type(&mut feats_t);

        let list_t = duckdb_create_list_type(struct_t);
        duckdb_destroy_logical_type(&mut struct_t);
        list_t
    }
}

fn build_scenarios_input_type() -> duckdb_logical_type {
    unsafe {
        let mut bigint_t = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_BIGINT);
        let mut feats_t = build_double_list_type();

        let c_id_name = CString::new("cluster_id").unwrap();
        let m_feats_name = CString::new("mean_features").unwrap();
        let mut names = [c_id_name.as_ptr(), m_feats_name.as_ptr()];
        let mut types = [bigint_t, feats_t];

        let mut struct_t = duckdb_create_struct_type(types.as_mut_ptr(), names.as_mut_ptr(), 2);
        duckdb_destroy_logical_type(&mut bigint_t);
        duckdb_destroy_logical_type(&mut feats_t);

        let list_t = duckdb_create_list_type(struct_t);
        duckdb_destroy_logical_type(&mut struct_t);
        list_t
    }
}

fn build_match_config_type() -> duckdb_logical_type {
    unsafe {
        let mut d_t1 = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE);
        let mut d_t2 = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE);
        let mut d_t3 = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE);

        let n1 = CString::new("threshold_coefficient").unwrap();
        let n2 = CString::new("prev_dispersion").unwrap();
        let n3 = CString::new("curr_dispersion").unwrap();
        let mut names = [n1.as_ptr(), n2.as_ptr(), n3.as_ptr()];
        let mut types = [d_t1, d_t2, d_t3];

        let struct_t = duckdb_create_struct_type(types.as_mut_ptr(), names.as_mut_ptr(), 3);
        duckdb_destroy_logical_type(&mut d_t1);
        duckdb_destroy_logical_type(&mut d_t2);
        duckdb_destroy_logical_type(&mut d_t3);
        struct_t
    }
}

fn build_match_return_type() -> duckdb_logical_type {
    unsafe {
        let mut bigint_t = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_BIGINT);
        let mut double_t = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE);
        let mut bool_t = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN);
        let mut varchar_t = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR);

        let mut bigint_list = duckdb_create_list_type(bigint_t);
        let mut double_list = duckdb_create_list_type(double_t);
        let mut double_2d_list = duckdb_create_list_type(double_list);
        let mut bool_list = duckdb_create_list_type(bool_t);
        let mut bool_2d_list = duckdb_create_list_type(bool_list);

        // Links struct
        let link_names = [
            CString::new("prev_cluster_id").unwrap(),
            CString::new("curr_cluster_id").unwrap(),
            CString::new("distance").unwrap(),
            CString::new("distance_ratio").unwrap(),
            CString::new("alternative_status").unwrap(),
            CString::new("alternative_edge_count").unwrap(),
            CString::new("alternative_total_distance").unwrap(),
            CString::new("alternative_distance_increase").unwrap(),
            CString::new("alternative_distance_increase_ratio").unwrap(),
            CString::new("has_same_count_alternative").unwrap(),
            CString::new("ambiguous").unwrap(),
        ];
        let mut link_name_ptrs: Vec<*const c_char> =
            link_names.iter().map(|s| s.as_ptr()).collect();
        let mut link_types = [
            bigint_t, bigint_t, double_t, double_t, varchar_t, bigint_t, double_t, double_t,
            double_t, bool_t, bool_t,
        ];
        let mut link_struct = duckdb_create_struct_type(
            link_types.as_mut_ptr(),
            link_name_ptrs.as_mut_ptr(),
            link_types.len() as u64,
        );
        let mut links_list = duckdb_create_list_type(link_struct);
        duckdb_destroy_logical_type(&mut link_struct);

        // Unmatched struct
        let un_names = [
            CString::new("cluster_id").unwrap(),
            CString::new("reason").unwrap(),
            CString::new("min_candidate_distance").unwrap(),
            CString::new("matched_in_some_optimum").unwrap(),
        ];
        let mut un_name_ptrs: Vec<*const c_char> = un_names.iter().map(|s| s.as_ptr()).collect();
        let mut un_types = [bigint_t, varchar_t, double_t, bool_t];
        let mut un_struct = duckdb_create_struct_type(
            un_types.as_mut_ptr(),
            un_name_ptrs.as_mut_ptr(),
            un_types.len() as u64,
        );
        let mut un_prev_list = duckdb_create_list_type(un_struct);
        let mut un_curr_list = duckdb_create_list_type(un_struct);
        duckdb_destroy_logical_type(&mut un_struct);

        // Outer return struct
        let outer_names = [
            CString::new("algorithm_version").unwrap(),
            CString::new("prev_cluster_ids").unwrap(),
            CString::new("curr_cluster_ids").unwrap(),
            CString::new("prev_dispersion").unwrap(),
            CString::new("curr_dispersion").unwrap(),
            CString::new("pair_scale").unwrap(),
            CString::new("threshold_coefficient").unwrap(),
            CString::new("threshold").unwrap(),
            CString::new("distance_matrix").unwrap(),
            CString::new("distance_ratio_matrix").unwrap(),
            CString::new("eligible_matrix").unwrap(),
            CString::new("edge_count").unwrap(),
            CString::new("total_distance").unwrap(),
            CString::new("optimal_solution_count").unwrap(),
            CString::new("ambiguous").unwrap(),
            CString::new("threshold_margin").unwrap(),
            CString::new("optimality_margin").unwrap(),
            CString::new("has_optimality_margin").unwrap(),
            CString::new("links").unwrap(),
            CString::new("unmatched_prev").unwrap(),
            CString::new("unmatched_curr").unwrap(),
        ];
        let mut outer_name_ptrs: Vec<*const c_char> =
            outer_names.iter().map(|s| s.as_ptr()).collect();
        let mut outer_types = [
            varchar_t,
            bigint_list,
            bigint_list,
            double_t,
            double_t,
            double_t,
            double_t,
            double_t,
            double_2d_list,
            double_2d_list,
            bool_2d_list,
            bigint_t,
            double_t,
            bigint_t,
            bool_t,
            double_t,
            double_t,
            bool_t,
            links_list,
            un_prev_list,
            un_curr_list,
        ];

        let ret_struct = duckdb_create_struct_type(
            outer_types.as_mut_ptr(),
            outer_name_ptrs.as_mut_ptr(),
            outer_types.len() as u64,
        );

        duckdb_destroy_logical_type(&mut bigint_t);
        duckdb_destroy_logical_type(&mut double_t);
        duckdb_destroy_logical_type(&mut bool_t);
        duckdb_destroy_logical_type(&mut varchar_t);
        duckdb_destroy_logical_type(&mut bigint_list);
        duckdb_destroy_logical_type(&mut double_list);
        duckdb_destroy_logical_type(&mut double_2d_list);
        duckdb_destroy_logical_type(&mut bool_list);
        duckdb_destroy_logical_type(&mut bool_2d_list);
        duckdb_destroy_logical_type(&mut links_list);
        duckdb_destroy_logical_type(&mut un_prev_list);
        duckdb_destroy_logical_type(&mut un_curr_list);

        ret_struct
    }
}

// ---------------------------------------------------------------------------
// Extractors
// ---------------------------------------------------------------------------

unsafe fn extract_dispersion_input(
    members_vec: duckdb_vector,
    weights_vec: duckdb_vector,
    row: u64,
) -> Result<(Vec<MemberInput>, Vec<f64>), SqlMatchError> {
    let func = DISPERSION_FUNCTION_NAME;
    if !row_is_valid(duckdb_vector_get_validity(members_vec), row) {
        return Err(SqlMatchError::new(
            func,
            "NULL_INPUT",
            row,
            "members list is NULL",
        ));
    }
    if !row_is_valid(duckdb_vector_get_validity(weights_vec), row) {
        return Err(SqlMatchError::new(
            func,
            "NULL_INPUT",
            row,
            "weights list is NULL",
        ));
    }

    let m_entry = checked_list_entry(func, members_vec, row, row)?;
    let m_offset = m_entry.offset;
    let m_length = m_entry.length;
    if m_length == 0 {
        return Err(SqlMatchError::new(
            func,
            "EMPTY_MEMBERS",
            row,
            "members list is empty",
        ));
    }

    let w_entry = checked_list_entry(func, weights_vec, row, row)?;
    let w_offset = w_entry.offset;
    let w_length = w_entry.length;
    if w_length == 0 {
        return Err(SqlMatchError::new(
            func,
            "INVALID_WEIGHTS",
            row,
            "weights list is empty",
        ));
    }

    // Extract weights
    let w_child = duckdb_list_vector_get_child(weights_vec);
    let w_validity = duckdb_vector_get_validity(w_child);
    let w_data = duckdb_vector_get_data(w_child) as *const f64;
    let mut weights = Vec::with_capacity(w_length as usize);
    for idx in 0..w_length {
        let r = w_offset + idx;
        if !row_is_valid(w_validity, r) {
            return Err(SqlMatchError::new(
                func,
                "NULL_INPUT",
                row,
                format!("weight at index {idx} is NULL"),
            ));
        }
        let val = *w_data.add(r as usize);
        weights.push(val);
    }

    // Extract members
    let member_structs = duckdb_list_vector_get_child(members_vec);
    let ids_vec = duckdb_struct_vector_get_child(member_structs, 0);
    let feats_vec = duckdb_struct_vector_get_child(member_structs, 1);
    let m_validity = duckdb_vector_get_validity(member_structs);
    let id_validity = duckdb_vector_get_validity(ids_vec);
    let f_child = duckdb_list_vector_get_child(feats_vec);

    let mut members = Vec::with_capacity(m_length as usize);
    for m_idx in 0..m_length {
        let m_row = m_offset + m_idx;
        if !row_is_valid(m_validity, m_row) {
            return Err(SqlMatchError::new(
                func,
                "NULL_INPUT",
                row,
                format!("member at index {m_idx} is NULL"),
            ));
        }
        if !row_is_valid(id_validity, m_row) {
            return Err(SqlMatchError::new(
                func,
                "NULL_INPUT",
                row,
                format!("member_id at index {m_idx} is NULL"),
            ));
        }
        let member_id = read_string(ids_vec, m_row);
        let f_entry = checked_list_entry(func, feats_vec, m_row, row)?;
        let f_len = f_entry.length;
        let f_off = f_entry.offset;
        let f_validity = duckdb_vector_get_validity(f_child);
        let f_data = duckdb_vector_get_data(f_child) as *const f64;

        let mut features = Vec::with_capacity(f_len as usize);
        for f_i in 0..f_len {
            let fr = f_off + f_i;
            if !row_is_valid(f_validity, fr) {
                return Err(SqlMatchError::new(
                    func,
                    "NULL_INPUT",
                    row,
                    format!("feature at member {m_idx}, index {f_i} is NULL"),
                ));
            }
            let fv = *f_data.add(fr as usize);
            features.push(fv);
        }

        members.push(MemberInput {
            member_index: m_idx as usize,
            member_id,
            features,
        });
    }

    Ok((members, weights))
}

unsafe fn extract_scenarios(
    func: &'static str,
    scenarios_vec: duckdb_vector,
    row: u64,
    side_name: &'static str,
) -> Result<Vec<ScenarioNodeInput>, SqlMatchError> {
    if !row_is_valid(duckdb_vector_get_validity(scenarios_vec), row) {
        return Err(SqlMatchError::new(
            func,
            "NULL_INPUT",
            row,
            format!("{side_name} list is NULL"),
        ));
    }
    let entry = checked_list_entry(func, scenarios_vec, row, row)?;
    let length = entry.length;
    let offset = entry.offset;
    if length == 0 {
        return Err(SqlMatchError::new(
            func,
            "EMPTY_SCENARIOS",
            row,
            format!("{side_name} list is empty"),
        ));
    }

    let struct_vec = duckdb_list_vector_get_child(scenarios_vec);
    let cid_vec = duckdb_struct_vector_get_child(struct_vec, 0);
    let feats_vec = duckdb_struct_vector_get_child(struct_vec, 1);
    let s_validity = duckdb_vector_get_validity(struct_vec);
    let cid_validity = duckdb_vector_get_validity(cid_vec);
    let cid_data = duckdb_vector_get_data(cid_vec) as *const i64;
    let f_child = duckdb_list_vector_get_child(feats_vec);

    let mut scenarios = Vec::with_capacity(length as usize);
    for idx in 0..length {
        let r = offset + idx;
        if !row_is_valid(s_validity, r) {
            return Err(SqlMatchError::new(
                func,
                "NULL_INPUT",
                row,
                format!("{side_name} struct at index {idx} is NULL"),
            ));
        }
        if !row_is_valid(cid_validity, r) {
            return Err(SqlMatchError::new(
                func,
                "NULL_INPUT",
                row,
                format!("{side_name} cluster_id at index {idx} is NULL"),
            ));
        }
        let cid_raw = *cid_data.add(r as usize);
        if cid_raw < 0 {
            return Err(SqlMatchError::new(
                func,
                "INVALID_SCENARIO_ID",
                row,
                format!("cluster_id must be non-negative, got {cid_raw}"),
            ));
        }
        let cluster_id = cid_raw as usize;

        let f_entry = checked_list_entry(func, feats_vec, r, row)?;
        let f_len = f_entry.length;
        let f_off = f_entry.offset;
        let f_validity = duckdb_vector_get_validity(f_child);
        let f_data = duckdb_vector_get_data(f_child) as *const f64;

        let mut mean_features = Vec::with_capacity(f_len as usize);
        for f_i in 0..f_len {
            let fr = f_off + f_i;
            if !row_is_valid(f_validity, fr) {
                return Err(SqlMatchError::new(
                    func,
                    "NULL_INPUT",
                    row,
                    format!("{side_name} feature at scenario {idx}, index {f_i} is NULL"),
                ));
            }
            let fv = *f_data.add(fr as usize);
            mean_features.push(fv);
        }

        scenarios.push(ScenarioNodeInput {
            cluster_id,
            mean_features,
            member_count: 1,
            member_fraction: 1.0,
        });
    }

    Ok(scenarios)
}

unsafe fn extract_match_input(
    prev_vec: duckdb_vector,
    curr_vec: duckdb_vector,
    weights_vec: duckdb_vector,
    config_vec: duckdb_vector,
    row: u64,
) -> Result<ScaledMatchInput, SqlMatchError> {
    let func = MATCH_SCENARIOS_FUNCTION_NAME;
    let prev_scenarios = extract_scenarios(func, prev_vec, row, "prev_scenarios")?;
    let curr_scenarios = extract_scenarios(func, curr_vec, row, "curr_scenarios")?;

    if !row_is_valid(duckdb_vector_get_validity(weights_vec), row) {
        return Err(SqlMatchError::new(
            func,
            "NULL_INPUT",
            row,
            "weights list is NULL",
        ));
    }
    let w_entry = checked_list_entry(func, weights_vec, row, row)?;
    let w_len = w_entry.length;
    let w_off = w_entry.offset;
    if w_len == 0 {
        return Err(SqlMatchError::new(
            func,
            "EMPTY_FEATURES",
            row,
            "weights list is empty",
        ));
    }

    let w_child = duckdb_list_vector_get_child(weights_vec);
    let w_validity = duckdb_vector_get_validity(w_child);
    let w_data = duckdb_vector_get_data(w_child) as *const f64;
    let mut weights = Vec::with_capacity(w_len as usize);
    for idx in 0..w_len {
        let r = w_off + idx;
        if !row_is_valid(w_validity, r) {
            return Err(SqlMatchError::new(
                func,
                "NULL_INPUT",
                row,
                format!("weight at index {idx} is NULL"),
            ));
        }
        weights.push(*w_data.add(r as usize));
    }

    // Config struct
    if !row_is_valid(duckdb_vector_get_validity(config_vec), row) {
        return Err(SqlMatchError::new(
            func,
            "NULL_INPUT",
            row,
            "config struct is NULL",
        ));
    }
    let c_coeff_v = duckdb_struct_vector_get_child(config_vec, 0);
    let c_prev_disp_v = duckdb_struct_vector_get_child(config_vec, 1);
    let c_curr_disp_v = duckdb_struct_vector_get_child(config_vec, 2);

    if !row_is_valid(duckdb_vector_get_validity(c_coeff_v), row)
        || !row_is_valid(duckdb_vector_get_validity(c_prev_disp_v), row)
        || !row_is_valid(duckdb_vector_get_validity(c_curr_disp_v), row)
    {
        return Err(SqlMatchError::new(
            func,
            "NULL_INPUT",
            row,
            "config fields must not be NULL",
        ));
    }

    let c_val = *(duckdb_vector_get_data(c_coeff_v) as *const f64).add(row as usize);
    let sp_val = *(duckdb_vector_get_data(c_prev_disp_v) as *const f64).add(row as usize);
    let sc_val = *(duckdb_vector_get_data(c_curr_disp_v) as *const f64).add(row as usize);

    Ok(ScaledMatchInput {
        prev_scenarios,
        curr_scenarios,
        weights,
        threshold_coefficient: c_val,
        prev_dispersion: sp_val,
        curr_dispersion: sc_val,
    })
}

// ---------------------------------------------------------------------------
// Callbacks
// ---------------------------------------------------------------------------

unsafe extern "C" fn dispersion_callback(
    info: duckdb_function_info,
    chunk: duckdb_data_chunk,
    output: duckdb_vector,
) {
    let result = catch_unwind(AssertUnwindSafe(|| -> Result<(), SqlMatchError> {
        let count = duckdb_data_chunk_get_size(chunk);
        if count == 0 {
            return Ok(());
        }
        let members_vec = duckdb_data_chunk_get_vector(chunk, 0);
        let weights_vec = duckdb_data_chunk_get_vector(chunk, 1);
        if members_vec.is_null() || weights_vec.is_null() {
            return Err(SqlMatchError::new(
                DISPERSION_FUNCTION_NAME,
                "NULL_INPUT",
                0,
                "input argument vector is NULL",
            ));
        }

        duckdb_vector_ensure_validity_writable(output);
        let out_data = duckdb_vector_get_data(output) as *mut f64;

        // Compute all rows first
        let mut row_results = Vec::with_capacity(count as usize);
        for row in 0..count {
            let (members, weights) = extract_dispersion_input(members_vec, weights_vec, row)?;
            let s = ensemble_dispersion(&members, &weights)
                .map_err(|e| SqlMatchError::from_core(DISPERSION_FUNCTION_NAME, e, row))?;
            row_results.push(s);
        }

        // Write to output vector
        for (row, s) in row_results.into_iter().enumerate() {
            *out_data.add(row) = s;
        }

        Ok(())
    }));

    match result {
        Ok(Ok(())) => {}
        Ok(Err(sql_err)) => {
            let c_str = sql_err.to_c_string();
            set_error_raw(info, &c_str);
        }
        Err(_) => {
            let c_str =
                CString::new("ens_dispersion [PANIC] unexpected panic in callback").unwrap();
            set_error_raw(info, &c_str);
        }
    }
}

unsafe extern "C" fn match_scenarios_callback(
    info: duckdb_function_info,
    chunk: duckdb_data_chunk,
    output: duckdb_vector,
) {
    let result = catch_unwind(AssertUnwindSafe(|| -> Result<(), SqlMatchError> {
        let count = duckdb_data_chunk_get_size(chunk);
        if count == 0 {
            return Ok(());
        }
        let prev_vec = duckdb_data_chunk_get_vector(chunk, 0);
        let curr_vec = duckdb_data_chunk_get_vector(chunk, 1);
        let weights_vec = duckdb_data_chunk_get_vector(chunk, 2);
        let config_vec = duckdb_data_chunk_get_vector(chunk, 3);
        if prev_vec.is_null()
            || curr_vec.is_null()
            || weights_vec.is_null()
            || config_vec.is_null()
        {
            return Err(SqlMatchError::new(
                MATCH_SCENARIOS_FUNCTION_NAME,
                "NULL_INPUT",
                0,
                "input argument vector is NULL",
            ));
        }

        let mut chunk_results = Vec::with_capacity(count as usize);
        let mut prev_inputs = Vec::with_capacity(count as usize);

        for row in 0..count {
            let input = extract_match_input(prev_vec, curr_vec, weights_vec, config_vec, row)?;
            let res = match_scenarios_scaled(&input)
                .map_err(|e| SqlMatchError::from_core(MATCH_SCENARIOS_FUNCTION_NAME, e, row))?;
            prev_inputs.push(input);
            chunk_results.push(res);
        }

        write_match_results_to_output(&prev_inputs, &chunk_results, output).map_err(|e| {
            SqlMatchError::new(MATCH_SCENARIOS_FUNCTION_NAME, "WRITE_FAILURE", 0, e)
        })?;

        Ok(())
    }));

    match result {
        Ok(Ok(())) => {}
        Ok(Err(sql_err)) => {
            let c_str = sql_err.to_c_string();
            set_error_raw(info, &c_str);
        }
        Err(_) => {
            let c_str =
                CString::new("ens_match_scenarios [PANIC] unexpected panic in callback").unwrap();
            set_error_raw(info, &c_str);
        }
    }
}

// ---------------------------------------------------------------------------
// Writing Match Results Output
// ---------------------------------------------------------------------------

unsafe fn write_match_results_to_output(
    inputs: &[ScaledMatchInput],
    results: &[MatchResult],
    output: duckdb_vector,
) -> Result<(), &'static str> {
    let count = results.len();
    duckdb_vector_ensure_validity_writable(output);

    // Get struct child vectors
    let v_algo = duckdb_struct_vector_get_child(output, 0);
    let v_prev_ids = duckdb_struct_vector_get_child(output, 1);
    let v_curr_ids = duckdb_struct_vector_get_child(output, 2);
    let v_prev_disp = duckdb_struct_vector_get_child(output, 3);
    let v_curr_disp = duckdb_struct_vector_get_child(output, 4);
    let v_pair_scale = duckdb_struct_vector_get_child(output, 5);
    let v_thresh_coeff = duckdb_struct_vector_get_child(output, 6);
    let v_threshold = duckdb_struct_vector_get_child(output, 7);
    let v_dist_mat = duckdb_struct_vector_get_child(output, 8);
    let v_dist_ratio_mat = duckdb_struct_vector_get_child(output, 9);
    let v_elig_mat = duckdb_struct_vector_get_child(output, 10);
    let v_edge_count = duckdb_struct_vector_get_child(output, 11);
    let v_total_dist = duckdb_struct_vector_get_child(output, 12);
    let v_opt_count = duckdb_struct_vector_get_child(output, 13);
    let v_ambiguous = duckdb_struct_vector_get_child(output, 14);
    let v_thresh_margin = duckdb_struct_vector_get_child(output, 15);
    let v_opt_margin = duckdb_struct_vector_get_child(output, 16);
    let v_has_opt_margin = duckdb_struct_vector_get_child(output, 17);
    let v_links = duckdb_struct_vector_get_child(output, 18);
    let v_un_prev = duckdb_struct_vector_get_child(output, 19);
    let v_un_curr = duckdb_struct_vector_get_child(output, 20);

    // Precalculate list sizes
    let mut total_prev_ids = 0usize;
    let mut total_curr_ids = 0usize;
    let mut total_dist_mat_rows = 0usize;
    let mut total_dist_mat_cells = 0usize;
    let mut total_links = 0usize;
    let mut total_un_prev = 0usize;
    let mut total_un_curr = 0usize;

    for (inp, res) in inputs.iter().zip(results.iter()) {
        let m = inp.prev_scenarios.len();
        let n = inp.curr_scenarios.len();
        total_prev_ids = total_prev_ids.checked_add(m).ok_or("LIST size overflow")?;
        total_curr_ids = total_curr_ids.checked_add(n).ok_or("LIST size overflow")?;
        total_dist_mat_rows = total_dist_mat_rows
            .checked_add(m)
            .ok_or("LIST size overflow")?;
        total_dist_mat_cells = total_dist_mat_cells
            .checked_add(m.checked_mul(n).ok_or("LIST size overflow")?)
            .ok_or("LIST size overflow")?;
        total_links = total_links
            .checked_add(res.links.len())
            .ok_or("LIST size overflow")?;
        total_un_prev = total_un_prev
            .checked_add(res.unmatched_prev.len())
            .ok_or("LIST size overflow")?;
        total_un_curr = total_un_curr
            .checked_add(res.unmatched_curr.len())
            .ok_or("LIST size overflow")?;
    }

    reserve_list(v_prev_ids, total_prev_ids)?;
    reserve_list(v_curr_ids, total_curr_ids)?;

    reserve_list(v_dist_mat, total_dist_mat_rows)?;
    let d_mat_inner = duckdb_list_vector_get_child(v_dist_mat);
    reserve_list(d_mat_inner, total_dist_mat_cells)?;

    reserve_list(v_dist_ratio_mat, total_dist_mat_rows)?;
    let dr_mat_inner = duckdb_list_vector_get_child(v_dist_ratio_mat);
    reserve_list(dr_mat_inner, total_dist_mat_cells)?;

    reserve_list(v_elig_mat, total_dist_mat_rows)?;
    let el_mat_inner = duckdb_list_vector_get_child(v_elig_mat);
    reserve_list(el_mat_inner, total_dist_mat_cells)?;

    reserve_list(v_links, total_links)?;
    reserve_list(v_un_prev, total_un_prev)?;
    reserve_list(v_un_curr, total_un_curr)?;

    // Offsets for writing into lists
    let mut off_prev_ids = 0u64;
    let mut off_curr_ids = 0u64;
    let mut off_d_row = 0u64;
    let mut off_d_cell = 0u64;
    let mut off_dr_row = 0u64;
    let mut off_dr_cell = 0u64;
    let mut off_el_row = 0u64;
    let mut off_el_cell = 0u64;
    let mut off_links = 0u64;
    let mut off_un_prev = 0u64;
    let mut off_un_curr = 0u64;

    for row in 0..count {
        let inp = &inputs[row];
        let res = &results[row];

        let m = inp.prev_scenarios.len();
        let n = inp.curr_scenarios.len();
        let scale = res.pair_scale.unwrap_or(1.0);
        let coeff = res.threshold_coefficient.unwrap_or(1.0);
        let tau = coeff * scale;

        // 0: algorithm_version
        duckdb_vector_assign_string_element_len(
            v_algo,
            row as u64,
            MATCH_ALGORITHM_VERSION.as_ptr().cast(),
            MATCH_ALGORITHM_VERSION.len() as u64,
        );

        // 1: prev_cluster_ids
        let prev_ids_child = duckdb_list_vector_get_child(v_prev_ids);
        let prev_ids_data = duckdb_vector_get_data(prev_ids_child) as *mut i64;
        let prev_ids_entry = duckdb_list_entry {
            offset: off_prev_ids,
            length: m as u64,
        };
        for (i, sc) in inp.prev_scenarios.iter().enumerate() {
            *prev_ids_data.add((off_prev_ids as usize) + i) = sc.cluster_id as i64;
        }
        off_prev_ids += m as u64;
        *(duckdb_vector_get_data(v_prev_ids) as *mut duckdb_list_entry).add(row) = prev_ids_entry;

        // 2: curr_cluster_ids
        let curr_ids_child = duckdb_list_vector_get_child(v_curr_ids);
        let curr_ids_data = duckdb_vector_get_data(curr_ids_child) as *mut i64;
        let curr_ids_entry = duckdb_list_entry {
            offset: off_curr_ids,
            length: n as u64,
        };
        for (j, sc) in inp.curr_scenarios.iter().enumerate() {
            *curr_ids_data.add((off_curr_ids as usize) + j) = sc.cluster_id as i64;
        }
        off_curr_ids += n as u64;
        *(duckdb_vector_get_data(v_curr_ids) as *mut duckdb_list_entry).add(row) = curr_ids_entry;

        // 3..7: dispersions, scale, coeff, threshold
        *(duckdb_vector_get_data(v_prev_disp) as *mut f64).add(row) =
            res.prev_dispersion.unwrap_or(0.0);
        *(duckdb_vector_get_data(v_curr_disp) as *mut f64).add(row) =
            res.curr_dispersion.unwrap_or(0.0);
        *(duckdb_vector_get_data(v_pair_scale) as *mut f64).add(row) = scale;
        *(duckdb_vector_get_data(v_thresh_coeff) as *mut f64).add(row) = coeff;
        *(duckdb_vector_get_data(v_threshold) as *mut f64).add(row) = tau;

        // 8..10: 2D matrices
        let d_outer_data = duckdb_vector_get_data(v_dist_mat) as *mut duckdb_list_entry;
        let d_inner_data = duckdb_vector_get_data(d_mat_inner) as *mut duckdb_list_entry;
        let d_cell_data =
            duckdb_vector_get_data(duckdb_list_vector_get_child(d_mat_inner)) as *mut f64;

        let dr_outer_data = duckdb_vector_get_data(v_dist_ratio_mat) as *mut duckdb_list_entry;
        let dr_inner_data = duckdb_vector_get_data(dr_mat_inner) as *mut duckdb_list_entry;
        let dr_cell_data =
            duckdb_vector_get_data(duckdb_list_vector_get_child(dr_mat_inner)) as *mut f64;

        let el_outer_data = duckdb_vector_get_data(v_elig_mat) as *mut duckdb_list_entry;
        let el_inner_data = duckdb_vector_get_data(el_mat_inner) as *mut duckdb_list_entry;
        let el_cell_data =
            duckdb_vector_get_data(duckdb_list_vector_get_child(el_mat_inner)) as *mut bool;

        *d_outer_data.add(row) = duckdb_list_entry {
            offset: off_d_row,
            length: m as u64,
        };
        *dr_outer_data.add(row) = duckdb_list_entry {
            offset: off_dr_row,
            length: m as u64,
        };
        *el_outer_data.add(row) = duckdb_list_entry {
            offset: off_el_row,
            length: m as u64,
        };

        let mut min_thresh_margin = f64::INFINITY;
        for i in 0..m {
            let row_d_off = off_d_cell;
            let row_dr_off = off_dr_cell;
            let row_el_off = off_el_cell;

            for j in 0..n {
                let dist = res.distance_matrix[i][j];
                let ratio = dist / scale;
                let elig = dist <= tau;
                let margin = (dist - tau).abs();
                if margin < min_thresh_margin {
                    min_thresh_margin = margin;
                }

                *d_cell_data.add(row_d_off as usize + j) = dist;
                *dr_cell_data.add(row_dr_off as usize + j) = ratio;
                *el_cell_data.add(row_el_off as usize + j) = elig;
            }

            *d_inner_data.add((off_d_row as usize) + i) = duckdb_list_entry {
                offset: row_d_off,
                length: n as u64,
            };
            *dr_inner_data.add((off_dr_row as usize) + i) = duckdb_list_entry {
                offset: row_dr_off,
                length: n as u64,
            };
            *el_inner_data.add((off_el_row as usize) + i) = duckdb_list_entry {
                offset: row_el_off,
                length: n as u64,
            };

            off_d_cell += n as u64;
            off_dr_cell += n as u64;
            off_el_cell += n as u64;
        }
        off_d_row += m as u64;
        off_dr_row += m as u64;
        off_el_row += m as u64;

        if !min_thresh_margin.is_finite() {
            min_thresh_margin = 0.0;
        }

        // 11..14: edge_count, total_distance, optimal_solution_count, ambiguous
        *(duckdb_vector_get_data(v_edge_count) as *mut i64).add(row) =
            res.objective.matched_count as i64;
        *(duckdb_vector_get_data(v_total_dist) as *mut f64).add(row) =
            res.objective.weighted_distance_sum;
        *(duckdb_vector_get_data(v_opt_count) as *mut i64).add(row) = res.optima_count as i64;
        let is_ambiguous = res.links.iter().any(|l| l.ambiguous) || res.optima_count > 1;
        *(duckdb_vector_get_data(v_ambiguous) as *mut bool).add(row) = is_ambiguous;

        // 15..17: threshold_margin, optimality_margin, has_optimality_margin
        *(duckdb_vector_get_data(v_thresh_margin) as *mut f64).add(row) = min_thresh_margin;

        let mut min_opt_margin = f64::INFINITY;
        let mut has_opt_margin = false;
        for link in &res.links {
            match &link.substitutability {
                Substitutability::Tied => {
                    has_opt_margin = true;
                    min_opt_margin = 0.0;
                }
                Substitutability::SameCount { gap, .. } => {
                    has_opt_margin = true;
                    if *gap < min_opt_margin {
                        min_opt_margin = *gap;
                    }
                }
                _ => {}
            }
        }
        if !has_opt_margin || !min_opt_margin.is_finite() {
            min_opt_margin = 0.0;
        }
        *(duckdb_vector_get_data(v_opt_margin) as *mut f64).add(row) = min_opt_margin;
        *(duckdb_vector_get_data(v_has_opt_margin) as *mut bool).add(row) = has_opt_margin;

        // 18: links list
        let links_child = duckdb_list_vector_get_child(v_links);
        let l_prev_id_v = duckdb_struct_vector_get_child(links_child, 0);
        let l_curr_id_v = duckdb_struct_vector_get_child(links_child, 1);
        let l_dist_v = duckdb_struct_vector_get_child(links_child, 2);
        let l_dist_ratio_v = duckdb_struct_vector_get_child(links_child, 3);
        let l_status_v = duckdb_struct_vector_get_child(links_child, 4);
        let l_alt_edge_v = duckdb_struct_vector_get_child(links_child, 5);
        let l_alt_tot_v = duckdb_struct_vector_get_child(links_child, 6);
        let l_alt_inc_v = duckdb_struct_vector_get_child(links_child, 7);
        let l_alt_inc_r_v = duckdb_struct_vector_get_child(links_child, 8);
        let l_has_alt_v = duckdb_struct_vector_get_child(links_child, 9);
        let l_amb_v = duckdb_struct_vector_get_child(links_child, 10);

        let links_entry = duckdb_list_entry {
            offset: off_links,
            length: res.links.len() as u64,
        };
        for (l_idx, link) in res.links.iter().enumerate() {
            let l_r = (off_links as usize) + l_idx;
            *(duckdb_vector_get_data(l_prev_id_v) as *mut i64).add(l_r) =
                link.prev_cluster_id as i64;
            *(duckdb_vector_get_data(l_curr_id_v) as *mut i64).add(l_r) =
                link.curr_cluster_id as i64;
            *(duckdb_vector_get_data(l_dist_v) as *mut f64).add(l_r) = link.distance_k;
            let ratio = link
                .distance_ratio
                .unwrap_or_else(|| link.distance_k / scale);
            *(duckdb_vector_get_data(l_dist_ratio_v) as *mut f64).add(l_r) = ratio;
            *(duckdb_vector_get_data(l_amb_v) as *mut bool).add(l_r) = link.ambiguous;

            let (status_str, alt_cnt, alt_tot, alt_inc, alt_inc_r, has_alt) =
                match &link.substitutability {
                    Substitutability::Tied => (
                        "tied",
                        res.objective.matched_count as i64,
                        res.objective.weighted_distance_sum,
                        0.0,
                        0.0,
                        true,
                    ),
                    Substitutability::SameCount { gap, alt_objective } => {
                        let r = *gap / scale;
                        (
                            "same_count",
                            res.objective.matched_count as i64,
                            *alt_objective,
                            *gap,
                            r,
                            true,
                        )
                    }
                    Substitutability::CountDrop {
                        alt_count,
                        alt_objective,
                    } => (
                        "count_drop",
                        *alt_count as i64,
                        *alt_objective,
                        0.0,
                        0.0,
                        false,
                    ),
                };

            duckdb_vector_assign_string_element_len(
                l_status_v,
                l_r as u64,
                status_str.as_ptr().cast(),
                status_str.len() as u64,
            );
            *(duckdb_vector_get_data(l_alt_edge_v) as *mut i64).add(l_r) = alt_cnt;
            *(duckdb_vector_get_data(l_alt_tot_v) as *mut f64).add(l_r) = alt_tot;
            *(duckdb_vector_get_data(l_alt_inc_v) as *mut f64).add(l_r) = alt_inc;
            *(duckdb_vector_get_data(l_alt_inc_r_v) as *mut f64).add(l_r) = alt_inc_r;
            *(duckdb_vector_get_data(l_has_alt_v) as *mut bool).add(l_r) = has_alt;
        }
        off_links += res.links.len() as u64;
        *(duckdb_vector_get_data(v_links) as *mut duckdb_list_entry).add(row) = links_entry;

        // 19: unmatched_prev
        let un_prev_child = duckdb_list_vector_get_child(v_un_prev);
        let un_p_cid_v = duckdb_struct_vector_get_child(un_prev_child, 0);
        let un_p_reason_v = duckdb_struct_vector_get_child(un_prev_child, 1);
        let un_p_min_d_v = duckdb_struct_vector_get_child(un_prev_child, 2);
        let un_p_opt_v = duckdb_struct_vector_get_child(un_prev_child, 3);

        let un_prev_entry = duckdb_list_entry {
            offset: off_un_prev,
            length: res.unmatched_prev.len() as u64,
        };
        for (u_idx, un) in res.unmatched_prev.iter().enumerate() {
            let u_r = (off_un_prev as usize) + u_idx;
            *(duckdb_vector_get_data(un_p_cid_v) as *mut i64).add(u_r) = un.cluster_id as i64;
            duckdb_vector_assign_string_element_len(
                un_p_reason_v,
                u_r as u64,
                un.reason.as_ptr().cast(),
                un.reason.len() as u64,
            );
            // Min candidate distance across all curr scenarios
            let p_idx = inp
                .prev_scenarios
                .iter()
                .position(|s| s.cluster_id == un.cluster_id)
                .unwrap_or(0);
            let min_d = (0..n)
                .map(|j| res.distance_matrix[p_idx][j])
                .fold(f64::INFINITY, f64::min);
            *(duckdb_vector_get_data(un_p_min_d_v) as *mut f64).add(u_r) =
                if min_d.is_finite() { min_d } else { 0.0 };
            *(duckdb_vector_get_data(un_p_opt_v) as *mut bool).add(u_r) =
                un.matched_in_some_optimum;
        }
        off_un_prev += res.unmatched_prev.len() as u64;
        *(duckdb_vector_get_data(v_un_prev) as *mut duckdb_list_entry).add(row) = un_prev_entry;

        // 20: unmatched_curr
        let un_curr_child = duckdb_list_vector_get_child(v_un_curr);
        let un_c_cid_v = duckdb_struct_vector_get_child(un_curr_child, 0);
        let un_c_reason_v = duckdb_struct_vector_get_child(un_curr_child, 1);
        let un_c_min_d_v = duckdb_struct_vector_get_child(un_curr_child, 2);
        let un_c_opt_v = duckdb_struct_vector_get_child(un_curr_child, 3);

        let un_curr_entry = duckdb_list_entry {
            offset: off_un_curr,
            length: res.unmatched_curr.len() as u64,
        };
        for (u_idx, un) in res.unmatched_curr.iter().enumerate() {
            let u_r = (off_un_curr as usize) + u_idx;
            *(duckdb_vector_get_data(un_c_cid_v) as *mut i64).add(u_r) = un.cluster_id as i64;
            duckdb_vector_assign_string_element_len(
                un_c_reason_v,
                u_r as u64,
                un.reason.as_ptr().cast(),
                un.reason.len() as u64,
            );
            // Min candidate distance across all prev scenarios
            let c_idx = inp
                .curr_scenarios
                .iter()
                .position(|s| s.cluster_id == un.cluster_id)
                .unwrap_or(0);
            let min_d = (0..m)
                .map(|i| res.distance_matrix[i][c_idx])
                .fold(f64::INFINITY, f64::min);
            *(duckdb_vector_get_data(un_c_min_d_v) as *mut f64).add(u_r) =
                if min_d.is_finite() { min_d } else { 0.0 };
            *(duckdb_vector_get_data(un_c_opt_v) as *mut bool).add(u_r) =
                un.matched_in_some_optimum;
        }
        off_un_curr += res.unmatched_curr.len() as u64;
        *(duckdb_vector_get_data(v_un_curr) as *mut duckdb_list_entry).add(row) = un_curr_entry;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

pub unsafe fn register_match_functions(connection: duckdb_connection) -> Result<(), String> {
    unsafe {
        // 1. ens_dispersion
        let disp_name = CString::new(DISPERSION_FUNCTION_NAME).unwrap();
        let mut disp_fn = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(disp_fn, disp_name.as_ptr());

        let mut m_type = build_dispersion_members_type();
        let mut w_type = build_double_list_type();
        let mut ret_double = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE);

        duckdb_scalar_function_add_parameter(disp_fn, m_type);
        duckdb_scalar_function_add_parameter(disp_fn, w_type);
        duckdb_scalar_function_set_return_type(disp_fn, ret_double);

        duckdb_destroy_logical_type(&mut m_type);
        duckdb_destroy_logical_type(&mut w_type);
        duckdb_destroy_logical_type(&mut ret_double);

        duckdb_scalar_function_set_special_handling(disp_fn);
        duckdb_scalar_function_set_function(disp_fn, Some(dispersion_callback));

        let state1 = duckdb_register_scalar_function(connection, disp_fn);
        duckdb_destroy_scalar_function(&mut disp_fn);
        if state1 != DuckDBSuccess {
            return Err(format!("failed to register {DISPERSION_FUNCTION_NAME}"));
        }

        // 2. ens_match_scenarios
        let match_name = CString::new(MATCH_SCENARIOS_FUNCTION_NAME).unwrap();
        let mut match_fn = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(match_fn, match_name.as_ptr());

        let mut sc_prev_type = build_scenarios_input_type();
        let mut sc_curr_type = build_scenarios_input_type();
        let mut weights_type = build_double_list_type();
        let mut cfg_type = build_match_config_type();
        let mut ret_match_type = build_match_return_type();

        duckdb_scalar_function_add_parameter(match_fn, sc_prev_type);
        duckdb_scalar_function_add_parameter(match_fn, sc_curr_type);
        duckdb_scalar_function_add_parameter(match_fn, weights_type);
        duckdb_scalar_function_add_parameter(match_fn, cfg_type);
        duckdb_scalar_function_set_return_type(match_fn, ret_match_type);

        duckdb_destroy_logical_type(&mut sc_prev_type);
        duckdb_destroy_logical_type(&mut sc_curr_type);
        duckdb_destroy_logical_type(&mut weights_type);
        duckdb_destroy_logical_type(&mut cfg_type);
        duckdb_destroy_logical_type(&mut ret_match_type);

        duckdb_scalar_function_set_special_handling(match_fn);
        duckdb_scalar_function_set_function(match_fn, Some(match_scenarios_callback));

        let state2 = duckdb_register_scalar_function(connection, match_fn);
        duckdb_destroy_scalar_function(&mut match_fn);
        if state2 != DuckDBSuccess {
            return Err(format!(
                "failed to register {MATCH_SCENARIOS_FUNCTION_NAME}"
            ));
        }

        Ok(())
    }
}
