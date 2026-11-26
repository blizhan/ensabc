//! M3 DuckDB extension: exposes `ens_fsum`, `ens_evolve_scenarios`, and `ens_diagnose_batch`.

#![allow(unsafe_op_in_unsafe_fn)]

use std::{
    ffi::{CString, c_char},
    panic::{AssertUnwindSafe, catch_unwind},
};

use duckdb::ffi::{
    DUCKDB_TYPE_DUCKDB_TYPE_BIGINT, DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN,
    DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE, DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR, DuckDBSuccess,
    duckdb_connection, duckdb_create_list_type, duckdb_create_logical_type,
    duckdb_create_scalar_function, duckdb_create_struct_type, duckdb_data_chunk,
    duckdb_data_chunk_get_size, duckdb_data_chunk_get_vector, duckdb_destroy_logical_type,
    duckdb_destroy_scalar_function, duckdb_function_info, duckdb_list_entry,
    duckdb_list_vector_get_child, duckdb_logical_type, duckdb_register_scalar_function,
    duckdb_scalar_function_add_parameter, duckdb_scalar_function_set_function,
    duckdb_scalar_function_set_name, duckdb_scalar_function_set_return_type,
    duckdb_scalar_function_set_special_handling, duckdb_struct_vector_get_child, duckdb_vector,
    duckdb_vector_assign_string_element_len, duckdb_vector_ensure_validity_writable,
    duckdb_vector_get_data, duckdb_vector_get_validity,
};

use ensabc_core::{
    BackboneAssertion, CoreErrorCode, EvolutionConfig, EvolutionResult, FlowRule, InitMethod,
    MATCH_ALGORITHM_VERSION, NodeEvolutionSummary, ScaledEvolutionInput, ScenarioNodeInput,
    SecondaryRule, Substitutability, TaskInput, cluster, ensemble_dispersion,
    evolve_scenarios_scaled, fsum::Expansion,
};

use crate::match_ext::{
    SqlMatchError, build_double_list_type, build_match_return_type, checked_list_entry,
    read_string, reserve_list, row_is_valid, set_error_raw,
};

const FSUM_FUNCTION_NAME: &str = "ens_fsum";
const EVOLVE_SCENARIOS_FUNCTION_NAME: &str = "ens_evolve_scenarios";
const DIAGNOSE_BATCH_FUNCTION_NAME: &str = "ens_diagnose_batch";

#[inline]
fn norm_f64(x: f64) -> f64 {
    if x == 0.0 { 0.0 } else { x }
}

#[inline]
fn f64_eq_exact(got: f64, exp: f64) -> bool {
    got.is_finite() && exp.is_finite() && norm_f64(got).to_bits() == norm_f64(exp).to_bits()
}

// ---------------------------------------------------------------------------
// Logical Type Builders
// ---------------------------------------------------------------------------

fn build_evolve_scenarios_input_type() -> duckdb_logical_type {
    unsafe {
        let mut bigint_t = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_BIGINT);
        let mut double_t = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE);
        let mut feats_t = build_double_list_type();

        let names_c = [
            CString::new("cluster_id").unwrap(),
            CString::new("member_count").unwrap(),
            CString::new("member_fraction").unwrap(),
            CString::new("mean_features").unwrap(),
        ];
        let mut name_ptrs: Vec<*const c_char> = names_c.iter().map(|s| s.as_ptr()).collect();
        let mut types = [bigint_t, bigint_t, double_t, feats_t];

        let mut struct_t = duckdb_create_struct_type(
            types.as_mut_ptr(),
            name_ptrs.as_mut_ptr(),
            types.len() as u64,
        );

        duckdb_destroy_logical_type(&mut bigint_t);
        duckdb_destroy_logical_type(&mut double_t);
        duckdb_destroy_logical_type(&mut feats_t);

        let list_t = duckdb_create_list_type(struct_t);
        duckdb_destroy_logical_type(&mut struct_t);
        list_t
    }
}

fn build_evolve_config_type() -> duckdb_logical_type {
    unsafe {
        let mut varchar_t = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR);
        let mut double_t = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE);

        let names_c = [
            CString::new("secondary_rule").unwrap(),
            CString::new("flow_rule").unwrap(),
            CString::new("fraction_sum_tolerance").unwrap(),
            CString::new("high_uncertainty_threshold_ratio").unwrap(),
            CString::new("high_uncertainty_optimality_ratio").unwrap(),
            CString::new("high_uncertainty_secondary_ratio").unwrap(),
        ];
        let mut name_ptrs: Vec<*const c_char> = names_c.iter().map(|s| s.as_ptr()).collect();
        let mut types = [varchar_t, varchar_t, double_t, double_t, double_t, double_t];

        let struct_t = duckdb_create_struct_type(
            types.as_mut_ptr(),
            name_ptrs.as_mut_ptr(),
            types.len() as u64,
        );

        duckdb_destroy_logical_type(&mut varchar_t);
        duckdb_destroy_logical_type(&mut double_t);
        struct_t
    }
}

fn build_node_summary_list_type(is_prev: bool) -> duckdb_logical_type {
    unsafe {
        let mut bigint_t = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_BIGINT);
        let mut double_t = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE);
        let mut bool_t = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN);
        let mut varchar_t = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR);

        let deg_prim_name = if is_prev {
            "out_degree_primary"
        } else {
            "in_degree_primary"
        };
        let deg_sec_name = if is_prev {
            "out_degree_secondary"
        } else {
            "in_degree_secondary"
        };

        let names_c = [
            CString::new("cluster_id").unwrap(),
            CString::new("canonical_rank").unwrap(),
            CString::new("member_count").unwrap(),
            CString::new("member_fraction").unwrap(),
            CString::new(deg_prim_name).unwrap(),
            CString::new(deg_sec_name).unwrap(),
            CString::new("node_role").unwrap(),
            CString::new("participates_in_split").unwrap(),
            CString::new("participates_in_merge").unwrap(),
            CString::new("has_unmatched_reason").unwrap(),
            CString::new("unmatched_reason").unwrap(),
            CString::new("has_min_candidate_distance").unwrap(),
            CString::new("min_candidate_distance").unwrap(),
            CString::new("min_candidate_distance_over_scale").unwrap(),
            CString::new("has_primary_counterpart").unwrap(),
            CString::new("primary_counterpart_cluster_id").unwrap(),
            CString::new("primary_delta_fraction").unwrap(),
            CString::new("attributed_flow_fraction").unwrap(),
            CString::new("unattributed_fraction").unwrap(),
            CString::new("has_threshold_margin").unwrap(),
            CString::new("threshold_margin").unwrap(),
            CString::new("ambiguous_tie").unwrap(),
            CString::new("high_uncertainty").unwrap(),
        ];
        let mut name_ptrs: Vec<*const c_char> = names_c.iter().map(|s| s.as_ptr()).collect();
        let mut types = [
            bigint_t, bigint_t, bigint_t, double_t, bigint_t, bigint_t, varchar_t, bool_t, bool_t,
            bool_t, varchar_t, bool_t, double_t, double_t, bool_t, bigint_t, double_t, double_t,
            double_t, bool_t, double_t, bool_t, bool_t,
        ];

        let mut node_struct = duckdb_create_struct_type(
            types.as_mut_ptr(),
            name_ptrs.as_mut_ptr(),
            types.len() as u64,
        );
        let node_list = duckdb_create_list_type(node_struct);

        duckdb_destroy_logical_type(&mut node_struct);
        duckdb_destroy_logical_type(&mut bigint_t);
        duckdb_destroy_logical_type(&mut double_t);
        duckdb_destroy_logical_type(&mut bool_t);
        duckdb_destroy_logical_type(&mut varchar_t);
        node_list
    }
}

fn build_evolve_return_type() -> duckdb_logical_type {
    unsafe {
        let mut bigint_t = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_BIGINT);
        let mut double_t = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE);
        let mut bool_t = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN);
        let mut varchar_t = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR);

        let mut bigint_list = duckdb_create_list_type(bigint_t);
        let mut varchar_list = duckdb_create_list_type(varchar_t);

        // 1. edges: STRUCT(...)[24 fields]
        let edge_names = [
            CString::new("prev_cluster_id").unwrap(),
            CString::new("curr_cluster_id").unwrap(),
            CString::new("edge_role").unwrap(),
            CString::new("event_type").unwrap(),
            CString::new("rule_basis").unwrap(),
            CString::new("distance").unwrap(),
            CString::new("distance_over_scale").unwrap(),
            CString::new("prev_member_fraction").unwrap(),
            CString::new("curr_member_fraction").unwrap(),
            CString::new("edge_delta_fraction").unwrap(),
            CString::new("attributed_flow").unwrap(),
            CString::new("edge_strength").unwrap(),
            CString::new("threshold_margin").unwrap(),
            CString::new("threshold_margin_ratio").unwrap(),
            CString::new("primary_alternative_status").unwrap(),
            CString::new("has_optimality_margin").unwrap(),
            CString::new("optimality_margin").unwrap(),
            CString::new("optimality_margin_ratio").unwrap(),
            CString::new("has_secondary_margin").unwrap(),
            CString::new("secondary_margin").unwrap(),
            CString::new("secondary_margin_ratio").unwrap(),
            CString::new("ambiguous_tie").unwrap(),
            CString::new("high_uncertainty").unwrap(),
            CString::new("uncertainty_reasons").unwrap(),
        ];
        let mut edge_name_ptrs: Vec<*const c_char> =
            edge_names.iter().map(|s| s.as_ptr()).collect();
        let mut edge_types = [
            bigint_t,
            bigint_t,
            varchar_t,
            varchar_t,
            varchar_t,
            double_t,
            double_t,
            double_t,
            double_t,
            double_t,
            double_t,
            double_t,
            double_t,
            double_t,
            varchar_t,
            bool_t,
            double_t,
            double_t,
            bool_t,
            double_t,
            double_t,
            bool_t,
            bool_t,
            varchar_list,
        ];
        let mut edge_struct = duckdb_create_struct_type(
            edge_types.as_mut_ptr(),
            edge_name_ptrs.as_mut_ptr(),
            edge_types.len() as u64,
        );
        let mut edges_list = duckdb_create_list_type(edge_struct);
        duckdb_destroy_logical_type(&mut edge_struct);

        // 2 & 3. prev_nodes & curr_nodes
        let mut prev_nodes_list = build_node_summary_list_type(true);
        let mut curr_nodes_list = build_node_summary_list_type(false);

        // 4. events: STRUCT(...)[18 fields]
        let ev_names = [
            CString::new("event_index").unwrap(),
            CString::new("event_type").unwrap(),
            CString::new("prev_cluster_ids").unwrap(),
            CString::new("curr_cluster_ids").unwrap(),
            CString::new("has_primary_link").unwrap(),
            CString::new("primary_prev_cluster_id").unwrap(),
            CString::new("primary_curr_cluster_id").unwrap(),
            CString::new("rule_basis").unwrap(),
            CString::new("max_morphology_distance").unwrap(),
            CString::new("max_morphology_over_scale").unwrap(),
            CString::new("prev_fraction_sum").unwrap(),
            CString::new("curr_fraction_sum").unwrap(),
            CString::new("net_delta_fraction").unwrap(),
            CString::new("attributed_flow_sum").unwrap(),
            CString::new("has_min_margin_ratio").unwrap(),
            CString::new("min_margin_ratio").unwrap(),
            CString::new("ambiguous_tie").unwrap(),
            CString::new("high_uncertainty").unwrap(),
        ];
        let mut ev_name_ptrs: Vec<*const c_char> = ev_names.iter().map(|s| s.as_ptr()).collect();
        let mut ev_types = [
            bigint_t,
            varchar_t,
            bigint_list,
            bigint_list,
            bool_t,
            bigint_t,
            bigint_t,
            varchar_t,
            double_t,
            double_t,
            double_t,
            double_t,
            double_t,
            double_t,
            bool_t,
            double_t,
            bool_t,
            bool_t,
        ];
        let mut ev_struct = duckdb_create_struct_type(
            ev_types.as_mut_ptr(),
            ev_name_ptrs.as_mut_ptr(),
            ev_types.len() as u64,
        );
        let mut events_list = duckdb_create_list_type(ev_struct);
        duckdb_destroy_logical_type(&mut ev_struct);

        // 5. summary: STRUCT(...)[31 fields]
        let sum_names = [
            CString::new("primary_edge_count").unwrap(),
            CString::new("secondary_split_edge_count").unwrap(),
            CString::new("secondary_merge_edge_count").unwrap(),
            CString::new("continuation_count").unwrap(),
            CString::new("split_count").unwrap(),
            CString::new("merge_count").unwrap(),
            CString::new("emergence_count").unwrap(),
            CString::new("disappearance_count").unwrap(),
            CString::new("primary_total_distance").unwrap(),
            CString::new("primary_max_distance").unwrap(),
            CString::new("primary_max_distance_over_scale").unwrap(),
            CString::new("all_edges_max_distance").unwrap(),
            CString::new("all_edges_max_distance_over_scale").unwrap(),
            CString::new("transition_tv_redistribution").unwrap(),
            CString::new("secondary_attributed_flow").unwrap(),
            CString::new("total_attributed_flow").unwrap(),
            CString::new("disappeared_fraction_sum").unwrap(),
            CString::new("emerged_fraction_sum").unwrap(),
            CString::new("component_tv_residual").unwrap(),
            CString::new("min_threshold_margin").unwrap(),
            CString::new("min_threshold_margin_ratio").unwrap(),
            CString::new("raw_matrix_threshold_margin").unwrap(),
            CString::new("has_optimality_margin").unwrap(),
            CString::new("min_optimality_margin").unwrap(),
            CString::new("min_optimality_margin_ratio").unwrap(),
            CString::new("has_secondary_margin").unwrap(),
            CString::new("min_secondary_margin").unwrap(),
            CString::new("min_secondary_margin_ratio").unwrap(),
            CString::new("ambiguous_tie").unwrap(),
            CString::new("high_uncertainty_edge_count").unwrap(),
            CString::new("high_uncertainty").unwrap(),
        ];
        let mut sum_name_ptrs: Vec<*const c_char> = sum_names.iter().map(|s| s.as_ptr()).collect();
        let mut sum_types = [
            bigint_t, bigint_t, bigint_t, bigint_t, bigint_t, bigint_t, bigint_t, bigint_t,
            double_t, double_t, double_t, double_t, double_t, double_t, double_t, double_t,
            double_t, double_t, double_t, double_t, double_t, double_t, bool_t, double_t, double_t,
            bool_t, double_t, double_t, bool_t, bigint_t, bool_t,
        ];
        let mut sum_struct = duckdb_create_struct_type(
            sum_types.as_mut_ptr(),
            sum_name_ptrs.as_mut_ptr(),
            sum_types.len() as u64,
        );

        // Outer return STRUCT [19 fields]
        let outer_names = [
            CString::new("algorithm_version").unwrap(),
            CString::new("secondary_rule").unwrap(),
            CString::new("flow_rule").unwrap(),
            CString::new("uncertainty_rule").unwrap(),
            CString::new("non_physical_disclaimer").unwrap(),
            CString::new("prev_cluster_ids").unwrap(),
            CString::new("curr_cluster_ids").unwrap(),
            CString::new("prev_dispersion").unwrap(),
            CString::new("curr_dispersion").unwrap(),
            CString::new("pair_scale").unwrap(),
            CString::new("threshold_coefficient").unwrap(),
            CString::new("threshold").unwrap(),
            CString::new("has_reference_scale").unwrap(),
            CString::new("reference_scale").unwrap(),
            CString::new("edges").unwrap(),
            CString::new("prev_nodes").unwrap(),
            CString::new("curr_nodes").unwrap(),
            CString::new("events").unwrap(),
            CString::new("summary").unwrap(),
        ];
        let mut outer_name_ptrs: Vec<*const c_char> =
            outer_names.iter().map(|s| s.as_ptr()).collect();
        let mut outer_types = [
            varchar_t,
            varchar_t,
            varchar_t,
            varchar_t,
            varchar_t,
            bigint_list,
            bigint_list,
            double_t,
            double_t,
            double_t,
            double_t,
            double_t,
            bool_t,
            double_t,
            edges_list,
            prev_nodes_list,
            curr_nodes_list,
            events_list,
            sum_struct,
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
        duckdb_destroy_logical_type(&mut varchar_list);
        duckdb_destroy_logical_type(&mut edges_list);
        duckdb_destroy_logical_type(&mut prev_nodes_list);
        duckdb_destroy_logical_type(&mut curr_nodes_list);
        duckdb_destroy_logical_type(&mut events_list);
        duckdb_destroy_logical_type(&mut sum_struct);

        ret_struct
    }
}

// ---------------------------------------------------------------------------
// Input Extraction Helpers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) struct DecodedBackboneLink {
    pub(crate) prev_cluster_id: usize,
    pub(crate) curr_cluster_id: usize,
    pub(crate) distance: f64,
    pub(crate) distance_ratio: f64,
    pub(crate) alternative_status: String,
    pub(crate) alternative_edge_count: i64,
    pub(crate) alternative_total_distance: f64,
    pub(crate) alternative_distance_increase: f64,
    pub(crate) alternative_distance_increase_ratio: f64,
    pub(crate) has_same_count_alternative: bool,
    pub(crate) ambiguous: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct DecodedUnmatched {
    pub(crate) cluster_id: i64,
    pub(crate) reason: String,
    pub(crate) min_candidate_distance: f64,
    pub(crate) matched_in_some_optimum: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct DecodedBackboneMeta {
    pub(crate) algorithm_version: String,
    pub(crate) prev_dispersion: f64,
    pub(crate) curr_dispersion: f64,
    pub(crate) pair_scale: f64,
    pub(crate) threshold_coefficient: f64,
    pub(crate) distance_ratio_matrix: Vec<Vec<f64>>,
    pub(crate) edge_count: i64,
    pub(crate) total_distance: f64,
    pub(crate) optimal_solution_count: i64,
    pub(crate) ambiguous: bool,
    pub(crate) threshold_margin: f64,
    pub(crate) optimality_margin: f64,
    pub(crate) has_optimality_margin: bool,
    pub(crate) links: Vec<DecodedBackboneLink>,
    pub(crate) unmatched_prev: Vec<DecodedUnmatched>,
    pub(crate) unmatched_curr: Vec<DecodedUnmatched>,
    pub(crate) assertion: BackboneAssertion,
}

pub(crate) fn validate_positive_member_count(
    func: &'static str,
    row: u64,
    side_name: &'static str,
    idx_field_name: &'static str,
    s_idx: u64,
    cid_raw: i64,
    mcnt_raw: i64,
) -> Result<usize, SqlMatchError> {
    if mcnt_raw <= 0 {
        return Err(SqlMatchError::new(
            func,
            "ZERO_MEMBER_SCENARIO",
            row,
            format!(
                "{side_name}[{s_idx}] ({cid_raw}) has non-positive member_count={mcnt_raw} {idx_field_name}={s_idx}"
            ),
        ));
    }
    usize::try_from(mcnt_raw).map_err(|_| {
        SqlMatchError::new(
            func,
            "SIZE_OVERFLOW",
            row,
            format!("{side_name}[{s_idx}] member_count overflows usize"),
        )
    })
}

unsafe fn extract_evolve_scenarios_side(
    func: &'static str,
    scenarios_vec: duckdb_vector,
    row: u64,
    side_name: &'static str,
    idx_field_name: &'static str,
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
    let mcnt_vec = duckdb_struct_vector_get_child(struct_vec, 1);
    let mfrac_vec = duckdb_struct_vector_get_child(struct_vec, 2);
    let feats_vec = duckdb_struct_vector_get_child(struct_vec, 3);

    let s_validity = duckdb_vector_get_validity(struct_vec);
    let cid_validity = duckdb_vector_get_validity(cid_vec);
    let mcnt_validity = duckdb_vector_get_validity(mcnt_vec);
    let mfrac_validity = duckdb_vector_get_validity(mfrac_vec);

    let cid_data = duckdb_vector_get_data(cid_vec) as *const i64;
    let mcnt_data = duckdb_vector_get_data(mcnt_vec) as *const i64;
    let mfrac_data = duckdb_vector_get_data(mfrac_vec) as *const f64;
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
        if !row_is_valid(mcnt_validity, r) {
            return Err(SqlMatchError::new(
                func,
                "NULL_INPUT",
                row,
                format!("{side_name} member_count at index {idx} is NULL"),
            ));
        }
        if !row_is_valid(mfrac_validity, r) {
            return Err(SqlMatchError::new(
                func,
                "NULL_INPUT",
                row,
                format!("{side_name} member_fraction at index {idx} is NULL"),
            ));
        }

        let cid_raw = *cid_data.add(r as usize);
        if cid_raw < 0 {
            return Err(SqlMatchError::new(
                func,
                "INVALID_SCENARIO_ID",
                row,
                format!("cluster_id must be non-negative, got {cid_raw} {idx_field_name}={idx}"),
            ));
        }
        let cluster_id = cid_raw as usize;

        let mcnt_raw = *mcnt_data.add(r as usize);
        let member_count = validate_positive_member_count(
            func,
            row,
            side_name,
            idx_field_name,
            idx,
            cid_raw,
            mcnt_raw,
        )?;
        let member_fraction = *mfrac_data.add(r as usize);

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
            mean_features.push(*f_data.add(fr as usize));
        }

        scenarios.push(ScenarioNodeInput {
            cluster_id,
            mean_features,
            member_count,
            member_fraction,
        });
    }

    Ok(scenarios)
}

unsafe fn extract_i64_list(
    func: &'static str,
    list_vec: duckdb_vector,
    index: u64,
    row: u64,
    field_label: &'static str,
) -> Result<Vec<i64>, SqlMatchError> {
    let entry = checked_list_entry(func, list_vec, index, row)?;
    let child = duckdb_list_vector_get_child(list_vec);
    let validity = duckdb_vector_get_validity(child);
    let data = duckdb_vector_get_data(child) as *const i64;
    let mut out = Vec::with_capacity(entry.length as usize);
    for i in 0..entry.length {
        let r = entry.offset + i;
        if !row_is_valid(validity, r) {
            return Err(SqlMatchError::new(
                func,
                "NULL_INPUT",
                row,
                format!("{field_label}[{i}] is NULL"),
            ));
        }
        out.push(*data.add(r as usize));
    }
    Ok(out)
}

unsafe fn extract_f64_2d_list(
    func: &'static str,
    mat_vec: duckdb_vector,
    row: u64,
    field_label: &'static str,
) -> Result<Vec<Vec<f64>>, SqlMatchError> {
    let outer = checked_list_entry(func, mat_vec, row, row)?;
    let inner_vec = duckdb_list_vector_get_child(mat_vec);
    let cell_vec = duckdb_list_vector_get_child(inner_vec);
    let cell_validity = duckdb_vector_get_validity(cell_vec);
    let cell_data = duckdb_vector_get_data(cell_vec) as *const f64;

    let mut mat = Vec::with_capacity(outer.length as usize);
    for i in 0..outer.length {
        let inner = checked_list_entry(func, inner_vec, outer.offset + i, row)?;
        let mut row_vals = Vec::with_capacity(inner.length as usize);
        for j in 0..inner.length {
            let c_idx = inner.offset + j;
            if !row_is_valid(cell_validity, c_idx) {
                return Err(SqlMatchError::new(
                    func,
                    "NULL_INPUT",
                    row,
                    format!("{field_label}[{i}][{j}] is NULL"),
                ));
            }
            row_vals.push(*cell_data.add(c_idx as usize));
        }
        mat.push(row_vals);
    }
    Ok(mat)
}

unsafe fn extract_bool_2d_list(
    func: &'static str,
    mat_vec: duckdb_vector,
    row: u64,
    field_label: &'static str,
) -> Result<Vec<Vec<bool>>, SqlMatchError> {
    let outer = checked_list_entry(func, mat_vec, row, row)?;
    let inner_vec = duckdb_list_vector_get_child(mat_vec);
    let cell_vec = duckdb_list_vector_get_child(inner_vec);
    let cell_validity = duckdb_vector_get_validity(cell_vec);
    let cell_data = duckdb_vector_get_data(cell_vec) as *const bool;

    let mut mat = Vec::with_capacity(outer.length as usize);
    for i in 0..outer.length {
        let inner = checked_list_entry(func, inner_vec, outer.offset + i, row)?;
        let mut row_vals = Vec::with_capacity(inner.length as usize);
        for j in 0..inner.length {
            let c_idx = inner.offset + j;
            if !row_is_valid(cell_validity, c_idx) {
                return Err(SqlMatchError::new(
                    func,
                    "NULL_INPUT",
                    row,
                    format!("{field_label}[{i}][{j}] is NULL"),
                ));
            }
            row_vals.push(*cell_data.add(c_idx as usize));
        }
        mat.push(row_vals);
    }
    Ok(mat)
}

unsafe fn extract_unmatched_list(
    func: &'static str,
    list_vec: duckdb_vector,
    row: u64,
    field_label: &'static str,
) -> Result<Vec<DecodedUnmatched>, SqlMatchError> {
    let entry = checked_list_entry(func, list_vec, row, row)?;
    let struct_vec = duckdb_list_vector_get_child(list_vec);
    let s_validity = duckdb_vector_get_validity(struct_vec);

    let cid_v = duckdb_struct_vector_get_child(struct_vec, 0);
    let reason_v = duckdb_struct_vector_get_child(struct_vec, 1);
    let min_d_v = duckdb_struct_vector_get_child(struct_vec, 2);
    let opt_v = duckdb_struct_vector_get_child(struct_vec, 3);

    let cid_val = duckdb_vector_get_validity(cid_v);
    let reason_val = duckdb_vector_get_validity(reason_v);
    let min_d_val = duckdb_vector_get_validity(min_d_v);
    let opt_val = duckdb_vector_get_validity(opt_v);

    let cid_data = duckdb_vector_get_data(cid_v) as *const i64;
    let min_d_data = duckdb_vector_get_data(min_d_v) as *const f64;
    let opt_data = duckdb_vector_get_data(opt_v) as *const bool;

    let mut out = Vec::with_capacity(entry.length as usize);
    for i in 0..entry.length {
        let r = entry.offset + i;
        if !row_is_valid(s_validity, r)
            || !row_is_valid(cid_val, r)
            || !row_is_valid(reason_val, r)
            || !row_is_valid(min_d_val, r)
            || !row_is_valid(opt_val, r)
        {
            return Err(SqlMatchError::new(
                func,
                "NULL_INPUT",
                row,
                format!("{field_label}[{i}] contains NULL"),
            ));
        }
        let cid_raw = *cid_data.add(r as usize);
        let min_d = *min_d_data.add(r as usize);
        let reason = read_string(reason_v, r);
        let in_opt = *opt_data.add(r as usize);
        out.push(DecodedUnmatched {
            cluster_id: cid_raw,
            reason,
            min_candidate_distance: min_d,
            matched_in_some_optimum: in_opt,
        });
    }
    Ok(out)
}

unsafe fn extract_backbone_match(
    func: &'static str,
    backbone_vec: duckdb_vector,
    row: u64,
) -> Result<DecodedBackboneMeta, SqlMatchError> {
    if !row_is_valid(duckdb_vector_get_validity(backbone_vec), row) {
        return Err(SqlMatchError::new(
            func,
            "NULL_INPUT",
            row,
            "backbone_match struct is NULL",
        ));
    }

    let children: [duckdb_vector; 21] =
        std::array::from_fn(|idx| duckdb_struct_vector_get_child(backbone_vec, idx as u64));
    for (idx, &ch) in children.iter().enumerate() {
        if !row_is_valid(duckdb_vector_get_validity(ch), row) {
            return Err(SqlMatchError::new(
                func,
                "NULL_INPUT",
                row,
                format!("backbone_match field {idx} is NULL"),
            ));
        }
    }

    let algorithm_version = read_string(children[0], row);
    let prev_ids_raw = extract_i64_list(func, children[1], row, row, "backbone.prev_cluster_ids")?;
    let curr_ids_raw = extract_i64_list(func, children[2], row, row, "backbone.curr_cluster_ids")?;

    let prev_dispersion = *(duckdb_vector_get_data(children[3]) as *const f64).add(row as usize);
    let curr_dispersion = *(duckdb_vector_get_data(children[4]) as *const f64).add(row as usize);
    let pair_scale = *(duckdb_vector_get_data(children[5]) as *const f64).add(row as usize);
    let threshold_coefficient =
        *(duckdb_vector_get_data(children[6]) as *const f64).add(row as usize);
    let threshold = *(duckdb_vector_get_data(children[7]) as *const f64).add(row as usize);

    let distance_matrix = extract_f64_2d_list(func, children[8], row, "backbone.distance_matrix")?;
    let distance_ratio_matrix =
        extract_f64_2d_list(func, children[9], row, "backbone.distance_ratio_matrix")?;
    let eligible_matrix =
        extract_bool_2d_list(func, children[10], row, "backbone.eligible_matrix")?;

    let edge_count = *(duckdb_vector_get_data(children[11]) as *const i64).add(row as usize);
    let total_distance = *(duckdb_vector_get_data(children[12]) as *const f64).add(row as usize);
    let optimal_solution_count =
        *(duckdb_vector_get_data(children[13]) as *const i64).add(row as usize);
    let ambiguous = *(duckdb_vector_get_data(children[14]) as *const bool).add(row as usize);
    let threshold_margin = *(duckdb_vector_get_data(children[15]) as *const f64).add(row as usize);
    let optimality_margin = *(duckdb_vector_get_data(children[16]) as *const f64).add(row as usize);
    let has_optimality_margin =
        *(duckdb_vector_get_data(children[17]) as *const bool).add(row as usize);

    // Validate links list & decode all 11 nested link fields
    let links_entry = checked_list_entry(func, children[18], row, row)?;
    let link_struct_v = duckdb_list_vector_get_child(children[18]);
    let link_s_validity = duckdb_vector_get_validity(link_struct_v);
    let link_children: [duckdb_vector; 11] =
        std::array::from_fn(|idx| duckdb_struct_vector_get_child(link_struct_v, idx as u64));
    let l_prev_data = duckdb_vector_get_data(link_children[0]) as *const i64;
    let l_curr_data = duckdb_vector_get_data(link_children[1]) as *const i64;
    let l_dist_data = duckdb_vector_get_data(link_children[2]) as *const f64;
    let l_dist_ratio_data = duckdb_vector_get_data(link_children[3]) as *const f64;
    let l_alt_edge_data = duckdb_vector_get_data(link_children[5]) as *const i64;
    let l_alt_tot_data = duckdb_vector_get_data(link_children[6]) as *const f64;
    let l_alt_inc_data = duckdb_vector_get_data(link_children[7]) as *const f64;
    let l_alt_inc_r_data = duckdb_vector_get_data(link_children[8]) as *const f64;
    let l_has_alt_data = duckdb_vector_get_data(link_children[9]) as *const bool;
    let l_amb_data = duckdb_vector_get_data(link_children[10]) as *const bool;

    let mut raw_links = Vec::with_capacity(links_entry.length as usize);
    for l_i in 0..links_entry.length {
        let lr = links_entry.offset + l_i;
        if !row_is_valid(link_s_validity, lr) {
            return Err(SqlMatchError::new(
                func,
                "NULL_INPUT",
                row,
                format!("backbone_match.links[{l_i}] is NULL"),
            ));
        }
        for (f_idx, &l_ch) in link_children.iter().enumerate() {
            if !row_is_valid(duckdb_vector_get_validity(l_ch), lr) {
                return Err(SqlMatchError::new(
                    func,
                    "NULL_INPUT",
                    row,
                    format!("backbone_match.links[{l_i}] field {f_idx} is NULL"),
                ));
            }
        }
        let lru = lr as usize;
        raw_links.push((
            *l_prev_data.add(lru),
            *l_curr_data.add(lru),
            *l_dist_data.add(lru),
            *l_dist_ratio_data.add(lru),
            read_string(link_children[4], lr),
            *l_alt_edge_data.add(lru),
            *l_alt_tot_data.add(lru),
            *l_alt_inc_data.add(lru),
            *l_alt_inc_r_data.add(lru),
            *l_has_alt_data.add(lru),
            *l_amb_data.add(lru),
        ));
    }

    let unmatched_prev =
        extract_unmatched_list(func, children[19], row, "backbone.unmatched_prev")?;
    let unmatched_curr =
        extract_unmatched_list(func, children[20], row, "backbone.unmatched_curr")?;

    let mut prev_cluster_ids = Vec::with_capacity(prev_ids_raw.len());
    for id in prev_ids_raw {
        if id < 0 {
            return Err(SqlMatchError::new(
                func,
                "BACKBONE_INCONSISTENCY",
                row,
                format!("backbone_match.prev_cluster_ids contains negative id {id}"),
            ));
        }
        prev_cluster_ids.push(id as usize);
    }

    let mut curr_cluster_ids = Vec::with_capacity(curr_ids_raw.len());
    for id in curr_ids_raw {
        if id < 0 {
            return Err(SqlMatchError::new(
                func,
                "BACKBONE_INCONSISTENCY",
                row,
                format!("backbone_match.curr_cluster_ids contains negative id {id}"),
            ));
        }
        curr_cluster_ids.push(id as usize);
    }

    let mut primary_links = Vec::with_capacity(raw_links.len());
    let mut links = Vec::with_capacity(raw_links.len());
    for (
        pos,
        (
            p_id,
            c_id,
            distance,
            distance_ratio,
            alternative_status,
            alternative_edge_count,
            alternative_total_distance,
            alternative_distance_increase,
            alternative_distance_increase_ratio,
            has_same_count_alternative,
            link_ambiguous,
        ),
    ) in raw_links.into_iter().enumerate()
    {
        if p_id < 0 || c_id < 0 {
            return Err(SqlMatchError::new(
                func,
                "INVALID_PRIMARY_LINK",
                row,
                format!(
                    "backbone_match.links[{pos}] contains negative cluster_id ({p_id}, {c_id})"
                ),
            ));
        }
        let pu = p_id as usize;
        let cu = c_id as usize;
        primary_links.push((pu, cu));
        links.push(DecodedBackboneLink {
            prev_cluster_id: pu,
            curr_cluster_id: cu,
            distance,
            distance_ratio,
            alternative_status,
            alternative_edge_count,
            alternative_total_distance,
            alternative_distance_increase,
            alternative_distance_increase_ratio,
            has_same_count_alternative,
            ambiguous: link_ambiguous,
        });
    }

    Ok(DecodedBackboneMeta {
        algorithm_version,
        prev_dispersion,
        curr_dispersion,
        pair_scale,
        threshold_coefficient,
        distance_ratio_matrix,
        edge_count,
        total_distance,
        optimal_solution_count,
        ambiguous,
        threshold_margin,
        optimality_margin,
        has_optimality_margin,
        links,
        unmatched_prev,
        unmatched_curr,
        assertion: BackboneAssertion {
            prev_cluster_ids,
            curr_cluster_ids,
            threshold,
            distance_matrix: Some(distance_matrix),
            eligible_matrix: Some(eligible_matrix),
            primary_links,
        },
    })
}

unsafe fn extract_evolve_config(
    func: &'static str,
    config_vec: duckdb_vector,
    row: u64,
) -> Result<EvolutionConfig, SqlMatchError> {
    if !row_is_valid(duckdb_vector_get_validity(config_vec), row) {
        return Err(SqlMatchError::new(
            func,
            "NULL_INPUT",
            row,
            "config struct is NULL",
        ));
    }

    let children: [duckdb_vector; 6] =
        std::array::from_fn(|idx| duckdb_struct_vector_get_child(config_vec, idx as u64));
    for (idx, &ch) in children.iter().enumerate() {
        if !row_is_valid(duckdb_vector_get_validity(ch), row) {
            return Err(SqlMatchError::new(
                func,
                "NULL_INPUT",
                row,
                format!("config field {idx} is NULL"),
            ));
        }
    }

    let sec_str = read_string(children[0], row);
    let flow_str = read_string(children[1], row);
    let frac_tol = *(duckdb_vector_get_data(children[2]) as *const f64).add(row as usize);
    let hu_thr = *(duckdb_vector_get_data(children[3]) as *const f64).add(row as usize);
    let hu_opt = *(duckdb_vector_get_data(children[4]) as *const f64).add(row as usize);
    let hu_sec = *(duckdb_vector_get_data(children[5]) as *const f64).add(row as usize);

    let secondary_rule = match sec_str.as_str() {
        "nearest_feasible_v1" => SecondaryRule::NearestFeasibleV1,
        other => {
            return Err(SqlMatchError::new(
                func,
                "INVALID_EVOLUTION_CONFIG",
                row,
                format!("unsupported secondary_rule: {other}"),
            ));
        }
    };
    let flow_rule = match flow_str.as_str() {
        "star_min_mass_proportional_v1" => FlowRule::StarMinMassProportionalV1,
        other => {
            return Err(SqlMatchError::new(
                func,
                "INVALID_EVOLUTION_CONFIG",
                row,
                format!("unsupported flow_rule: {other}"),
            ));
        }
    };

    Ok(EvolutionConfig {
        secondary_rule,
        flow_rule,
        fraction_sum_tolerance: frac_tol,
        high_uncertainty_threshold_ratio: hu_thr,
        high_uncertainty_optimality_ratio: hu_opt,
        high_uncertainty_secondary_ratio: hu_sec,
    })
}

pub(crate) fn verify_extra_backbone_meta(
    func: &'static str,
    row: u64,
    meta: &DecodedBackboneMeta,
    res: &EvolutionResult,
) -> Result<(), SqlMatchError> {
    if meta.algorithm_version != MATCH_ALGORITHM_VERSION {
        return Err(SqlMatchError::new(
            func,
            "BACKBONE_INCONSISTENCY",
            row,
            format!(
                "backbone_match.algorithm_version '{}' != '{}'",
                meta.algorithm_version, MATCH_ALGORITHM_VERSION
            ),
        ));
    }

    let exp_prev_disp = res.prev_dispersion.unwrap_or(0.0);
    if !f64_eq_exact(meta.prev_dispersion, exp_prev_disp) {
        return Err(SqlMatchError::new(
            func,
            "BACKBONE_INCONSISTENCY",
            row,
            format!(
                "backbone_match.prev_dispersion {} != expected {}",
                meta.prev_dispersion, exp_prev_disp
            ),
        ));
    }

    let exp_curr_disp = res.curr_dispersion.unwrap_or(0.0);
    if !f64_eq_exact(meta.curr_dispersion, exp_curr_disp) {
        return Err(SqlMatchError::new(
            func,
            "BACKBONE_INCONSISTENCY",
            row,
            format!(
                "backbone_match.curr_dispersion {} != expected {}",
                meta.curr_dispersion, exp_curr_disp
            ),
        ));
    }

    let exp_scale = norm_f64(res.pair_scale.unwrap_or(0.0));
    if !f64_eq_exact(meta.pair_scale, exp_scale) {
        return Err(SqlMatchError::new(
            func,
            "BACKBONE_INCONSISTENCY",
            row,
            format!(
                "backbone_match.pair_scale {} != expected {}",
                meta.pair_scale, exp_scale
            ),
        ));
    }

    let exp_coeff = res.threshold_coefficient.unwrap_or(0.0);
    if !f64_eq_exact(meta.threshold_coefficient, exp_coeff) {
        return Err(SqlMatchError::new(
            func,
            "BACKBONE_INCONSISTENCY",
            row,
            format!(
                "backbone_match.threshold_coefficient {} != expected {}",
                meta.threshold_coefficient, exp_coeff
            ),
        ));
    }

    let m = res.prev_cluster_ids.len();
    let n = res.curr_cluster_ids.len();

    if meta.distance_ratio_matrix.len() != m {
        return Err(SqlMatchError::new(
            func,
            "BACKBONE_INCONSISTENCY",
            row,
            format!(
                "backbone_match.distance_ratio_matrix row count {} != expected {m}",
                meta.distance_ratio_matrix.len()
            ),
        ));
    }
    for i in 0..m {
        if meta.distance_ratio_matrix[i].len() != n {
            return Err(SqlMatchError::new(
                func,
                "BACKBONE_INCONSISTENCY",
                row,
                format!(
                    "backbone_match.distance_ratio_matrix[{i}] col count {} != expected {n}",
                    meta.distance_ratio_matrix[i].len()
                ),
            ));
        }
        for j in 0..n {
            let got = meta.distance_ratio_matrix[i][j];
            let exp = res.backbone.distance_matrix[i][j] / exp_scale;
            if !f64_eq_exact(got, exp) {
                return Err(SqlMatchError::new(
                    func,
                    "BACKBONE_INCONSISTENCY",
                    row,
                    format!(
                        "backbone_match.distance_ratio_matrix[{i}][{j}] = {got} != expected {exp}"
                    ),
                ));
            }
        }
    }

    if meta.edge_count != res.backbone.objective.matched_count as i64 {
        return Err(SqlMatchError::new(
            func,
            "BACKBONE_INCONSISTENCY",
            row,
            format!(
                "backbone_match.edge_count {} != expected {}",
                meta.edge_count, res.backbone.objective.matched_count
            ),
        ));
    }

    let exp_tot = norm_f64(res.backbone.objective.weighted_distance_sum);
    if !f64_eq_exact(meta.total_distance, exp_tot) {
        return Err(SqlMatchError::new(
            func,
            "BACKBONE_INCONSISTENCY",
            row,
            format!(
                "backbone_match.total_distance {} != expected {}",
                meta.total_distance, exp_tot
            ),
        ));
    }

    if meta.optimal_solution_count != res.backbone.optima_count as i64 {
        return Err(SqlMatchError::new(
            func,
            "BACKBONE_INCONSISTENCY",
            row,
            format!(
                "backbone_match.optimal_solution_count {} != expected {}",
                meta.optimal_solution_count, res.backbone.optima_count
            ),
        ));
    }

    let exp_ambiguous =
        res.backbone.links.iter().any(|l| l.ambiguous) || res.backbone.optima_count > 1;
    if meta.ambiguous != exp_ambiguous {
        return Err(SqlMatchError::new(
            func,
            "BACKBONE_INCONSISTENCY",
            row,
            format!(
                "backbone_match.ambiguous {} != expected {exp_ambiguous}",
                meta.ambiguous
            ),
        ));
    }

    let mut exp_thresh_margin = f64::INFINITY;
    for i in 0..m {
        for j in 0..n {
            let margin = (res.backbone.distance_matrix[i][j] - res.threshold).abs();
            if margin < exp_thresh_margin {
                exp_thresh_margin = margin;
            }
        }
    }
    if !exp_thresh_margin.is_finite() {
        exp_thresh_margin = 0.0;
    }
    if !f64_eq_exact(meta.threshold_margin, exp_thresh_margin) {
        return Err(SqlMatchError::new(
            func,
            "BACKBONE_INCONSISTENCY",
            row,
            format!(
                "backbone_match.threshold_margin {} != expected {exp_thresh_margin}",
                meta.threshold_margin
            ),
        ));
    }

    let mut exp_opt_margin = f64::INFINITY;
    let mut exp_has_opt_margin = false;
    for link in &res.backbone.links {
        match &link.substitutability {
            Substitutability::Tied => {
                exp_has_opt_margin = true;
                exp_opt_margin = 0.0;
            }
            Substitutability::SameCount { gap, .. } => {
                exp_has_opt_margin = true;
                if *gap < exp_opt_margin {
                    exp_opt_margin = *gap;
                }
            }
            Substitutability::CountDrop { .. } => {}
        }
    }
    if !exp_has_opt_margin || !exp_opt_margin.is_finite() {
        exp_opt_margin = 0.0;
    }
    if meta.has_optimality_margin != exp_has_opt_margin {
        return Err(SqlMatchError::new(
            func,
            "BACKBONE_INCONSISTENCY",
            row,
            format!(
                "backbone_match.has_optimality_margin {} != expected {exp_has_opt_margin}",
                meta.has_optimality_margin
            ),
        ));
    }
    if !f64_eq_exact(meta.optimality_margin, exp_opt_margin) {
        return Err(SqlMatchError::new(
            func,
            "BACKBONE_INCONSISTENCY",
            row,
            format!(
                "backbone_match.optimality_margin {} != expected {exp_opt_margin}",
                meta.optimality_margin
            ),
        ));
    }

    if meta.links.len() != res.backbone.links.len() {
        return Err(SqlMatchError::new(
            func,
            "BACKBONE_INCONSISTENCY",
            row,
            format!(
                "backbone_match.links length {} != expected {}",
                meta.links.len(),
                res.backbone.links.len()
            ),
        ));
    }

    for (k, (got, exp)) in meta.links.iter().zip(res.backbone.links.iter()).enumerate() {
        if got.prev_cluster_id != exp.prev_cluster_id || got.curr_cluster_id != exp.curr_cluster_id
        {
            return Err(SqlMatchError::new(
                func,
                "BACKBONE_INCONSISTENCY",
                row,
                format!(
                    "backbone_match.links[{k}] pair ({}, {}) != expected ({}, {})",
                    got.prev_cluster_id,
                    got.curr_cluster_id,
                    exp.prev_cluster_id,
                    exp.curr_cluster_id
                ),
            ));
        }
        if !f64_eq_exact(got.distance, exp.distance_k) {
            return Err(SqlMatchError::new(
                func,
                "BACKBONE_INCONSISTENCY",
                row,
                format!(
                    "backbone_match.links[{k}].distance {} != expected {}",
                    got.distance, exp.distance_k
                ),
            ));
        }
        let exp_dist_ratio = exp
            .distance_ratio
            .unwrap_or_else(|| exp.distance_k / exp_scale);
        if !f64_eq_exact(got.distance_ratio, exp_dist_ratio) {
            return Err(SqlMatchError::new(
                func,
                "BACKBONE_INCONSISTENCY",
                row,
                format!(
                    "backbone_match.links[{k}].distance_ratio {} != expected {exp_dist_ratio}",
                    got.distance_ratio
                ),
            ));
        }

        let (exp_status, exp_alt_cnt, exp_alt_tot, exp_alt_inc, exp_alt_inc_r, exp_has_alt) =
            match &exp.substitutability {
                Substitutability::Tied => (
                    "tied",
                    res.backbone.objective.matched_count as i64,
                    res.backbone.objective.weighted_distance_sum,
                    0.0,
                    0.0,
                    true,
                ),
                Substitutability::SameCount { gap, alt_objective } => (
                    "same_count",
                    res.backbone.objective.matched_count as i64,
                    *alt_objective,
                    *gap,
                    *gap / exp_scale,
                    true,
                ),
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

        if got.alternative_status != exp_status {
            return Err(SqlMatchError::new(
                func,
                "BACKBONE_INCONSISTENCY",
                row,
                format!(
                    "backbone_match.links[{k}].alternative_status '{}' != expected '{exp_status}'",
                    got.alternative_status
                ),
            ));
        }
        if got.alternative_edge_count != exp_alt_cnt {
            return Err(SqlMatchError::new(
                func,
                "BACKBONE_INCONSISTENCY",
                row,
                format!(
                    "backbone_match.links[{k}].alternative_edge_count {} != expected {exp_alt_cnt}",
                    got.alternative_edge_count
                ),
            ));
        }
        if !f64_eq_exact(got.alternative_total_distance, exp_alt_tot) {
            return Err(SqlMatchError::new(
                func,
                "BACKBONE_INCONSISTENCY",
                row,
                format!(
                    "backbone_match.links[{k}].alternative_total_distance {} != expected {exp_alt_tot}",
                    got.alternative_total_distance
                ),
            ));
        }
        if !f64_eq_exact(got.alternative_distance_increase, exp_alt_inc) {
            return Err(SqlMatchError::new(
                func,
                "BACKBONE_INCONSISTENCY",
                row,
                format!(
                    "backbone_match.links[{k}].alternative_distance_increase {} != expected {exp_alt_inc}",
                    got.alternative_distance_increase
                ),
            ));
        }
        if !f64_eq_exact(got.alternative_distance_increase_ratio, exp_alt_inc_r) {
            return Err(SqlMatchError::new(
                func,
                "BACKBONE_INCONSISTENCY",
                row,
                format!(
                    "backbone_match.links[{k}].alternative_distance_increase_ratio {} != expected {exp_alt_inc_r}",
                    got.alternative_distance_increase_ratio
                ),
            ));
        }
        if got.has_same_count_alternative != exp_has_alt {
            return Err(SqlMatchError::new(
                func,
                "BACKBONE_INCONSISTENCY",
                row,
                format!(
                    "backbone_match.links[{k}].has_same_count_alternative {} != expected {exp_has_alt}",
                    got.has_same_count_alternative
                ),
            ));
        }
        if got.ambiguous != exp.ambiguous {
            return Err(SqlMatchError::new(
                func,
                "BACKBONE_INCONSISTENCY",
                row,
                format!(
                    "backbone_match.links[{k}].ambiguous {} != expected {}",
                    got.ambiguous, exp.ambiguous
                ),
            ));
        }
    }

    if meta.unmatched_prev.len() != res.backbone.unmatched_prev.len() {
        return Err(SqlMatchError::new(
            func,
            "BACKBONE_INCONSISTENCY",
            row,
            format!(
                "backbone_match.unmatched_prev length {} != expected {}",
                meta.unmatched_prev.len(),
                res.backbone.unmatched_prev.len()
            ),
        ));
    }
    for (k, (got, exp)) in meta
        .unmatched_prev
        .iter()
        .zip(res.backbone.unmatched_prev.iter())
        .enumerate()
    {
        if got.cluster_id < 0 || (got.cluster_id as usize) != exp.cluster_id {
            return Err(SqlMatchError::new(
                func,
                "BACKBONE_INCONSISTENCY",
                row,
                format!(
                    "backbone_match.unmatched_prev[{k}].cluster_id {} != expected {}",
                    got.cluster_id, exp.cluster_id
                ),
            ));
        }
        if got.reason != exp.reason {
            return Err(SqlMatchError::new(
                func,
                "BACKBONE_INCONSISTENCY",
                row,
                format!(
                    "backbone_match.unmatched_prev[{k}].reason '{}' != expected '{}'",
                    got.reason, exp.reason
                ),
            ));
        }
        let p_idx = res
            .prev_cluster_ids
            .iter()
            .position(|&cid| cid == exp.cluster_id)
            .unwrap_or(0);
        let min_d = (0..n)
            .map(|j| res.backbone.distance_matrix[p_idx][j])
            .fold(f64::INFINITY, f64::min);
        let exp_min_d = if min_d.is_finite() { min_d } else { 0.0 };
        if !f64_eq_exact(got.min_candidate_distance, exp_min_d) {
            return Err(SqlMatchError::new(
                func,
                "BACKBONE_INCONSISTENCY",
                row,
                format!(
                    "backbone_match.unmatched_prev[{k}].min_candidate_distance {} != expected {exp_min_d}",
                    got.min_candidate_distance
                ),
            ));
        }
        if got.matched_in_some_optimum != exp.matched_in_some_optimum {
            return Err(SqlMatchError::new(
                func,
                "BACKBONE_INCONSISTENCY",
                row,
                format!(
                    "backbone_match.unmatched_prev[{k}].matched_in_some_optimum {} != expected {}",
                    got.matched_in_some_optimum, exp.matched_in_some_optimum
                ),
            ));
        }
    }

    if meta.unmatched_curr.len() != res.backbone.unmatched_curr.len() {
        return Err(SqlMatchError::new(
            func,
            "BACKBONE_INCONSISTENCY",
            row,
            format!(
                "backbone_match.unmatched_curr length {} != expected {}",
                meta.unmatched_curr.len(),
                res.backbone.unmatched_curr.len()
            ),
        ));
    }
    for (k, (got, exp)) in meta
        .unmatched_curr
        .iter()
        .zip(res.backbone.unmatched_curr.iter())
        .enumerate()
    {
        if got.cluster_id < 0 || (got.cluster_id as usize) != exp.cluster_id {
            return Err(SqlMatchError::new(
                func,
                "BACKBONE_INCONSISTENCY",
                row,
                format!(
                    "backbone_match.unmatched_curr[{k}].cluster_id {} != expected {}",
                    got.cluster_id, exp.cluster_id
                ),
            ));
        }
        if got.reason != exp.reason {
            return Err(SqlMatchError::new(
                func,
                "BACKBONE_INCONSISTENCY",
                row,
                format!(
                    "backbone_match.unmatched_curr[{k}].reason '{}' != expected '{}'",
                    got.reason, exp.reason
                ),
            ));
        }
        let c_idx = res
            .curr_cluster_ids
            .iter()
            .position(|&cid| cid == exp.cluster_id)
            .unwrap_or(0);
        let min_d = (0..m)
            .map(|i| res.backbone.distance_matrix[i][c_idx])
            .fold(f64::INFINITY, f64::min);
        let exp_min_d = if min_d.is_finite() { min_d } else { 0.0 };
        if !f64_eq_exact(got.min_candidate_distance, exp_min_d) {
            return Err(SqlMatchError::new(
                func,
                "BACKBONE_INCONSISTENCY",
                row,
                format!(
                    "backbone_match.unmatched_curr[{k}].min_candidate_distance {} != expected {exp_min_d}",
                    got.min_candidate_distance
                ),
            ));
        }
        if got.matched_in_some_optimum != exp.matched_in_some_optimum {
            return Err(SqlMatchError::new(
                func,
                "BACKBONE_INCONSISTENCY",
                row,
                format!(
                    "backbone_match.unmatched_curr[{k}].matched_in_some_optimum {} != expected {}",
                    got.matched_in_some_optimum, exp.matched_in_some_optimum
                ),
            ));
        }
    }

    Ok(())
}

unsafe fn extract_and_run_evolve_row(
    prev_vec: duckdb_vector,
    curr_vec: duckdb_vector,
    weights_vec: duckdb_vector,
    backbone_vec: duckdb_vector,
    config_vec: duckdb_vector,
    row: u64,
) -> Result<EvolutionResult, SqlMatchError> {
    let func = EVOLVE_SCENARIOS_FUNCTION_NAME;

    let prev_scenarios =
        extract_evolve_scenarios_side(func, prev_vec, row, "prev_scenarios", "prev_index")?;
    let curr_scenarios =
        extract_evolve_scenarios_side(func, curr_vec, row, "curr_scenarios", "curr_index")?;

    if !row_is_valid(duckdb_vector_get_validity(weights_vec), row) {
        return Err(SqlMatchError::new(
            func,
            "NULL_INPUT",
            row,
            "weights list is NULL",
        ));
    }
    let w_entry = checked_list_entry(func, weights_vec, row, row)?;
    if w_entry.length == 0 {
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
    let mut weights = Vec::with_capacity(w_entry.length as usize);
    for idx in 0..w_entry.length {
        let r = w_entry.offset + idx;
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

    let bb_meta = extract_backbone_match(func, backbone_vec, row)?;
    let config = extract_evolve_config(func, config_vec, row)?;

    let scaled_input = ScaledEvolutionInput {
        prev_scenarios,
        curr_scenarios,
        weights,
        threshold_coefficient: bb_meta.threshold_coefficient,
        prev_dispersion: bb_meta.prev_dispersion,
        curr_dispersion: bb_meta.curr_dispersion,
        config,
        backbone: Some(bb_meta.assertion.clone()),
    };

    let res = evolve_scenarios_scaled(&scaled_input)
        .map_err(|e| SqlMatchError::from_core(func, e, row))?;
    verify_extra_backbone_meta(func, row, &bb_meta, &res)?;
    Ok(res)
}

// ---------------------------------------------------------------------------
// Writing Output Vectors (after entire chunk succeeds)
// ---------------------------------------------------------------------------

unsafe fn assign_str(vec: duckdb_vector, row: u64, text: &str) {
    duckdb_vector_assign_string_element_len(vec, row, text.as_ptr().cast(), text.len() as u64);
}

unsafe fn write_node_summaries(
    list_vec: duckdb_vector,
    row: usize,
    nodes: &[NodeEvolutionSummary],
    offset: &mut u64,
) {
    let start = *offset;
    let len = nodes.len() as u64;
    *(duckdb_vector_get_data(list_vec) as *mut duckdb_list_entry).add(row) = duckdb_list_entry {
        offset: start,
        length: len,
    };

    let n_struct = duckdb_list_vector_get_child(list_vec);
    let c0 = duckdb_vector_get_data(duckdb_struct_vector_get_child(n_struct, 0)) as *mut i64;
    let c1 = duckdb_vector_get_data(duckdb_struct_vector_get_child(n_struct, 1)) as *mut i64;
    let c2 = duckdb_vector_get_data(duckdb_struct_vector_get_child(n_struct, 2)) as *mut i64;
    let c3 = duckdb_vector_get_data(duckdb_struct_vector_get_child(n_struct, 3)) as *mut f64;
    let c4 = duckdb_vector_get_data(duckdb_struct_vector_get_child(n_struct, 4)) as *mut i64;
    let c5 = duckdb_vector_get_data(duckdb_struct_vector_get_child(n_struct, 5)) as *mut i64;
    let v6 = duckdb_struct_vector_get_child(n_struct, 6);
    let c7 = duckdb_vector_get_data(duckdb_struct_vector_get_child(n_struct, 7)) as *mut bool;
    let c8 = duckdb_vector_get_data(duckdb_struct_vector_get_child(n_struct, 8)) as *mut bool;
    let c9 = duckdb_vector_get_data(duckdb_struct_vector_get_child(n_struct, 9)) as *mut bool;
    let v10 = duckdb_struct_vector_get_child(n_struct, 10);
    let c11 = duckdb_vector_get_data(duckdb_struct_vector_get_child(n_struct, 11)) as *mut bool;
    let c12 = duckdb_vector_get_data(duckdb_struct_vector_get_child(n_struct, 12)) as *mut f64;
    let c13 = duckdb_vector_get_data(duckdb_struct_vector_get_child(n_struct, 13)) as *mut f64;
    let c14 = duckdb_vector_get_data(duckdb_struct_vector_get_child(n_struct, 14)) as *mut bool;
    let c15 = duckdb_vector_get_data(duckdb_struct_vector_get_child(n_struct, 15)) as *mut i64;
    let c16 = duckdb_vector_get_data(duckdb_struct_vector_get_child(n_struct, 16)) as *mut f64;
    let c17 = duckdb_vector_get_data(duckdb_struct_vector_get_child(n_struct, 17)) as *mut f64;
    let c18 = duckdb_vector_get_data(duckdb_struct_vector_get_child(n_struct, 18)) as *mut f64;
    let c19 = duckdb_vector_get_data(duckdb_struct_vector_get_child(n_struct, 19)) as *mut bool;
    let c20 = duckdb_vector_get_data(duckdb_struct_vector_get_child(n_struct, 20)) as *mut f64;
    let c21 = duckdb_vector_get_data(duckdb_struct_vector_get_child(n_struct, 21)) as *mut bool;
    let c22 = duckdb_vector_get_data(duckdb_struct_vector_get_child(n_struct, 22)) as *mut bool;

    for (idx, n) in nodes.iter().enumerate() {
        let nr = (start as usize) + idx;
        *c0.add(nr) = n.cluster_id as i64;
        *c1.add(nr) = n.canonical_rank as i64;
        *c2.add(nr) = n.member_count as i64;
        *c3.add(nr) = norm_f64(n.member_fraction);
        *c4.add(nr) = n.primary_degree as i64;
        *c5.add(nr) = n.secondary_degree as i64;
        assign_str(v6, nr as u64, n.node_role.as_str());
        *c7.add(nr) = n.participates_in_split;
        *c8.add(nr) = n.participates_in_merge;

        *c9.add(nr) = n.unmatched_reason.is_some();
        assign_str(
            v10,
            nr as u64,
            n.unmatched_reason.map(|r| r.as_str()).unwrap_or(""),
        );

        *c11.add(nr) = n.min_candidate_distance.is_some();
        *c12.add(nr) = norm_f64(n.min_candidate_distance.unwrap_or(0.0));
        *c13.add(nr) = norm_f64(n.min_candidate_distance_over_scale.unwrap_or(0.0));

        *c14.add(nr) = n.primary_counterpart_cluster_id.is_some();
        *c15.add(nr) = n
            .primary_counterpart_cluster_id
            .map(|id| id as i64)
            .unwrap_or(-1);
        *c16.add(nr) = norm_f64(n.primary_delta_fraction.unwrap_or(0.0));
        *c17.add(nr) = norm_f64(n.attributed_flow_fraction);
        *c18.add(nr) = norm_f64(n.unattributed_fraction);

        *c19.add(nr) = n.threshold_margin.is_some();
        *c20.add(nr) = norm_f64(n.threshold_margin.unwrap_or(0.0));
        *c21.add(nr) = n.ambiguous_tie;
        *c22.add(nr) = n.high_uncertainty;
    }

    *offset += len;
}

unsafe fn write_evolve_results_to_output(
    results: &[EvolutionResult],
    output: duckdb_vector,
) -> Result<(), &'static str> {
    let count = results.len();
    duckdb_vector_ensure_validity_writable(output);

    let v_algo = duckdb_struct_vector_get_child(output, 0);
    let v_sec_rule = duckdb_struct_vector_get_child(output, 1);
    let v_flow_rule = duckdb_struct_vector_get_child(output, 2);
    let v_unc_rule = duckdb_struct_vector_get_child(output, 3);
    let v_disclaimer = duckdb_struct_vector_get_child(output, 4);
    let v_prev_ids = duckdb_struct_vector_get_child(output, 5);
    let v_curr_ids = duckdb_struct_vector_get_child(output, 6);
    let v_prev_disp = duckdb_struct_vector_get_child(output, 7);
    let v_curr_disp = duckdb_struct_vector_get_child(output, 8);
    let v_pair_scale = duckdb_struct_vector_get_child(output, 9);
    let v_thresh_coeff = duckdb_struct_vector_get_child(output, 10);
    let v_threshold = duckdb_struct_vector_get_child(output, 11);
    let v_has_ref_scale = duckdb_struct_vector_get_child(output, 12);
    let v_ref_scale = duckdb_struct_vector_get_child(output, 13);
    let v_edges = duckdb_struct_vector_get_child(output, 14);
    let v_prev_nodes = duckdb_struct_vector_get_child(output, 15);
    let v_curr_nodes = duckdb_struct_vector_get_child(output, 16);
    let v_events = duckdb_struct_vector_get_child(output, 17);
    let v_summary = duckdb_struct_vector_get_child(output, 18);

    // 1. Precalculate ALL list capacities before any reserve_list or get_data calls
    let mut total_prev_ids = 0usize;
    let mut total_curr_ids = 0usize;
    let mut total_edges = 0usize;
    let mut total_edge_reasons = 0usize;
    let mut total_prev_nodes = 0usize;
    let mut total_curr_nodes = 0usize;
    let mut total_events = 0usize;
    let mut total_ev_prev_ids = 0usize;
    let mut total_ev_curr_ids = 0usize;

    for res in results {
        total_prev_ids = total_prev_ids
            .checked_add(res.prev_cluster_ids.len())
            .ok_or("LIST size overflow")?;
        total_curr_ids = total_curr_ids
            .checked_add(res.curr_cluster_ids.len())
            .ok_or("LIST size overflow")?;
        total_edges = total_edges
            .checked_add(res.edges.len())
            .ok_or("LIST size overflow")?;
        for e in &res.edges {
            total_edge_reasons = total_edge_reasons
                .checked_add(e.uncertainty_reasons.len())
                .ok_or("LIST size overflow")?;
        }
        total_prev_nodes = total_prev_nodes
            .checked_add(res.prev_nodes.len())
            .ok_or("LIST size overflow")?;
        total_curr_nodes = total_curr_nodes
            .checked_add(res.curr_nodes.len())
            .ok_or("LIST size overflow")?;
        total_events = total_events
            .checked_add(res.events.len())
            .ok_or("LIST size overflow")?;
        for ev in &res.events {
            total_ev_prev_ids = total_ev_prev_ids
                .checked_add(ev.prev_cluster_ids.len())
                .ok_or("LIST size overflow")?;
            total_ev_curr_ids = total_ev_curr_ids
                .checked_add(ev.curr_cluster_ids.len())
                .ok_or("LIST size overflow")?;
        }
    }

    // 2. Reserve ALL outer and nested LIST vectors before taking any raw data pointers
    reserve_list(v_prev_ids, total_prev_ids)?;
    reserve_list(v_curr_ids, total_curr_ids)?;

    reserve_list(v_edges, total_edges)?;
    let edge_struct_v = duckdb_list_vector_get_child(v_edges);
    let v_edge_reasons = duckdb_struct_vector_get_child(edge_struct_v, 23);
    reserve_list(v_edge_reasons, total_edge_reasons)?;

    reserve_list(v_prev_nodes, total_prev_nodes)?;
    reserve_list(v_curr_nodes, total_curr_nodes)?;

    reserve_list(v_events, total_events)?;
    let ev_struct_v = duckdb_list_vector_get_child(v_events);
    let v_ev_prev_ids = duckdb_struct_vector_get_child(ev_struct_v, 2);
    let v_ev_curr_ids = duckdb_struct_vector_get_child(ev_struct_v, 3);
    reserve_list(v_ev_prev_ids, total_ev_prev_ids)?;
    reserve_list(v_ev_curr_ids, total_ev_curr_ids)?;

    // 3. Track offsets and write chunk rows
    let mut off_prev_ids = 0u64;
    let mut off_curr_ids = 0u64;
    let mut off_edges = 0u64;
    let mut off_edge_reasons = 0u64;
    let mut off_prev_nodes = 0u64;
    let mut off_curr_nodes = 0u64;
    let mut off_events = 0u64;
    let mut off_ev_prev_ids = 0u64;
    let mut off_ev_curr_ids = 0u64;

    let prev_ids_child = duckdb_list_vector_get_child(v_prev_ids);
    let curr_ids_child = duckdb_list_vector_get_child(v_curr_ids);
    let edge_reasons_child = duckdb_list_vector_get_child(v_edge_reasons);
    let ev_prev_ids_child = duckdb_list_vector_get_child(v_ev_prev_ids);
    let ev_curr_ids_child = duckdb_list_vector_get_child(v_ev_curr_ids);

    for row in 0..count {
        let res = &results[row];

        // 0..4: top-level strings
        assign_str(v_algo, row as u64, &res.algorithm_version);
        assign_str(v_sec_rule, row as u64, &res.secondary_rule);
        assign_str(v_flow_rule, row as u64, &res.flow_rule);
        assign_str(v_unc_rule, row as u64, &res.uncertainty_rule);
        assign_str(v_disclaimer, row as u64, &res.non_physical_disclaimer);

        // 5: prev_cluster_ids
        let p_len = res.prev_cluster_ids.len() as u64;
        let p_data = duckdb_vector_get_data(prev_ids_child) as *mut i64;
        for (i, &cid) in res.prev_cluster_ids.iter().enumerate() {
            *p_data.add((off_prev_ids as usize) + i) = cid as i64;
        }
        *(duckdb_vector_get_data(v_prev_ids) as *mut duckdb_list_entry).add(row) =
            duckdb_list_entry {
                offset: off_prev_ids,
                length: p_len,
            };
        off_prev_ids += p_len;

        // 6: curr_cluster_ids
        let c_len = res.curr_cluster_ids.len() as u64;
        let c_data = duckdb_vector_get_data(curr_ids_child) as *mut i64;
        for (j, &cid) in res.curr_cluster_ids.iter().enumerate() {
            *c_data.add((off_curr_ids as usize) + j) = cid as i64;
        }
        *(duckdb_vector_get_data(v_curr_ids) as *mut duckdb_list_entry).add(row) =
            duckdb_list_entry {
                offset: off_curr_ids,
                length: c_len,
            };
        off_curr_ids += c_len;

        // 7..13: scale & threshold scalars
        *(duckdb_vector_get_data(v_prev_disp) as *mut f64).add(row) =
            norm_f64(res.prev_dispersion.unwrap_or(0.0));
        *(duckdb_vector_get_data(v_curr_disp) as *mut f64).add(row) =
            norm_f64(res.curr_dispersion.unwrap_or(0.0));
        *(duckdb_vector_get_data(v_pair_scale) as *mut f64).add(row) =
            norm_f64(res.pair_scale.unwrap_or(0.0));
        *(duckdb_vector_get_data(v_thresh_coeff) as *mut f64).add(row) =
            norm_f64(res.threshold_coefficient.unwrap_or(0.0));
        *(duckdb_vector_get_data(v_threshold) as *mut f64).add(row) = norm_f64(res.threshold);
        *(duckdb_vector_get_data(v_has_ref_scale) as *mut bool).add(row) =
            res.reference_scale.is_some();
        *(duckdb_vector_get_data(v_ref_scale) as *mut f64).add(row) =
            norm_f64(res.reference_scale.unwrap_or(0.0));

        // 14: edges
        let e_start = off_edges;
        let e_len = res.edges.len() as u64;
        *(duckdb_vector_get_data(v_edges) as *mut duckdb_list_entry).add(row) = duckdb_list_entry {
            offset: e_start,
            length: e_len,
        };
        let e_c0 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(edge_struct_v, 0)) as *mut i64;
        let e_c1 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(edge_struct_v, 1)) as *mut i64;
        let e_v2 = duckdb_struct_vector_get_child(edge_struct_v, 2);
        let e_v3 = duckdb_struct_vector_get_child(edge_struct_v, 3);
        let e_v4 = duckdb_struct_vector_get_child(edge_struct_v, 4);
        let e_c5 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(edge_struct_v, 5)) as *mut f64;
        let e_c6 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(edge_struct_v, 6)) as *mut f64;
        let e_c7 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(edge_struct_v, 7)) as *mut f64;
        let e_c8 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(edge_struct_v, 8)) as *mut f64;
        let e_c9 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(edge_struct_v, 9)) as *mut f64;
        let e_c10 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(edge_struct_v, 10)) as *mut f64;
        let e_c11 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(edge_struct_v, 11)) as *mut f64;
        let e_c12 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(edge_struct_v, 12)) as *mut f64;
        let e_c13 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(edge_struct_v, 13)) as *mut f64;
        let e_v14 = duckdb_struct_vector_get_child(edge_struct_v, 14);
        let e_c15 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(edge_struct_v, 15)) as *mut bool;
        let e_c16 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(edge_struct_v, 16)) as *mut f64;
        let e_c17 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(edge_struct_v, 17)) as *mut f64;
        let e_c18 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(edge_struct_v, 18)) as *mut bool;
        let e_c19 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(edge_struct_v, 19)) as *mut f64;
        let e_c20 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(edge_struct_v, 20)) as *mut f64;
        let e_c21 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(edge_struct_v, 21)) as *mut bool;
        let e_c22 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(edge_struct_v, 22)) as *mut bool;
        let e_reasons_entries = duckdb_vector_get_data(v_edge_reasons) as *mut duckdb_list_entry;

        for (e_idx, e) in res.edges.iter().enumerate() {
            let er = (e_start as usize) + e_idx;
            *e_c0.add(er) = e.prev_cluster_id as i64;
            *e_c1.add(er) = e.curr_cluster_id as i64;
            assign_str(e_v2, er as u64, e.edge_role.as_str());
            assign_str(e_v3, er as u64, e.event_type.as_str());
            assign_str(e_v4, er as u64, &e.rule_basis);
            *e_c5.add(er) = norm_f64(e.distance);
            *e_c6.add(er) = norm_f64(e.distance_over_scale.unwrap_or(0.0));
            *e_c7.add(er) = norm_f64(e.prev_member_fraction);
            *e_c8.add(er) = norm_f64(e.curr_member_fraction);
            *e_c9.add(er) = norm_f64(e.edge_delta_fraction);
            *e_c10.add(er) = norm_f64(e.attributed_flow);
            *e_c11.add(er) = norm_f64(e.edge_strength);
            *e_c12.add(er) = norm_f64(e.threshold_margin);
            *e_c13.add(er) = norm_f64(e.threshold_margin_ratio.unwrap_or(0.0));
            assign_str(e_v14, er as u64, e.primary_alternative_status.as_str());
            *e_c15.add(er) = e.optimality_margin.is_some();
            *e_c16.add(er) = norm_f64(e.optimality_margin.unwrap_or(0.0));
            *e_c17.add(er) = norm_f64(e.optimality_margin_ratio.unwrap_or(0.0));
            *e_c18.add(er) = e.secondary_margin.is_some();
            *e_c19.add(er) = norm_f64(e.secondary_margin.unwrap_or(0.0));
            *e_c20.add(er) = norm_f64(e.secondary_margin_ratio.unwrap_or(0.0));
            *e_c21.add(er) = e.ambiguous_tie;
            *e_c22.add(er) = e.high_uncertainty;

            let r_start = off_edge_reasons;
            let r_len = e.uncertainty_reasons.len() as u64;
            for (r_i, reason) in e.uncertainty_reasons.iter().enumerate() {
                assign_str(edge_reasons_child, r_start + (r_i as u64), reason.as_str());
            }
            *e_reasons_entries.add(er) = duckdb_list_entry {
                offset: r_start,
                length: r_len,
            };
            off_edge_reasons += r_len;
        }
        off_edges += e_len;

        // 15 & 16: prev_nodes & curr_nodes
        write_node_summaries(v_prev_nodes, row, &res.prev_nodes, &mut off_prev_nodes);
        write_node_summaries(v_curr_nodes, row, &res.curr_nodes, &mut off_curr_nodes);

        // 17: events
        let ev_start = off_events;
        let ev_len = res.events.len() as u64;
        *(duckdb_vector_get_data(v_events) as *mut duckdb_list_entry).add(row) =
            duckdb_list_entry {
                offset: ev_start,
                length: ev_len,
            };
        let ev_c0 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(ev_struct_v, 0)) as *mut i64;
        let ev_v1 = duckdb_struct_vector_get_child(ev_struct_v, 1);
        let ev_p_entries = duckdb_vector_get_data(v_ev_prev_ids) as *mut duckdb_list_entry;
        let ev_p_data = duckdb_vector_get_data(ev_prev_ids_child) as *mut i64;
        let ev_c_entries = duckdb_vector_get_data(v_ev_curr_ids) as *mut duckdb_list_entry;
        let ev_c_data = duckdb_vector_get_data(ev_curr_ids_child) as *mut i64;
        let ev_c4 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(ev_struct_v, 4)) as *mut bool;
        let ev_c5 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(ev_struct_v, 5)) as *mut i64;
        let ev_c6 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(ev_struct_v, 6)) as *mut i64;
        let ev_v7 = duckdb_struct_vector_get_child(ev_struct_v, 7);
        let ev_c8 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(ev_struct_v, 8)) as *mut f64;
        let ev_c9 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(ev_struct_v, 9)) as *mut f64;
        let ev_c10 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(ev_struct_v, 10)) as *mut f64;
        let ev_c11 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(ev_struct_v, 11)) as *mut f64;
        let ev_c12 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(ev_struct_v, 12)) as *mut f64;
        let ev_c13 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(ev_struct_v, 13)) as *mut f64;
        let ev_c14 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(ev_struct_v, 14)) as *mut bool;
        let ev_c15 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(ev_struct_v, 15)) as *mut f64;
        let ev_c16 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(ev_struct_v, 16)) as *mut bool;
        let ev_c17 =
            duckdb_vector_get_data(duckdb_struct_vector_get_child(ev_struct_v, 17)) as *mut bool;

        for (ev_i, ev) in res.events.iter().enumerate() {
            let evr = (ev_start as usize) + ev_i;
            *ev_c0.add(evr) = ev.event_index as i64;
            assign_str(ev_v1, evr as u64, ev.event_type.as_str());

            let ep_start = off_ev_prev_ids;
            let ep_len = ev.prev_cluster_ids.len() as u64;
            for (k, &cid) in ev.prev_cluster_ids.iter().enumerate() {
                *ev_p_data.add((ep_start as usize) + k) = cid as i64;
            }
            *ev_p_entries.add(evr) = duckdb_list_entry {
                offset: ep_start,
                length: ep_len,
            };
            off_ev_prev_ids += ep_len;

            let ec_start = off_ev_curr_ids;
            let ec_len = ev.curr_cluster_ids.len() as u64;
            for (k, &cid) in ev.curr_cluster_ids.iter().enumerate() {
                *ev_c_data.add((ec_start as usize) + k) = cid as i64;
            }
            *ev_c_entries.add(evr) = duckdb_list_entry {
                offset: ec_start,
                length: ec_len,
            };
            off_ev_curr_ids += ec_len;

            *ev_c4.add(evr) = ev.primary_link.is_some();
            *ev_c5.add(evr) = ev.primary_link.map(|(p, _)| p as i64).unwrap_or(-1);
            *ev_c6.add(evr) = ev.primary_link.map(|(_, c)| c as i64).unwrap_or(-1);
            assign_str(ev_v7, evr as u64, &ev.rule_basis);
            *ev_c8.add(evr) = norm_f64(ev.max_morphology_distance);
            *ev_c9.add(evr) = norm_f64(ev.max_morphology_over_scale.unwrap_or(0.0));
            *ev_c10.add(evr) = norm_f64(ev.prev_fraction_sum);
            *ev_c11.add(evr) = norm_f64(ev.curr_fraction_sum);
            *ev_c12.add(evr) = norm_f64(ev.net_delta_fraction);
            *ev_c13.add(evr) = norm_f64(ev.attributed_flow_sum);
            *ev_c14.add(evr) = ev.min_margin_ratio.is_some();
            *ev_c15.add(evr) = norm_f64(ev.min_margin_ratio.unwrap_or(0.0));
            *ev_c16.add(evr) = ev.ambiguous_tie;
            *ev_c17.add(evr) = ev.high_uncertainty;
        }
        off_events += ev_len;

        // 18: summary (31 scalar fields)
        let s = &res.summary;
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 0)) as *mut i64)
            .add(row) = s.primary_edge_count as i64;
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 1)) as *mut i64)
            .add(row) = s.secondary_split_edge_count as i64;
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 2)) as *mut i64)
            .add(row) = s.secondary_merge_edge_count as i64;
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 3)) as *mut i64)
            .add(row) = s.continuation_count as i64;
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 4)) as *mut i64)
            .add(row) = s.split_count as i64;
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 5)) as *mut i64)
            .add(row) = s.merge_count as i64;
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 6)) as *mut i64)
            .add(row) = s.emergence_count as i64;
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 7)) as *mut i64)
            .add(row) = s.disappearance_count as i64;
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 8)) as *mut f64)
            .add(row) = norm_f64(s.primary_total_distance);
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 9)) as *mut f64)
            .add(row) = norm_f64(s.primary_max_distance);
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 10)) as *mut f64)
            .add(row) = norm_f64(s.primary_max_distance_over_scale.unwrap_or(0.0));
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 11)) as *mut f64)
            .add(row) = norm_f64(s.all_edges_max_distance);
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 12)) as *mut f64)
            .add(row) = norm_f64(s.all_edges_max_distance_over_scale.unwrap_or(0.0));
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 13)) as *mut f64)
            .add(row) = norm_f64(s.transition_tv_redistribution);
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 14)) as *mut f64)
            .add(row) = norm_f64(s.secondary_attributed_flow);
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 15)) as *mut f64)
            .add(row) = norm_f64(s.total_attributed_flow);
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 16)) as *mut f64)
            .add(row) = norm_f64(s.disappeared_fraction_sum);
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 17)) as *mut f64)
            .add(row) = norm_f64(s.emerged_fraction_sum);
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 18)) as *mut f64)
            .add(row) = norm_f64(s.component_tv_residual);
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 19)) as *mut f64)
            .add(row) = norm_f64(s.min_threshold_margin);
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 20)) as *mut f64)
            .add(row) = norm_f64(s.min_threshold_margin_ratio.unwrap_or(0.0));
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 21)) as *mut f64)
            .add(row) = norm_f64(s.raw_matrix_threshold_margin);
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 22)) as *mut bool)
            .add(row) = s.min_optimality_margin.is_some();
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 23)) as *mut f64)
            .add(row) = norm_f64(s.min_optimality_margin.unwrap_or(0.0));
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 24)) as *mut f64)
            .add(row) = norm_f64(s.min_optimality_margin_ratio.unwrap_or(0.0));
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 25)) as *mut bool)
            .add(row) = s.min_secondary_margin.is_some();
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 26)) as *mut f64)
            .add(row) = norm_f64(s.min_secondary_margin.unwrap_or(0.0));
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 27)) as *mut f64)
            .add(row) = norm_f64(s.min_secondary_margin_ratio.unwrap_or(0.0));
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 28)) as *mut bool)
            .add(row) = s.ambiguous_tie;
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 29)) as *mut i64)
            .add(row) = s.high_uncertainty_edge_count as i64;
        *(duckdb_vector_get_data(duckdb_struct_vector_get_child(v_summary, 30)) as *mut bool)
            .add(row) = s.high_uncertainty;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Scalar Function Callbacks
// ---------------------------------------------------------------------------

unsafe extern "C" fn fsum_callback(
    info: duckdb_function_info,
    chunk: duckdb_data_chunk,
    output: duckdb_vector,
) {
    let outcome = catch_unwind(AssertUnwindSafe(|| -> Result<(), SqlMatchError> {
        let count = duckdb_data_chunk_get_size(chunk);
        if count == 0 {
            return Ok(());
        }
        let vals_vec = duckdb_data_chunk_get_vector(chunk, 0);
        if vals_vec.is_null() {
            return Err(SqlMatchError::new(
                FSUM_FUNCTION_NAME,
                "NULL_INPUT",
                0,
                "values argument vector is NULL",
            ));
        }

        let top_validity = duckdb_vector_get_validity(vals_vec);
        let child_vec = duckdb_list_vector_get_child(vals_vec);
        let child_validity = duckdb_vector_get_validity(child_vec);
        let child_data = duckdb_vector_get_data(child_vec) as *const f64;

        let mut row_sums = Vec::with_capacity(count as usize);
        for row in 0..count {
            if !row_is_valid(top_validity, row) {
                return Err(SqlMatchError::new(
                    FSUM_FUNCTION_NAME,
                    "NULL_INPUT",
                    row,
                    "values list is NULL",
                ));
            }
            let entry = checked_list_entry(FSUM_FUNCTION_NAME, vals_vec, row, row)?;
            if entry.length == 0 {
                row_sums.push(0.0);
                continue;
            }
            let mut exp = Expansion::new();
            for i in 0..entry.length {
                let idx = entry.offset + i;
                if !row_is_valid(child_validity, idx) {
                    return Err(SqlMatchError::new(
                        FSUM_FUNCTION_NAME,
                        "NULL_INPUT",
                        row,
                        format!("element at index {i} is NULL"),
                    ));
                }
                let val = *child_data.add(idx as usize);
                if !val.is_finite() {
                    return Err(SqlMatchError::new(
                        FSUM_FUNCTION_NAME,
                        "NON_FINITE_INPUT",
                        row,
                        format!("non-finite element at index {i}: {val}"),
                    ));
                }
                exp.push(val).map_err(|_| {
                    SqlMatchError::new(
                        FSUM_FUNCTION_NAME,
                        "NUMERICAL_FAILURE",
                        row,
                        "intermediate expansion overflow in ens_fsum",
                    )
                })?;
            }
            let rounded = norm_f64(exp.round());
            if !rounded.is_finite() {
                return Err(SqlMatchError::new(
                    FSUM_FUNCTION_NAME,
                    "NUMERICAL_FAILURE",
                    row,
                    "rounded fsum overflowed to non-finite value",
                ));
            }
            row_sums.push(rounded);
        }

        duckdb_vector_ensure_validity_writable(output);
        let out_data = duckdb_vector_get_data(output) as *mut f64;
        for (row, val) in row_sums.into_iter().enumerate() {
            *out_data.add(row) = val;
        }

        Ok(())
    }));

    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(sql_err)) => {
            let c_str = sql_err.to_c_string();
            set_error_raw(info, &c_str);
        }
        Err(_) => {
            let c_str =
                CString::new("ens_fsum [PANIC] chunk_row=0: unexpected panic in callback").unwrap();
            set_error_raw(info, &c_str);
        }
    }
}

unsafe extern "C" fn evolve_scenarios_callback(
    info: duckdb_function_info,
    chunk: duckdb_data_chunk,
    output: duckdb_vector,
) {
    let outcome = catch_unwind(AssertUnwindSafe(|| -> Result<(), SqlMatchError> {
        let count = duckdb_data_chunk_get_size(chunk);
        if count == 0 {
            return Ok(());
        }
        let prev_vec = duckdb_data_chunk_get_vector(chunk, 0);
        let curr_vec = duckdb_data_chunk_get_vector(chunk, 1);
        let weights_vec = duckdb_data_chunk_get_vector(chunk, 2);
        let backbone_vec = duckdb_data_chunk_get_vector(chunk, 3);
        let config_vec = duckdb_data_chunk_get_vector(chunk, 4);

        if prev_vec.is_null()
            || curr_vec.is_null()
            || weights_vec.is_null()
            || backbone_vec.is_null()
            || config_vec.is_null()
        {
            return Err(SqlMatchError::new(
                EVOLVE_SCENARIOS_FUNCTION_NAME,
                "NULL_INPUT",
                0,
                "input argument vector is NULL",
            ));
        }

        let mut chunk_results = Vec::with_capacity(count as usize);
        for row in 0..count {
            let res = extract_and_run_evolve_row(
                prev_vec,
                curr_vec,
                weights_vec,
                backbone_vec,
                config_vec,
                row,
            )?;
            chunk_results.push(res);
        }

        write_evolve_results_to_output(&chunk_results, output).map_err(|msg| {
            SqlMatchError::new(EVOLVE_SCENARIOS_FUNCTION_NAME, "WRITE_FAILURE", 0, msg)
        })?;

        Ok(())
    }));

    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(sql_err)) => {
            let c_str = sql_err.to_c_string();
            set_error_raw(info, &c_str);
        }
        Err(_) => {
            let c_str = CString::new(
                "ens_evolve_scenarios [PANIC] chunk_row=0: unexpected panic in callback",
            )
            .unwrap();
            set_error_raw(info, &c_str);
        }
    }
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

pub unsafe fn register_evolve_functions(connection: duckdb_connection) -> Result<(), String> {
    unsafe {
        // 1. ens_fsum(DOUBLE[]) -> DOUBLE
        let fsum_name = CString::new(FSUM_FUNCTION_NAME).unwrap();
        let mut fsum_fn = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(fsum_fn, fsum_name.as_ptr());

        let mut vals_type = build_double_list_type();
        let mut ret_double = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE);

        duckdb_scalar_function_add_parameter(fsum_fn, vals_type);
        duckdb_scalar_function_set_return_type(fsum_fn, ret_double);

        duckdb_destroy_logical_type(&mut vals_type);
        duckdb_destroy_logical_type(&mut ret_double);

        duckdb_scalar_function_set_special_handling(fsum_fn);
        duckdb_scalar_function_set_function(fsum_fn, Some(fsum_callback));

        let state1 = duckdb_register_scalar_function(connection, fsum_fn);
        duckdb_destroy_scalar_function(&mut fsum_fn);
        if state1 != DuckDBSuccess {
            return Err(format!("failed to register {FSUM_FUNCTION_NAME}"));
        }

        // 2. ens_evolve_scenarios(...) -> STRUCT(...)
        let evolve_name = CString::new(EVOLVE_SCENARIOS_FUNCTION_NAME).unwrap();
        let mut evolve_fn = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(evolve_fn, evolve_name.as_ptr());

        let mut prev_type = build_evolve_scenarios_input_type();
        let mut curr_type = build_evolve_scenarios_input_type();
        let mut weights_type = build_double_list_type();
        let mut backbone_type = build_match_return_type();
        let mut cfg_type = build_evolve_config_type();
        let mut ret_evolve_type = build_evolve_return_type();

        duckdb_scalar_function_add_parameter(evolve_fn, prev_type);
        duckdb_scalar_function_add_parameter(evolve_fn, curr_type);
        duckdb_scalar_function_add_parameter(evolve_fn, weights_type);
        duckdb_scalar_function_add_parameter(evolve_fn, backbone_type);
        duckdb_scalar_function_add_parameter(evolve_fn, cfg_type);
        duckdb_scalar_function_set_return_type(evolve_fn, ret_evolve_type);

        duckdb_destroy_logical_type(&mut prev_type);
        duckdb_destroy_logical_type(&mut curr_type);
        duckdb_destroy_logical_type(&mut weights_type);
        duckdb_destroy_logical_type(&mut backbone_type);
        duckdb_destroy_logical_type(&mut cfg_type);
        duckdb_destroy_logical_type(&mut ret_evolve_type);

        duckdb_scalar_function_set_special_handling(evolve_fn);
        duckdb_scalar_function_set_function(evolve_fn, Some(evolve_scenarios_callback));

        let state2 = duckdb_register_scalar_function(connection, evolve_fn);
        duckdb_destroy_scalar_function(&mut evolve_fn);
        if state2 != DuckDBSuccess {
            return Err(format!(
                "failed to register {EVOLVE_SCENARIOS_FUNCTION_NAME}"
            ));
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Single-Batch Whitelist Diagnosis (`ens_diagnose_batch`)
// ---------------------------------------------------------------------------

fn validate_diagnose_config(task: &TaskInput, row: u64) -> Result<(), SqlMatchError> {
    let func = DIAGNOSE_BATCH_FUNCTION_NAME;
    let cfg = &task.config;
    let n = task.members.len();

    if cfg.cluster_count < 2 {
        return Err(SqlMatchError::new(
            func,
            "INVALID_CONFIG",
            row,
            format!("cluster_count must be >= 2, got {}", cfg.cluster_count),
        ));
    }
    if cfg.projection_dim < 1 {
        return Err(SqlMatchError::new(
            func,
            "INVALID_CONFIG",
            row,
            format!("projection_dim must be >= 1, got {}", cfg.projection_dim),
        ));
    }
    if cfg.tolerance < 0.0 || !cfg.tolerance.is_finite() {
        return Err(SqlMatchError::new(
            func,
            "INVALID_CONFIG",
            row,
            format!("tolerance must be finite and >= 0, got {}", cfg.tolerance),
        ));
    }
    if cfg.max_iterations == 0 {
        return Err(SqlMatchError::new(
            func,
            "INVALID_CONFIG",
            row,
            "max_iterations must be > 0",
        ));
    }
    if cfg.rank_threshold <= 0.0 || !cfg.rank_threshold.is_finite() {
        return Err(SqlMatchError::new(
            func,
            "INVALID_CONFIG",
            row,
            format!(
                "rank_threshold must be finite and > 0, got {}",
                cfg.rank_threshold
            ),
        ));
    }
    if cfg.restarts == 0 {
        return Err(SqlMatchError::new(
            func,
            "INVALID_CONFIG",
            row,
            "restarts must be > 0",
        ));
    }
    if cfg.cluster_count > n {
        return Err(SqlMatchError::new(
            func,
            "INVALID_CONFIG",
            row,
            format!(
                "cluster_count ({}) cannot exceed member count ({n})",
                cfg.cluster_count
            ),
        ));
    }

    match cfg.initialization {
        InitMethod::ExplicitCentersV1 => {
            if cfg.seed != 0 {
                return Err(SqlMatchError::new(
                    func,
                    "INVALID_CONFIG",
                    row,
                    format!("seed must be 0 for explicit_centers_v1, got {}", cfg.seed),
                ));
            }
            if cfg.initial_centers.len() != cfg.restarts {
                return Err(SqlMatchError::new(
                    func,
                    "INVALID_INITIAL_CENTERS",
                    row,
                    format!(
                        "explicit initial_centers must have {} sets for {} restarts",
                        cfg.restarts, cfg.restarts
                    ),
                ));
            }
            for (r_idx, r_centers) in cfg.initial_centers.iter().enumerate() {
                if r_centers.len() != cfg.cluster_count {
                    return Err(SqlMatchError::new(
                        func,
                        "INVALID_INITIAL_CENTERS",
                        row,
                        format!(
                            "restart {r_idx} centers count {} != cluster_count {}",
                            r_centers.len(),
                            cfg.cluster_count
                        ),
                    ));
                }
                for (c_idx, center) in r_centers.iter().enumerate() {
                    if center.len() != cfg.projection_dim {
                        return Err(SqlMatchError::new(
                            func,
                            "INVALID_INITIAL_CENTERS",
                            row,
                            format!(
                                "center {c_idx} in restart {r_idx} dimension {} != projection_dim {}",
                                center.len(),
                                cfg.projection_dim
                            ),
                        ));
                    }
                    for &coord in center {
                        if !coord.is_finite() {
                            return Err(SqlMatchError::new(
                                func,
                                "INVALID_INITIAL_CENTERS",
                                row,
                                "non-finite coordinate in explicit initial center",
                            ));
                        }
                    }
                }
            }
        }
        InitMethod::KMeansPlusPlusChaCha8V1 => {
            if !cfg.initial_centers.is_empty() {
                return Err(SqlMatchError::new(
                    func,
                    "INVALID_CONFIG",
                    row,
                    "initial_centers must be empty when initialization is kmeans++_chacha8_v1",
                ));
            }
        }
    }

    Ok(())
}

pub(crate) fn diagnose_batch_task(
    task: &TaskInput,
    weights: &[f64],
    row: u64,
) -> Result<String, SqlMatchError> {
    let func = DIAGNOSE_BATCH_FUNCTION_NAME;
    if task.members.is_empty() {
        return Err(SqlMatchError::new(
            func,
            "EMPTY_MEMBERS",
            row,
            "members list is empty",
        ));
    }
    validate_diagnose_config(task, row)?;

    if weights.is_empty() {
        return Err(SqlMatchError::new(
            func,
            "INVALID_WEIGHTS",
            row,
            "weights list is empty",
        ));
    }
    if weights.len() != task.members[0].features.len() {
        return Err(SqlMatchError::new(
            func,
            "DIMENSION_MISMATCH",
            row,
            format!(
                "weights length {} does not match member features length {}",
                weights.len(),
                task.members[0].features.len()
            ),
        ));
    }

    match ensemble_dispersion(&task.members, weights) {
        Ok(_) => {}
        Err(e) if e.code == CoreErrorCode::ZeroDispersion => {
            return Ok("ZERO_DISPERSION".to_string());
        }
        Err(e) => {
            return Err(SqlMatchError::from_core(func, e, row));
        }
    }

    match cluster(task) {
        Ok(_) => Ok("OK".to_string()),
        Err(e)
            if matches!(
                e.code,
                CoreErrorCode::InsufficientRank
                    | CoreErrorCode::AmbiguousSubspace
                    | CoreErrorCode::EmptyCluster
                    | CoreErrorCode::NotConverged
                    | CoreErrorCode::InsufficientDistinctVectors
                    | CoreErrorCode::InsufficientProjectedVectors
                    | CoreErrorCode::SvdNotConverged
            ) =>
        {
            Ok(e.code.as_str().to_string())
        }
        Err(e) => Err(SqlMatchError::from_core(func, e, row)),
    }
}

unsafe extern "C" fn diagnose_batch_callback(
    info: duckdb_function_info,
    chunk: duckdb_data_chunk,
    output: duckdb_vector,
) {
    let outcome = catch_unwind(AssertUnwindSafe(|| -> Result<(), SqlMatchError> {
        let count = duckdb_data_chunk_get_size(chunk);
        if count == 0 {
            return Ok(());
        }
        let members_vec = duckdb_data_chunk_get_vector(chunk, 0);
        let weights_vec = duckdb_data_chunk_get_vector(chunk, 1);
        let config_vec = duckdb_data_chunk_get_vector(chunk, 2);

        if members_vec.is_null() || weights_vec.is_null() || config_vec.is_null() {
            return Err(SqlMatchError::new(
                DIAGNOSE_BATCH_FUNCTION_NAME,
                "NULL_INPUT",
                0,
                "input argument vector is NULL",
            ));
        }

        let func = DIAGNOSE_BATCH_FUNCTION_NAME;
        let w_child = duckdb_list_vector_get_child(weights_vec);
        let w_validity = duckdb_vector_get_validity(w_child);
        let w_data = duckdb_vector_get_data(w_child) as *const f64;

        let mut diagnoses = Vec::with_capacity(count as usize);
        for row in 0..count {
            let task = crate::extract_task(members_vec, config_vec, row)
                .map_err(|err| SqlMatchError::new(func, err.code, row, err.message))?;

            if !row_is_valid(duckdb_vector_get_validity(weights_vec), row) {
                return Err(SqlMatchError::new(
                    func,
                    "NULL_INPUT",
                    row,
                    "weights list is NULL",
                ));
            }
            let w_entry = checked_list_entry(func, weights_vec, row, row)?;
            if w_entry.length == 0 {
                return Err(SqlMatchError::new(
                    func,
                    "INVALID_WEIGHTS",
                    row,
                    "weights list is empty",
                ));
            }

            let mut weights = Vec::with_capacity(w_entry.length as usize);
            for idx in 0..w_entry.length {
                let r = w_entry.offset + idx;
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

            let diag = diagnose_batch_task(&task, &weights, row)?;
            diagnoses.push(diag);
        }

        duckdb_vector_ensure_validity_writable(output);
        for (row, diag) in diagnoses.iter().enumerate() {
            assign_str(output, row as u64, diag);
        }

        Ok(())
    }));

    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(sql_err)) => {
            let c_str = sql_err.to_c_string();
            set_error_raw(info, &c_str);
        }
        Err(_) => {
            let c_str = CString::new(
                "ens_diagnose_batch [PANIC] chunk_row=0: unexpected panic in callback",
            )
            .unwrap();
            set_error_raw(info, &c_str);
        }
    }
}

pub unsafe fn register_diagnose_function(connection: duckdb_connection) -> Result<(), String> {
    unsafe {
        let diag_name = CString::new(DIAGNOSE_BATCH_FUNCTION_NAME).unwrap();
        let mut diag_fn = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(diag_fn, diag_name.as_ptr());

        let mut members_type = crate::build_members_type();
        let mut weights_type = build_double_list_type();
        let mut cfg_type = crate::build_config_type();
        let mut ret_varchar = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR);

        duckdb_scalar_function_add_parameter(diag_fn, members_type);
        duckdb_scalar_function_add_parameter(diag_fn, weights_type);
        duckdb_scalar_function_add_parameter(diag_fn, cfg_type);
        duckdb_scalar_function_set_return_type(diag_fn, ret_varchar);

        duckdb_destroy_logical_type(&mut members_type);
        duckdb_destroy_logical_type(&mut weights_type);
        duckdb_destroy_logical_type(&mut cfg_type);
        duckdb_destroy_logical_type(&mut ret_varchar);

        duckdb_scalar_function_set_special_handling(diag_fn);
        duckdb_scalar_function_set_function(diag_fn, Some(diagnose_batch_callback));

        let state = duckdb_register_scalar_function(connection, diag_fn);
        duckdb_destroy_scalar_function(&mut diag_fn);
        if state != DuckDBSuccess {
            return Err(format!("failed to register {DIAGNOSE_BATCH_FUNCTION_NAME}"));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ensabc_core::{MemberInput, ScenarioConfig, StopRule};

    fn make_sc(cid: usize, count: usize, frac: f64, feats: &[f64]) -> ScenarioNodeInput {
        ScenarioNodeInput {
            cluster_id: cid,
            mean_features: feats.to_vec(),
            member_count: count,
            member_fraction: frac,
        }
    }

    fn build_valid_meta_from_result(res: &EvolutionResult) -> DecodedBackboneMeta {
        let m = res.prev_cluster_ids.len();
        let n = res.curr_cluster_ids.len();
        let scale = norm_f64(res.pair_scale.unwrap());
        let tau = res.threshold;

        let mut distance_ratio_matrix = Vec::with_capacity(m);
        let mut eligible_matrix = Vec::with_capacity(m);
        let mut threshold_margin = f64::INFINITY;
        for i in 0..m {
            let mut dr_row = Vec::with_capacity(n);
            let mut el_row = Vec::with_capacity(n);
            for j in 0..n {
                let d = res.backbone.distance_matrix[i][j];
                dr_row.push(d / scale);
                el_row.push(d <= tau);
                let margin = (d - tau).abs();
                if margin < threshold_margin {
                    threshold_margin = margin;
                }
            }
            distance_ratio_matrix.push(dr_row);
            eligible_matrix.push(el_row);
        }

        let mut optimality_margin = f64::INFINITY;
        let mut has_optimality_margin = false;
        let mut links = Vec::with_capacity(res.backbone.links.len());
        let mut primary_links = Vec::with_capacity(res.backbone.links.len());

        for l in &res.backbone.links {
            primary_links.push((l.prev_cluster_id, l.curr_cluster_id));
            let (status, alt_cnt, alt_tot, alt_inc, alt_inc_r, has_alt) = match &l.substitutability
            {
                Substitutability::Tied => {
                    has_optimality_margin = true;
                    optimality_margin = 0.0;
                    (
                        "tied".to_string(),
                        res.backbone.objective.matched_count as i64,
                        res.backbone.objective.weighted_distance_sum,
                        0.0,
                        0.0,
                        true,
                    )
                }
                Substitutability::SameCount { gap, alt_objective } => {
                    has_optimality_margin = true;
                    if *gap < optimality_margin {
                        optimality_margin = *gap;
                    }
                    (
                        "same_count".to_string(),
                        res.backbone.objective.matched_count as i64,
                        *alt_objective,
                        *gap,
                        *gap / scale,
                        true,
                    )
                }
                Substitutability::CountDrop {
                    alt_count,
                    alt_objective,
                } => (
                    "count_drop".to_string(),
                    *alt_count as i64,
                    *alt_objective,
                    0.0,
                    0.0,
                    false,
                ),
            };
            links.push(DecodedBackboneLink {
                prev_cluster_id: l.prev_cluster_id,
                curr_cluster_id: l.curr_cluster_id,
                distance: l.distance_k,
                distance_ratio: l.distance_ratio.unwrap_or(l.distance_k / scale),
                alternative_status: status,
                alternative_edge_count: alt_cnt,
                alternative_total_distance: alt_tot,
                alternative_distance_increase: alt_inc,
                alternative_distance_increase_ratio: alt_inc_r,
                has_same_count_alternative: has_alt,
                ambiguous: l.ambiguous,
            });
        }
        if !has_optimality_margin || !optimality_margin.is_finite() {
            optimality_margin = 0.0;
        }

        let unmatched_prev = res
            .backbone
            .unmatched_prev
            .iter()
            .map(|u| {
                let p_idx = res
                    .prev_cluster_ids
                    .iter()
                    .position(|&cid| cid == u.cluster_id)
                    .unwrap();
                let min_d = (0..n)
                    .map(|j| res.backbone.distance_matrix[p_idx][j])
                    .fold(f64::INFINITY, f64::min);
                DecodedUnmatched {
                    cluster_id: u.cluster_id as i64,
                    reason: u.reason.clone(),
                    min_candidate_distance: min_d,
                    matched_in_some_optimum: u.matched_in_some_optimum,
                }
            })
            .collect();

        let unmatched_curr = res
            .backbone
            .unmatched_curr
            .iter()
            .map(|u| {
                let c_idx = res
                    .curr_cluster_ids
                    .iter()
                    .position(|&cid| cid == u.cluster_id)
                    .unwrap();
                let min_d = (0..m)
                    .map(|i| res.backbone.distance_matrix[i][c_idx])
                    .fold(f64::INFINITY, f64::min);
                DecodedUnmatched {
                    cluster_id: u.cluster_id as i64,
                    reason: u.reason.clone(),
                    min_candidate_distance: min_d,
                    matched_in_some_optimum: u.matched_in_some_optimum,
                }
            })
            .collect();

        DecodedBackboneMeta {
            algorithm_version: MATCH_ALGORITHM_VERSION.to_string(),
            prev_dispersion: res.prev_dispersion.unwrap(),
            curr_dispersion: res.curr_dispersion.unwrap(),
            pair_scale: scale,
            threshold_coefficient: res.threshold_coefficient.unwrap(),
            distance_ratio_matrix,
            edge_count: res.backbone.objective.matched_count as i64,
            total_distance: res.backbone.objective.weighted_distance_sum,
            optimal_solution_count: res.backbone.optima_count as i64,
            ambiguous: res.backbone.links.iter().any(|l| l.ambiguous)
                || res.backbone.optima_count > 1,
            threshold_margin,
            optimality_margin,
            has_optimality_margin,
            links,
            unmatched_prev,
            unmatched_curr,
            assertion: BackboneAssertion {
                prev_cluster_ids: res.prev_cluster_ids.clone(),
                curr_cluster_ids: res.curr_cluster_ids.clone(),
                threshold: tau,
                distance_matrix: Some(res.backbone.distance_matrix.clone()),
                eligible_matrix: Some(eligible_matrix),
                primary_links,
            },
        }
    }

    fn unwrap_sql<T>(res: Result<T, SqlMatchError>) -> T {
        match res {
            Ok(v) => v,
            Err(e) => panic!(
                "unexpected SqlMatchError: {}",
                e.to_c_string().to_string_lossy()
            ),
        }
    }

    fn assert_inconsistent(meta: &DecodedBackboneMeta, res: &EvolutionResult, label: &str) {
        let err = verify_extra_backbone_meta(EVOLVE_SCENARIOS_FUNCTION_NAME, 0, meta, res)
            .expect_err(&format!("{label} must fail verification"));
        let text = err.to_c_string().to_string_lossy().into_owned();
        assert!(
            text.contains("[BACKBONE_INCONSISTENCY]"),
            "{label} expected BACKBONE_INCONSISTENCY, got: {text}"
        );
    }

    #[test]
    fn h1_verify_all_21_backbone_fields_links_and_unmatched() {
        // Composite 3x3 case with 2 primary links (including same_count alternative) + 1 unmatched_prev + 1 unmatched_curr
        let res = evolve_scenarios_scaled(&ScaledEvolutionInput {
            prev_scenarios: vec![
                make_sc(0, 20, 0.40, &[0.0]),
                make_sc(1, 20, 0.40, &[0.6]),
                make_sc(2, 10, 0.20, &[5.0]),
            ],
            curr_scenarios: vec![
                make_sc(0, 20, 0.40, &[0.1]),
                make_sc(1, 20, 0.40, &[0.8]),
                make_sc(2, 10, 0.20, &[9.0]),
            ],
            weights: vec![1.0],
            threshold_coefficient: 1.5,
            prev_dispersion: 0.8,
            curr_dispersion: 1.2,
            config: EvolutionConfig::default(),
            backbone: None,
        })
        .unwrap();

        let base_meta = build_valid_meta_from_result(&res);
        unwrap_sql(verify_extra_backbone_meta(
            EVOLVE_SCENARIOS_FUNCTION_NAME,
            0,
            &base_meta,
            &res,
        ));

        // Top-level field tampers
        let mut m = base_meta.clone();
        m.distance_ratio_matrix[0][0] += 1e-12;
        assert_inconsistent(&m, &res, "distance_ratio_matrix[0][0] delta");

        let mut m = base_meta.clone();
        m.distance_ratio_matrix[0][0] = f64::NAN;
        assert_inconsistent(&m, &res, "distance_ratio_matrix[0][0] NaN");

        let mut m = base_meta.clone();
        m.ambiguous = !m.ambiguous;
        assert_inconsistent(&m, &res, "ambiguous flip");

        let mut m = base_meta.clone();
        m.threshold_margin += 1e-9;
        assert_inconsistent(&m, &res, "threshold_margin tamper");

        let mut m = base_meta.clone();
        m.optimality_margin += 1e-9;
        assert_inconsistent(&m, &res, "optimality_margin tamper");

        let mut m = base_meta.clone();
        m.has_optimality_margin = !m.has_optimality_margin;
        assert_inconsistent(&m, &res, "has_optimality_margin flip");

        // Reversed links order
        assert!(base_meta.links.len() >= 2);
        let mut m = base_meta.clone();
        m.links.reverse();
        assert_inconsistent(&m, &res, "reversed links order");

        // Every nested field of links[0]
        let mut m = base_meta.clone();
        m.links[0].distance += 1e-12;
        assert_inconsistent(&m, &res, "links[0].distance");

        let mut m = base_meta.clone();
        m.links[0].distance_ratio += 1e-12;
        assert_inconsistent(&m, &res, "links[0].distance_ratio");

        let mut m = base_meta.clone();
        m.links[0].alternative_status = "tied".to_string();
        assert_inconsistent(&m, &res, "links[0].alternative_status");

        let mut m = base_meta.clone();
        m.links[0].alternative_edge_count += 1;
        assert_inconsistent(&m, &res, "links[0].alternative_edge_count");

        let mut m = base_meta.clone();
        m.links[0].alternative_total_distance += 1e-12;
        assert_inconsistent(&m, &res, "links[0].alternative_total_distance");

        let mut m = base_meta.clone();
        m.links[0].alternative_distance_increase += 1e-12;
        assert_inconsistent(&m, &res, "links[0].alternative_distance_increase");

        let mut m = base_meta.clone();
        m.links[0].alternative_distance_increase_ratio += 1e-12;
        assert_inconsistent(&m, &res, "links[0].alternative_distance_increase_ratio");

        let mut m = base_meta.clone();
        m.links[0].has_same_count_alternative = !m.links[0].has_same_count_alternative;
        assert_inconsistent(&m, &res, "links[0].has_same_count_alternative");

        let mut m = base_meta.clone();
        m.links[0].ambiguous = !m.links[0].ambiguous;
        assert_inconsistent(&m, &res, "links[0].ambiguous");

        // Unmatched min_candidate_distance tampers (prev & curr)
        assert!(!base_meta.unmatched_prev.is_empty());
        assert!(!base_meta.unmatched_curr.is_empty());

        let mut m = base_meta.clone();
        m.unmatched_prev[0].min_candidate_distance += 1e-9;
        assert_inconsistent(&m, &res, "unmatched_prev[0].min_candidate_distance");

        let mut m = base_meta.clone();
        m.unmatched_prev[0].min_candidate_distance = f64::INFINITY;
        assert_inconsistent(&m, &res, "unmatched_prev[0].min_candidate_distance inf");

        let mut m = base_meta;
        m.unmatched_curr[0].min_candidate_distance += 1e-9;
        assert_inconsistent(&m, &res, "unmatched_curr[0].min_candidate_distance");
    }

    #[test]
    fn m2_validate_positive_member_count_rejects_zero_and_negative() {
        assert_eq!(
            unwrap_sql(validate_positive_member_count(
                EVOLVE_SCENARIOS_FUNCTION_NAME,
                0,
                "prev_scenarios",
                "prev_index",
                1,
                0,
                25
            )),
            25
        );
        for bad in [0_i64, -1_i64, -99_i64] {
            let err = validate_positive_member_count(
                EVOLVE_SCENARIOS_FUNCTION_NAME,
                0,
                "prev_scenarios",
                "prev_index",
                1,
                0,
                bad,
            )
            .expect_err("non-positive member_count must fail");
            let text = err.to_c_string().to_string_lossy().into_owned();
            assert!(
                text.contains("[ZERO_MEMBER_SCENARIO]") && text.contains("prev_index=1"),
                "unexpected error text: {text}"
            );
        }
    }

    #[test]
    fn h3_diagnose_batch_whitelist_and_hard_abort_contracts() {
        let base_cfg = ScenarioConfig {
            projection_dim: 1,
            cluster_count: 2,
            initialization: InitMethod::KMeansPlusPlusChaCha8V1,
            seed: 0,
            restarts: 5,
            initial_centers: vec![],
            stopping_rule: StopRule::SklearnVarianceScaledV1,
            tolerance: 1e-4,
            max_iterations: 300,
            rank_threshold: 1e-10,
        };

        // 1. OK
        let ok_task = TaskInput {
            config: base_cfg.clone(),
            members: vec![
                MemberInput {
                    member_index: 0,
                    member_id: "m0".into(),
                    features: vec![0.0, 0.0],
                },
                MemberInput {
                    member_index: 1,
                    member_id: "m1".into(),
                    features: vec![0.2, 0.1],
                },
                MemberInput {
                    member_index: 2,
                    member_id: "m2".into(),
                    features: vec![10.0, 9.0],
                },
                MemberInput {
                    member_index: 3,
                    member_id: "m3".into(),
                    features: vec![10.2, 9.1],
                },
            ],
        };
        assert_eq!(
            unwrap_sql(diagnose_batch_task(&ok_task, &[1.0, 1.0], 0)),
            "OK"
        );

        // 2. ZERO_DISPERSION (identical members, valid config)
        let zd_task = TaskInput {
            config: base_cfg.clone(),
            members: vec![
                MemberInput {
                    member_index: 0,
                    member_id: "m0".into(),
                    features: vec![5.0, 5.0],
                },
                MemberInput {
                    member_index: 1,
                    member_id: "m1".into(),
                    features: vec![5.0, 5.0],
                },
            ],
        };
        assert_eq!(
            unwrap_sql(diagnose_batch_task(&zd_task, &[1.0, 1.0], 0)),
            "ZERO_DISPERSION"
        );

        // 3. INSUFFICIENT_DISTINCT_VECTORS (2 distinct vectors but cluster_count = 3)
        let mut idv_cfg = base_cfg.clone();
        idv_cfg.cluster_count = 3;
        let idv_task = TaskInput {
            config: idv_cfg,
            members: vec![
                MemberInput {
                    member_index: 0,
                    member_id: "m0".into(),
                    features: vec![0.0, 0.0],
                },
                MemberInput {
                    member_index: 1,
                    member_id: "m1".into(),
                    features: vec![0.0, 0.0],
                },
                MemberInput {
                    member_index: 2,
                    member_id: "m2".into(),
                    features: vec![10.0, 10.0],
                },
            ],
        };
        assert_eq!(
            unwrap_sql(diagnose_batch_task(&idv_task, &[1.0, 1.0], 0)),
            "INSUFFICIENT_DISTINCT_VECTORS"
        );

        // 4. INSUFFICIENT_RANK (collinear 2D data with projection_dim = 2)
        let mut ir_cfg = base_cfg.clone();
        ir_cfg.projection_dim = 2;
        let ir_task = TaskInput {
            config: ir_cfg,
            members: vec![
                MemberInput {
                    member_index: 0,
                    member_id: "m0".into(),
                    features: vec![0.0, 0.0],
                },
                MemberInput {
                    member_index: 1,
                    member_id: "m1".into(),
                    features: vec![1.0, 2.0],
                },
                MemberInput {
                    member_index: 2,
                    member_id: "m2".into(),
                    features: vec![2.0, 4.0],
                },
            ],
        };
        assert_eq!(
            unwrap_sql(diagnose_batch_task(&ir_task, &[1.0, 1.0], 0)),
            "INSUFFICIENT_RANK"
        );

        // 5. Hard-abort on DIMENSION_MISMATCH
        let dm_err = diagnose_batch_task(&ok_task, &[1.0], 0)
            .expect_err("weights length mismatch must abort");
        assert!(
            dm_err
                .to_c_string()
                .to_string_lossy()
                .contains("[DIMENSION_MISMATCH]")
        );

        // 6. Hard-abort on NUMERICAL_FAILURE (variance overflow at 1e300)
        let nf_task = TaskInput {
            config: base_cfg.clone(),
            members: vec![
                MemberInput {
                    member_index: 0,
                    member_id: "m0".into(),
                    features: vec![-1e300, 0.0],
                },
                MemberInput {
                    member_index: 1,
                    member_id: "m1".into(),
                    features: vec![1e300, 1.0],
                },
            ],
        };
        let nf_err =
            diagnose_batch_task(&nf_task, &[1.0, 1.0], 0).expect_err("overflow must fail query");
        assert!(
            nf_err
                .to_c_string()
                .to_string_lossy()
                .contains("[NUMERICAL_FAILURE]")
        );

        // 7. Hard-abort on INVALID_CONFIG even if data has zero dispersion
        let mut bad_cfg = base_cfg;
        bad_cfg.cluster_count = 1;
        let bad_cfg_task = TaskInput {
            config: bad_cfg,
            members: zd_task.members,
        };
        let cfg_err = diagnose_batch_task(&bad_cfg_task, &[1.0, 1.0], 0)
            .expect_err("invalid config must abort");
        assert!(
            cfg_err
                .to_c_string()
                .to_string_lossy()
                .contains("[INVALID_CONFIG]")
        );
    }
}
