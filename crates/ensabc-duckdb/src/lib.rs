//! M0 DuckDB extension: the `ens_m0_echo` diagnostic roundtrip boundary.
//!
//! The function has the shape declared in the M0 SQL contract:
//!
//! ```sql
//! ens_m0_echo(
//!     members STRUCT(member_id VARCHAR, features DOUBLE[])[]
//! ) -> STRUCT(member_id VARCHAR, features DOUBLE[])[]
//! ```
//!
//! It is a *diagnostic* boundary, not the M1 clustering interface. It returns
//! the members it was given, unchanged and in order, and adds no cluster id,
//! probability, ordering or aggregation.
//!
//! Two properties drive the implementation:
//!
//! * **Top-level `NULL` must be rejected explicitly.** With DuckDB's default
//!   null handling a `NULL` argument is folded into a `NULL` constant by the
//!   binder and the function is never called, which would silently accept an
//!   illegal input. The function therefore registers with
//!   `duckdb_scalar_function_set_special_handling`, so the binder keeps the
//!   call and the `NULL` reaches the callback, where it is reported as an
//!   error.
//! * **Every nested layer must be validated.** The validation walks the list
//!   offsets and lengths and the struct children, so a `NULL` member item, an
//!   empty or duplicated `member_id`, an empty or `NULL` feature list, a
//!   `NULL`/NaN/infinite feature value and a task whose members disagree on
//!   the feature count are all rejected with a located reason.
//!
//! The function is registered through the DuckDB C API directly, because the
//! high-level `duckdb` crate does not expose the special-handling setter. The
//! extension entry point is written by hand as well, so that the raw
//! `duckdb_connection` the C API needs is available without opening a second
//! connection.

use std::{
    ffi::CString,
    panic::{AssertUnwindSafe, catch_unwind},
    ptr,
};

use duckdb::ffi::{
    DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE, DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR, DuckDBSuccess, duckdb_connect,
    duckdb_create_list_type, duckdb_create_logical_type, duckdb_create_scalar_function,
    duckdb_create_struct_type, duckdb_data_chunk, duckdb_data_chunk_get_size,
    duckdb_data_chunk_get_vector, duckdb_destroy_scalar_function, duckdb_function_info,
    duckdb_list_entry, duckdb_list_vector_get_child, duckdb_register_scalar_function,
    duckdb_scalar_function_add_parameter, duckdb_scalar_function_set_error,
    duckdb_scalar_function_set_function, duckdb_scalar_function_set_name,
    duckdb_scalar_function_set_return_type, duckdb_scalar_function_set_special_handling,
    duckdb_string_t, duckdb_string_t_data, duckdb_string_t_length, duckdb_struct_vector_get_child,
    duckdb_validity_row_is_valid, duckdb_vector, duckdb_vector_get_data, duckdb_vector_get_validity,
    duckdb_vector_reference_vector,
};

/// The name the function is registered under.
const FUNCTION_NAME: &str = "ens_m0_echo";

/// The minimum DuckDB version this extension is built against.
const MIN_DUCKDB_VERSION: &str = "v1.5.5";

/// The error message prefix. Every rejection names the function and states
/// the layer and the located position, so the caller can tell a top-level
/// rejection from a nested one. DuckDB adds the `Invalid Input Error` type
/// prefix itself, so it is not repeated here.
const ERROR_PREFIX: &str = "ens_m0_echo:";

/// An error raised while validating one input row.
struct ValidationError {
    message: String,
}

impl ValidationError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Report a rejection through the DuckDB function info handle.
///
/// DuckDB copies the message out of the pointer during the call, so the
/// string only has to live for the duration of the call.
fn set_error(info: duckdb_function_info, message: &str) {
    let text = format!("{ERROR_PREFIX} {message}");
    let owned = match CString::new(text) {
        Ok(value) => value,
        Err(_) => CString::new(ERROR_PREFIX).expect("the error prefix has no NUL byte"),
    };
    // SAFETY: `info` is the live function info handle DuckDB passed to the
    // callback, and `owned` outlives the call.
    unsafe {
        duckdb_scalar_function_set_error(info, owned.as_ptr());
    }
}

/// Read the validity bitmap of a vector and test one row.
fn row_is_valid(validity: *mut u64, row: u64) -> bool {
    if validity.is_null() {
        // A null validity pointer means the whole vector is valid.
        return true;
    }
    // SAFETY: `validity` is the validity mask of a live vector and `row` is
    // within that vector's populated range.
    unsafe { duckdb_validity_row_is_valid(validity, row) }
}

/// Read the `member_id` string of one struct row.
///
/// # Safety
///
/// `vector` must be a live VARCHAR vector and `row` must be within its
/// populated range.
/// # Safety
///
/// `vector` must be a live VARCHAR vector and `row` must be within its
/// populated range.
unsafe fn read_string(vector: duckdb_vector, row: u64) -> String {
    // SAFETY: the caller guarantees that the vector is live and that `row`
    // addresses an initialized slot of its VARCHAR payload.
    unsafe {
        let data = duckdb_vector_get_data(vector) as *const duckdb_string_t;
        let string = *data.add(row as usize);
        let length = duckdb_string_t_length(string) as usize;
        let bytes = duckdb_string_t_data(ptr::from_ref(&string).cast_mut());
        let slice = std::slice::from_raw_parts(bytes.cast::<u8>(), length);
        String::from_utf8_lossy(slice).into_owned()
    }
}
/// Validate one task (one list row) and return the reason it is rejected.
fn validate_task(members: duckdb_vector, row: u64) -> Result<(), ValidationError> {
    // SAFETY: `members` is a live vector owned by the current data chunk, and
    // every access below is bounded by the list offsets and lengths that
    // DuckDB wrote for this vector.
    unsafe {
        let members_validity = duckdb_vector_get_validity(members);
        if !row_is_valid(members_validity, row) {
            return Err(ValidationError::new(format!(
                "task input layer: the members list of task row {row} is NULL"
            )));
        }

        // The list entry of this row gives the offset and length of its members.
        let entries = duckdb_vector_get_data(members) as *const duckdb_list_entry;
        let entry = *entries.add(row as usize);
        let offset = entry.offset;
        let length = entry.length;

        if length == 0 {
            return Err(ValidationError::new(format!(
                "task input layer: task row {row} has an empty member list"
            )));
        }

        // The struct children of the list.
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

            // The member item itself may be NULL.
            if !row_is_valid(member_validity, member_row) {
                return Err(ValidationError::new(format!(
                    "task row {row} member {member_index}: the member item is NULL"
                )));
            }

            // The member id must be a non-NULL, non-empty, unique string.
            if !row_is_valid(ids_validity, member_row) {
                return Err(ValidationError::new(format!(
                    "task row {row} member {member_index}: member_id is NULL"
                )));
            }
            let member_id = read_string(ids, member_row);
            if member_id.is_empty() {
                return Err(ValidationError::new(format!(
                    "task row {row} member {member_index}: member_id is empty"
                )));
            }
            if seen_ids.iter().any(|seen| seen == &member_id) {
                return Err(ValidationError::new(format!(
                    "task row {row} member {member_index}: duplicate member_id {member_id:?}"
                )));
            }
            seen_ids.push(member_id);

            // The feature list must be a non-NULL, non-empty list of finite values.
            if !row_is_valid(features_validity, member_row) {
                return Err(ValidationError::new(format!(
                    "task row {row} member {member_index}: features is NULL"
                )));
            }
            let feature_entries = duckdb_vector_get_data(features) as *const duckdb_list_entry;
            let feature_entry = *feature_entries.add(member_row as usize);
            let feature_offset = feature_entry.offset;
            let feature_length = feature_entry.length;

            if feature_length == 0 {
                return Err(ValidationError::new(format!(
                    "task row {row} member {member_index}: features is empty"
                )));
            }

            // All members of one task must carry the same number of features.
            match expected_feature_count {
                None => expected_feature_count = Some(feature_length),
                Some(expected) if expected != feature_length => {
                    return Err(ValidationError::new(format!(
                        "task row {row} member {member_index}: feature length {feature_length} differs from the task's {expected}"
                    )));
                }
                Some(_) => {}
            }

            // The feature values must be present and finite.
            let feature_values = duckdb_list_vector_get_child(features);
            let feature_values_validity = duckdb_vector_get_validity(feature_values);
            let values = duckdb_vector_get_data(feature_values) as *const f64;
            for feature_index in 0..feature_length {
                let value_row = feature_offset + feature_index;
                if !row_is_valid(feature_values_validity, value_row) {
                    return Err(ValidationError::new(format!(
                        "task row {row} member {member_index} feature {feature_index}: the feature value is NULL"
                    )));
                }
                let value = *values.add(value_row as usize);
                if !value.is_finite() {
                    return Err(ValidationError::new(format!(
                        "task row {row} member {member_index} feature {feature_index}: the feature value is non-finite ({value})"
                    )));
                }
            }
        }

        Ok(())
    }
}

/// The scalar function callback.
///
/// The callback is wrapped in `catch_unwind` so that a panic cannot cross the
/// FFI boundary and abort the host process. A panic is reported as an
/// ordinary query error instead.
unsafe extern "C" fn echo_callback(
    info: duckdb_function_info,
    input: duckdb_data_chunk,
    output: duckdb_vector,
) {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: `input` is the live data chunk that DuckDB passes to the
        // callback, and `output` is the result vector of the same call.
        unsafe {
            let row_count = duckdb_data_chunk_get_size(input);
            if row_count == 0 {
                return Ok(());
            }

            let members = duckdb_data_chunk_get_vector(input, 0);
            if members.is_null() {
                return Err(ValidationError::new(
                    "task input layer: the members argument vector is missing",
                ));
            }

            for row in 0..row_count {
                validate_task(members, row)?;
            }

            // The values are returned unchanged: the input vector is
            // referenced directly, so the member order, the feature order and
            // the values are preserved exactly.
            duckdb_vector_reference_vector(output, members);
            Ok(())
        }
    }));

    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(error)) => set_error(info, &error.message),
        Err(_) => set_error(
            info,
            "the internal validation panicked; the input was rejected rather than propagated",
        ),
    }
}

/// Build the `STRUCT(member_id VARCHAR, features DOUBLE[])[]` logical type.
fn build_members_type() -> duckdb::ffi::duckdb_logical_type {
    // SAFETY: the logical types are created by DuckDB and immediately handed
    // to DuckDB, which takes ownership of the nested types.
    unsafe {
        let varchar = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR);
        let double = duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE);
        let feature_list = duckdb_create_list_type(double);

        let mut member_types = [varchar, feature_list];
        let member_id_name = CString::new("member_id").expect("no NUL in the member name");
        let features_name = CString::new("features").expect("no NUL in the member name");
        let mut member_names = [member_id_name.as_ptr(), features_name.as_ptr()];

        let member_struct = duckdb_create_struct_type(
            member_types.as_mut_ptr(),
            member_names.as_mut_ptr(),
            member_types.len() as u64,
        );

        // The struct and the inner list type are owned by the struct type, so
        // only the outer list type is destroyed by the caller.
        duckdb_create_list_type(member_struct)
    }
}

/// Register the function on the given connection.
///
/// # Safety
///
/// `connection` must be a live DuckDB connection.
unsafe fn register(connection: duckdb::ffi::duckdb_connection) -> Result<(), String> {
    // SAFETY: the caller guarantees a live connection, and every DuckDB C API
    // call below operates on handles that this function creates and destroys.
    unsafe {
        let name = CString::new(FUNCTION_NAME).expect("no NUL in the function name");
        let function = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(function, name.as_ptr());

        let members_type = build_members_type();
        duckdb_scalar_function_add_parameter(function, members_type);
        duckdb_scalar_function_set_return_type(function, members_type);
        duckdb::ffi::duckdb_destroy_logical_type(ptr::from_ref(&members_type).cast_mut());

        // The critical difference from the default null handling: a NULL
        // argument is delivered to the callback instead of being folded into a
        // NULL constant by the binder.
        duckdb_scalar_function_set_special_handling(function);
        duckdb_scalar_function_set_function(function, Some(echo_callback));

        let state = duckdb_register_scalar_function(connection, function);
        duckdb_destroy_scalar_function(ptr::from_ref(&function).cast_mut());
        if state != DuckDBSuccess {
            return Err(format!(
                "failed to register {FUNCTION_NAME} on the extension connection"
            ));
        }
        Ok(())
    }
}

/// The extension entry point.
///
/// DuckDB looks up `<extension name>_init_c_api` in the shared library. The
/// entry point receives the extension info and the extension access struct,
/// initializes the C API function table and registers the scalar function on a
/// connection to the extension's database.
///
/// # Safety
///
/// `info` and `access` must be the handles DuckDB passes to the extension
/// entry point. They are only valid for the duration of the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ensabc_m0_extension_init_c_api(
    info: duckdb::ffi::duckdb_extension_info,
    access: *const duckdb::ffi::duckdb_extension_access,
) -> bool {
    let init = || -> Result<(), String> {
        let have_api = unsafe {
            duckdb::ffi::duckdb_rs_extension_api_init(info, access, MIN_DUCKDB_VERSION)
        }
        .map_err(|message| message.to_string())?;
        if !have_api {
            return Err(
                "the DuckDB C API could not be initialized for this extension version".to_string()
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
            duckdb::ffi::duckdb_disconnect(ptr::from_ref(&connection).cast_mut());
        }
        registration
    };

    match init() {
        Ok(()) => true,
        Err(message) => {
            if let Some(set_error) = unsafe { (*access).set_error } {
                let text = CString::new(message).unwrap_or_else(|_| {
                    CString::new("the extension failed to initialize").expect("no NUL byte")
                });
                unsafe {
                    set_error(info, text.as_ptr());
                }
            }
            false
        }
    }
}
