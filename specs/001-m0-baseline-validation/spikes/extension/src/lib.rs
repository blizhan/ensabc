use std::{error::Error, sync::Arc};

use duckdb::{
    Connection,
    arrow::{
        array::{Array, Float64Array, ListArray, RecordBatch, StructArray},
        datatypes::{DataType, Field},
    },
    duckdb_entrypoint_c_api,
    vscalar::arrow::{ArrowFunctionSignature, VArrowScalar},
};

struct Echo;

impl VArrowScalar for Echo {
    type State = ();

    fn invoke(_: &Self::State, input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let members = input.column(0).as_any().downcast_ref::<ListArray>()
            .ok_or_else(|| invalid("members must be a list"))?;
        for row in 0..members.len() {
            if members.is_null(row) {
                return Err(invalid(format!("members row {row} is NULL")));
            }
            let items = members.value(row);
            let items = items.as_any().downcast_ref::<StructArray>()
                .ok_or_else(|| invalid(format!("members row {row} must contain structs")))?;
            if items.is_empty() {
                return Err(invalid(format!("members row {row} is empty")));
            }
            let ids = items.column(0);
            let feature_lists = items.column(1).as_any().downcast_ref::<ListArray>()
                .ok_or_else(|| invalid(format!("members row {row} features must be a list")))?;
            for item in 0..items.len() {
                if items.is_null(item) {
                    return Err(invalid(format!("members row {row} item {item} is NULL")));
                }
                if ids.is_null(item) {
                    return Err(invalid(format!("members row {row} item {item} member_id is NULL")));
                }
                if feature_lists.is_null(item) {
                    return Err(invalid(format!("members row {row} item {item} features is NULL")));
                }
                let values = feature_lists.value(item);
                let values = values.as_any().downcast_ref::<Float64Array>()
                    .ok_or_else(|| invalid(format!("members row {row} item {item} features must be DOUBLE[]")))?;
                if values.is_empty() {
                    return Err(invalid(format!("members row {row} item {item} features is empty")));
                }
                for feature in 0..values.len() {
                    if values.is_null(feature) || !values.value(feature).is_finite() {
                        return Err(invalid(format!("members row {row} item {item} feature {feature} is NULL or non-finite")));
                    }
                }
            }
        }
        Ok(input.column(0).clone())
    }

    fn signatures() -> Vec<ArrowFunctionSignature> {
        let features = DataType::List(Arc::new(Field::new("item", DataType::Float64, true)));
        let member = DataType::Struct(
            vec![
                Field::new("member_id", DataType::Utf8, true),
                Field::new("features", features, true),
            ].into(),
        );
        let members = DataType::List(Arc::new(Field::new("item", member, true)));
        vec![ArrowFunctionSignature::exact(vec![members.clone()], members)]
    }
}

fn invalid(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(std::io::Error::new(std::io::ErrorKind::InvalidInput, message.into()))
}

#[duckdb_entrypoint_c_api(ext_name = "ensabc_m0_extension", min_duckdb_version = "v1.5.5")]
pub fn extension_entrypoint(connection: Connection) -> Result<(), Box<dyn Error>> {
    connection.register_scalar_function::<Echo>("ens_m0_echo")?;
    Ok(())
}
