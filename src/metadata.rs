use datafusion::common::{exec_datafusion_err, DataFusionError};
use std::fmt::Display;
use std::str::FromStr;
use tonic::metadata::{Ascii, MetadataMap, MetadataValue};

fn values_from_metadata_opt<T: FromStr>(
    map: &MetadataMap,
    key: &str,
) -> Result<Option<Vec<T>>, DataFusionError>
where
    <T as FromStr>::Err: Display,
{
    if !map.contains_key(key) {
        return Ok(None);
    }
    values_from_metadata(map, key).map(Some)
}

fn values_from_metadata<T: FromStr>(map: &MetadataMap, key: &str) -> Result<Vec<T>, DataFusionError>
where
    <T as FromStr>::Err: Display,
{
    let value = map
        .get(key)
        .ok_or_else(|| exec_datafusion_err!("Key {key} not present in gRPC metadata"))?
        .to_str()
        .map_err(|err| {
            exec_datafusion_err!("Value for gRPC metadata key '{key}' is not valid ascii: {err}")
        })?;
    if value.is_empty() {
        return Ok(vec![]);
    }
    value
        .split(",")
        .map(|v| v.parse())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| exec_datafusion_err!("Failed to parse gRPC metadata key '{key}': {err}"))
}

fn value_from_metadata_opt<T: FromStr>(
    map: &MetadataMap,
    key: &str,
) -> Result<Option<T>, DataFusionError>
where
    <T as FromStr>::Err: Display,
{
    if !map.contains_key(key) {
        return Ok(None);
    }
    value_from_metadata(map, key).map(Some)
}

fn value_from_metadata<T: FromStr>(map: &MetadataMap, key: &str) -> Result<T, DataFusionError>
where
    <T as FromStr>::Err: Display,
{
    map.get(key)
        .ok_or_else(|| exec_datafusion_err!("Key {key} not present in gRPC metadata"))?
        .to_str()
        .map_err(|err| {
            exec_datafusion_err!("Value for gRPC metadata key '{key}' is not valid ascii: {err}")
        })?
        .parse()
        .map_err(|err| exec_datafusion_err!("Failed to parse gRPC metadata key '{key}': {err}"))
}

fn into_metadata_value(v: impl Display) -> Result<MetadataValue<Ascii>, DataFusionError> {
    v.to_string()
        .parse()
        .map_err(|err| DataFusionError::External(Box::new(err)))
}

fn into_metadata_values(v: &[impl Display]) -> Result<MetadataValue<Ascii>, DataFusionError> {
    into_metadata_value(
        v.iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(","),
    )
}
