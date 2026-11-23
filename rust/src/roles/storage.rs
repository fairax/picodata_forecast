use tarantool::index::IndexOptions;
use tarantool::space::{Field, Space};

#[tarantool::proc]
pub fn storage_init() -> tarantool::Result<bool> {
    tarantool::say_info!("Initializing forecast cache schema");
    create_schema()?;
    tarantool::say_info!("Forecast cache schema is ready");
    Ok(true)
}

fn create_schema() -> tarantool::Result<()> {
    let space = Space::builder("forecast_cache")
        .if_not_exists(true)
        .format([
            Field::string("key"),
            Field::unsigned("bucket_id"),
            Field::varbinary("body"),
            Field::unsigned("expires_at"),
            Field::unsigned("last_access"),
        ])
        .create()?;

    space.create_index(
        "primary",
        &IndexOptions {
            if_not_exists: Some(true),
            parts: Some(vec!["key".into()]),
            ..Default::default()
        },
    )?;

    space.create_index(
        "bucket_id",
        &IndexOptions {
            unique: Some(false),
            if_not_exists: Some(true),
            parts: Some(vec!["bucket_id".into()]),
            ..Default::default()
        },
    )?;

    space.create_index(
        "expires_at",
        &IndexOptions {
            unique: Some(false),
            if_not_exists: Some(true),
            parts: Some(vec!["expires_at".into(), "key".into()]),
            ..Default::default()
        },
    )?;

    space.create_index(
        "lru",
        &IndexOptions {
            unique: Some(false),
            if_not_exists: Some(true),
            parts: Some(vec!["last_access".into(), "key".into()]),
            ..Default::default()
        },
    )?;

    Ok(())
}
