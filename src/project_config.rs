use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, SessionContext};
use serde::{Deserialize, Serialize};
use std::ffi::OsStr;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaBuilder};
use datafusion::common::{DEFAULT_CSV_EXTENSION, DEFAULT_PARQUET_EXTENSION};
use datafusion::config::CatalogOptions;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use sqlparser::test_utils::table;
use walkdir::WalkDir;

// Format definitions for the config file
#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub(crate) scheduler: SchedulerConfig,
    pub(crate) executors: Vec<ExecutorConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SchedulerConfig {
    pub(crate) id_addr: String,
    pub(crate) port: u16,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExecutorConfig {
    pub(crate) id: i32,
    ip_addr: String,
    port: u16,
    numa_node: u8,
}

pub fn read_config(config_path: &str) -> Config {
    let config_str = fs::read_to_string(config_path).expect("Failed to read config file");
    toml::from_str(&config_str).expect("Failed to parse config file")
}


pub const TPCH_TABLES: &[&str] = &[
    "part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region",
];

pub async fn load_catalog(catalog_path: &str) -> Arc<SessionContext> {
    let ctx = SessionContext::new();
    
    for table in TPCH_TABLES {
        let table_provider = { get_table(&ctx, table, catalog_path).await.expect("error loading catalog") };
        ctx.register_table(*table, table_provider).expect("error registering table");
    }
    Arc::new(ctx)
}


async fn get_table(
    ctx: &SessionContext,
    table: &str,
    catalog_path: &str
) -> Result<Arc<dyn TableProvider>, DataFusionError> {


    let target_partitions = 2;

    // Obtain a snapshot of the SessionState
    let state = ctx.state();
    let (format, path, extension): (Arc<dyn FileFormat>, String, &'static str) = {
                let path = format!("{catalog_path}/{table}.tbl");
                eprintln!("Path is {}", path);

                let format = CsvFormat::default()
                    .with_delimiter(b'|')
                    .with_has_header(false);

                (Arc::new(format), path, ".tbl")};

    let options = ListingOptions::new(format)
        .with_file_extension(extension)
        .with_target_partitions(target_partitions)
        .with_collect_stat(state.config().collect_statistics());

    let table_path = ListingTableUrl::parse(path)?;
    let config = ListingTableConfig::new(table_path).with_listing_options(options);
    let config = config.with_schema(Arc::new(get_tbl_tpch_table_schema(table)));

    Ok(Arc::new(ListingTable::try_new(config)?))
}

/// The `.tbl` file contains a trailing column
pub fn get_tbl_tpch_table_schema(table: &str) -> Schema {
    let mut schema = SchemaBuilder::from(get_tpch_table_schema(table).fields);
    schema.push(Field::new("__placeholder", DataType::Utf8, true));
    schema.finish()
}

/// Get the schema for the benchmarks derived from TPC-H
pub fn get_tpch_table_schema(table: &str) -> Schema {
    match table {
        "part" => Schema::new(vec![
            Field::new("p_partkey", DataType::Int64, false),
            Field::new("p_name", DataType::Utf8, false),
            Field::new("p_mfgr", DataType::Utf8, false),
            Field::new("p_brand", DataType::Utf8, false),
            Field::new("p_type", DataType::Utf8, false),
            Field::new("p_size", DataType::Int32, false),
            Field::new("p_container", DataType::Utf8, false),
            Field::new("p_retailprice", DataType::Decimal128(15, 2), false),
            Field::new("p_comment", DataType::Utf8, false),
        ]),

        "supplier" => Schema::new(vec![
            Field::new("s_suppkey", DataType::Int64, false),
            Field::new("s_name", DataType::Utf8, false),
            Field::new("s_address", DataType::Utf8, false),
            Field::new("s_nationkey", DataType::Int64, false),
            Field::new("s_phone", DataType::Utf8, false),
            Field::new("s_acctbal", DataType::Decimal128(15, 2), false),
            Field::new("s_comment", DataType::Utf8, false),
        ]),

        "partsupp" => Schema::new(vec![
            Field::new("ps_partkey", DataType::Int64, false),
            Field::new("ps_suppkey", DataType::Int64, false),
            Field::new("ps_availqty", DataType::Int32, false),
            Field::new("ps_supplycost", DataType::Decimal128(15, 2), false),
            Field::new("ps_comment", DataType::Utf8, false),
        ]),

        "customer" => Schema::new(vec![
            Field::new("c_custkey", DataType::Int64, false),
            Field::new("c_name", DataType::Utf8, false),
            Field::new("c_address", DataType::Utf8, false),
            Field::new("c_nationkey", DataType::Int64, false),
            Field::new("c_phone", DataType::Utf8, false),
            Field::new("c_acctbal", DataType::Decimal128(15, 2), false),
            Field::new("c_mktsegment", DataType::Utf8, false),
            Field::new("c_comment", DataType::Utf8, false),
        ]),

        "orders" => Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_custkey", DataType::Int64, false),
            Field::new("o_orderstatus", DataType::Utf8, false),
            Field::new("o_totalprice", DataType::Decimal128(15, 2), false),
            Field::new("o_orderdate", DataType::Date32, false),
            Field::new("o_orderpriority", DataType::Utf8, false),
            Field::new("o_clerk", DataType::Utf8, false),
            Field::new("o_shippriority", DataType::Int32, false),
            Field::new("o_comment", DataType::Utf8, false),
        ]),

        "lineitem" => Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_partkey", DataType::Int64, false),
            Field::new("l_suppkey", DataType::Int64, false),
            Field::new("l_linenumber", DataType::Int32, false),
            Field::new("l_quantity", DataType::Decimal128(15, 2), false),
            Field::new("l_extendedprice", DataType::Decimal128(15, 2), false),
            Field::new("l_discount", DataType::Decimal128(15, 2), false),
            Field::new("l_tax", DataType::Decimal128(15, 2), false),
            Field::new("l_returnflag", DataType::Utf8, false),
            Field::new("l_linestatus", DataType::Utf8, false),
            Field::new("l_shipdate", DataType::Date32, false),
            Field::new("l_commitdate", DataType::Date32, false),
            Field::new("l_receiptdate", DataType::Date32, false),
            Field::new("l_shipinstruct", DataType::Utf8, false),
            Field::new("l_shipmode", DataType::Utf8, false),
            Field::new("l_comment", DataType::Utf8, false),
        ]),

        "nation" => Schema::new(vec![
            Field::new("n_nationkey", DataType::Int64, false),
            Field::new("n_name", DataType::Utf8, false),
            Field::new("n_regionkey", DataType::Int64, false),
            Field::new("n_comment", DataType::Utf8, false),
        ]),

        "region" => Schema::new(vec![
            Field::new("r_regionkey", DataType::Int64, false),
            Field::new("r_name", DataType::Utf8, false),
            Field::new("r_comment", DataType::Utf8, false),
        ]),

        _ => unimplemented!(),
    }
}




#[cfg(test)]
mod tests {

    use crate::project_config::read_config;
    #[test]
    pub fn test_read_config() {
        let config = read_config("./executors.toml");
        assert_eq!("0.0.0.0", config.scheduler.id_addr);
    }
}
