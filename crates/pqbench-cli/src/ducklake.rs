use clap::Args;
use pqbench::table::ducklake::{self, DuckLakeRequest};

/// Arguments for DuckLake snapshot analysis.
#[derive(Args)]
pub(crate) struct DuckLakeArgs {
    /// local DuckLake metadata catalog (SQLite)
    #[arg(required_unless_present = "source", conflicts_with = "source")]
    catalog: Option<String>,
    /// read the catalog path from a source document on standard input (`-` only)
    #[arg(long, value_name = "-")]
    source: Option<String>,
    /// schema name
    #[arg(long, default_value = "main")]
    schema: String,
    /// table name
    #[arg(long)]
    table: String,
    /// snapshot id; defaults to the latest snapshot
    #[arg(long)]
    snapshot: Option<u64>,
    /// emit the complete report as JSON
    #[arg(long = "json", conflicts_with = "d3")]
    json: bool,
    /// emit a self-contained d3 treemap HTML
    #[arg(long = "d3")]
    d3: bool,
}

pub(crate) fn run(args: &DuckLakeArgs) -> Result<(), crate::CliError> {
    let request = DuckLakeRequest {
        catalog: match (&args.source, &args.catalog) {
            (Some(source), _) => crate::source::read_source_table(source, "DuckLake")?,
            (None, Some(catalog)) => catalog.clone(),
            (None, None) => return Err("ducklake needs a catalog or --source -".into()),
        },
        schema: args.schema.clone(),
        table: args.table.clone(),
        snapshot: args.snapshot,
    };
    let runtime = tokio::runtime::Runtime::new()?;
    let report = runtime.block_on(ducklake::ducklake(&request))?;
    let output = if args.json {
        ducklake::render_json(&report)?
    } else if args.d3 {
        ducklake::render_html(&report)?
    } else {
        ducklake::render_text(&report)?
    };
    print!("{output}");
    Ok(())
}
