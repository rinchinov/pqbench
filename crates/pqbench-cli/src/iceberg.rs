use clap::Args;
use pqbench::table::iceberg::{self, IcebergRequest};

/// Arguments for Iceberg snapshot analysis.
#[derive(Args)]
pub(crate) struct IcebergArgs {
    /// Iceberg metadata JSON path or URI
    #[arg(required_unless_present = "source", conflicts_with = "source")]
    metadata: Option<String>,
    /// read the metadata location from a source document on standard input (`-` only)
    #[arg(long, value_name = "-")]
    source: Option<String>,
    /// snapshot id; defaults to the current snapshot
    #[arg(long)]
    snapshot_id: Option<i64>,
    /// emit the complete report as JSON
    #[arg(long = "json", conflicts_with = "d3")]
    json: bool,
    /// emit a self-contained d3 treemap HTML
    #[arg(long = "d3")]
    d3: bool,
}

pub(crate) fn run(args: &IcebergArgs) -> Result<(), crate::CliError> {
    let request = IcebergRequest {
        metadata: match (&args.source, &args.metadata) {
            (Some(source), _) => crate::source::read_source_table(source, "Iceberg")?,
            (None, Some(metadata)) => metadata.clone(),
            (None, None) => return Err("iceberg needs metadata or --source -".into()),
        },
        snapshot_id: args.snapshot_id,
    };
    let runtime = tokio::runtime::Runtime::new()?;
    let report = runtime.block_on(iceberg::iceberg(&request))?;
    let output = if args.json {
        iceberg::render_json(&report)?
    } else if args.d3 {
        iceberg::render_html(&report)?
    } else {
        iceberg::render_text(&report)?
    };
    print!("{output}");
    Ok(())
}
