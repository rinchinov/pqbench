use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, ValueEnum};

#[derive(Parser)]
#[command(
    name = "ducklakebench",
    about = "Physical storage analysis for local DuckLake snapshots",
    after_help = r#"
Examples:
  ducklakebench ./catalog.ducklake --table events
  ducklakebench ./catalog.ducklake --schema analytics --table events --snapshot 42
  ducklakebench ./catalog.ducklake --table events --json
  ducklakebench ./catalog.ducklake --table events --d3 > treemap.html
"#
)]
struct Cli {
    /// local DuckDB metadata catalog
    catalog: PathBuf,
    /// DuckLake schema (default: main)
    #[arg(long, default_value = "main")]
    schema: String,
    /// DuckLake table to analyze
    #[arg(long)]
    table: String,
    /// snapshot id (default: latest)
    #[arg(long)]
    snapshot: Option<u64>,
    /// output format
    #[arg(long, value_enum, default_value_t = Format::Text)]
    format: Format,
    /// shorthand for --format json
    #[arg(long, conflicts_with_all = ["d3", "format"])]
    json: bool,
    /// shorthand for --format d3
    #[arg(long, conflicts_with_all = ["json", "format"])]
    d3: bool,
}

#[derive(Clone, Copy, ValueEnum)]
enum Format {
    Text,
    Json,
    D3,
}

type CliError = Box<dyn std::error::Error + Send + Sync>;

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: Cli) -> Result<(), CliError> {
    let report =
        ducklakebench::read_local(&args.catalog, &args.schema, &args.table, args.snapshot)?;
    let format = if args.json {
        Format::Json
    } else if args.d3 {
        Format::D3
    } else {
        args.format
    };
    let output = match format {
        Format::Text => ducklakebench::render(&report),
        Format::Json => ducklakebench::json(&report)?,
        Format::D3 => ducklakebench::render_html(&report)?,
    };
    print!("{output}");
    if !output.ends_with('\n') {
        println!();
    }
    Ok(())
}
