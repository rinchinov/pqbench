use clap::Args;
use pqbench::bytemass;
use pqbench::table::TableInfo;
use std::io::IsTerminal;
use std::num::NonZeroUsize;

use crate::document::{self, Document};
use crate::CliError;

/// Arguments for `bytemass`.
#[derive(Args)]
pub(crate) struct BytemassArgs {
    /// parquet paths, a table/collection/source document, or `-` for stdin
    inputs: Vec<String>,
    /// maximum active tables in a collection
    #[arg(long, default_value = "4")]
    pub(crate) table_jobs: NonZeroUsize,
    /// maximum simultaneous footer reads shared by all tables
    #[arg(long, default_value = "32")]
    pub(crate) file_jobs: NonZeroUsize,
    /// save a report at each hierarchy level in a new directory (requires --json or --d3)
    #[arg(long)]
    pub(crate) output_dir: Option<std::path::PathBuf>,
    /// emit per-column byte masses as JSON instead of text stats
    #[arg(long = "json", conflicts_with = "d3")]
    pub(crate) json: bool,
    /// emit a self-contained d3 treemap HTML (open in a browser) instead of text stats
    #[arg(long = "d3")]
    pub(crate) d3: bool,
}

/// Build the typed request, measure, and render the CLI's chosen format. The
/// CLI owns the format decision; the library just returns the table.
pub(crate) fn run(args: &BytemassArgs) -> Result<(), CliError> {
    match resolve(args)? {
        Input::Parquet(inputs) => measure_files(inputs, args),
        Input::Table(info) => measure_table(info, args),
        Input::Collection(collection) => crate::collection::run(collection, args),
    }
}

enum Input {
    Parquet(Vec<String>),
    Table(TableInfo),
    Collection(pqbench::bytemass::batch::Collection<pqbench::bytemass::batch::Table>),
}

fn resolve(args: &BytemassArgs) -> Result<Input, CliError> {
    if args.inputs.is_empty() {
        if std::io::stdin().is_terminal() {
            return Err(
                "bytemass needs parquet files or a table/collection/source document".into(),
            );
        }
        return from_document("-");
    }
    if args.inputs.len() == 1 && document::looks_like_json(&args.inputs[0]) {
        return from_document(&args.inputs[0]);
    }
    if args
        .inputs
        .iter()
        .any(|input| document::looks_like_json(input))
    {
        return Err(
            "a document must be the only input; parquet paths cannot be mixed with it".into(),
        );
    }
    if args.output_dir.is_some() {
        return Err("--output-dir requires a collection document".into());
    }
    Ok(Input::Parquet(args.inputs.clone()))
}

fn from_document(input: &str) -> Result<Input, CliError> {
    match document::read_document(input)? {
        Document::Table(info) => Ok(Input::Table(info)),
        Document::Collection(collection) => Ok(Input::Collection(collection)),
        Document::RemoteSource(source) => Ok(Input::Parquet(source.inputs)),
    }
}

fn measure_table(info: TableInfo, args: &BytemassArgs) -> Result<(), CliError> {
    if args.output_dir.is_some() {
        return Err("--output-dir requires a collection document".into());
    }
    document::apply_env(&info.env)?;
    let inputs: Vec<String> = info.files.iter().map(|file| file.uri.clone()).collect();
    if inputs.is_empty() {
        return render_rows(&[], args);
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let rows = runtime.block_on(bytemass::bytemass(&bytemass::BytemassRequest { inputs }))?;
    for row in &rows {
        let file = info
            .files
            .iter()
            .find(|file| file.uri == row.file)
            .ok_or_else(|| format!("unexpected measured file: {}", row.file))?;
        if row.size != file.size {
            return Err(format!(
                "active file size differs from log: {} (expected {}, found {})",
                file.path, file.size, row.size
            )
            .into());
        }
    }
    render_rows(&rows, args)
}

fn measure_files(inputs: Vec<String>, args: &BytemassArgs) -> Result<(), CliError> {
    let request = bytemass::BytemassRequest { inputs };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let rows = runtime.block_on(bytemass::bytemass(&request))?;
    render_rows(&rows, args)
}

fn render_rows(rows: &[bytemass::MassRow], args: &BytemassArgs) -> Result<(), CliError> {
    let output = if args.json {
        bytemass::render_json(rows)?
    } else if args.d3 {
        bytemass::render_html(rows)?
    } else {
        bytemass::render_text(rows)?
    };
    print!("{output}");
    Ok(())
}
