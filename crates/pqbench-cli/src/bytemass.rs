use clap::Args;
use pqbench::bytemass;
use std::num::NonZeroUsize;

use crate::source::read_source_inputs;
use crate::CliError;

/// Arguments for `bytemass`.
#[derive(Args)]
pub(crate) struct BytemassArgs {
    /// parquet paths or glob masks; quote masks to prevent shell expansion
    #[arg(required_unless_present_any = ["source", "collection"], conflicts_with_all = ["source", "collection"])]
    inputs: Vec<String>,
    /// read the inputs from a source document on standard input (`-` only)
    #[arg(long, value_name = "-")]
    source: Option<String>,
    /// nested pqbench.collection JSON from a file or standard input (`-`)
    #[arg(long, conflicts_with = "source")]
    collection: Option<String>,
    /// maximum active tables in a collection
    #[arg(long, default_value = "4", requires = "collection")]
    pub(crate) table_jobs: NonZeroUsize,
    /// maximum simultaneous footer reads shared by all tables
    #[arg(long, default_value = "32", requires = "collection")]
    pub(crate) file_jobs: NonZeroUsize,
    /// save a report at each hierarchy level in a new directory (requires --json or --d3)
    #[arg(long, requires = "collection")]
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
    if let Some(input) = &args.collection {
        return crate::collection::run(input, args);
    }
    let request = bytemass::BytemassRequest {
        inputs: match &args.source {
            Some(source) => read_source_inputs(source)?,
            None => args.inputs.clone(),
        },
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let rows = runtime.block_on(bytemass::bytemass(&request))?;
    let output = if args.json {
        bytemass::render_json(&rows)?
    } else if args.d3 {
        bytemass::render_html(&rows)?
    } else {
        bytemass::render_text(&rows)?
    };
    print!("{output}");
    Ok(())
}
