//! Write the output tree, or the HTML page that parses it.

use std::path::Path;

use super::batch::{render_html, render_json, Collection, TableResult};
use crate::parquet_helpers::Error;

#[derive(Clone, Copy)]
pub enum ReportFormat {
    Json,
    Html,
}

/// Save the output tree (`index.json`) or the page that embeds it (`index.html`).
/// The directory must not already exist.
///
/// # Errors
/// Returns serialization or filesystem errors.
pub fn save_reports(
    report: &Collection<TableResult>,
    directory: &Path,
    format: ReportFormat,
) -> Result<(), Error> {
    std::fs::create_dir(directory).map_err(io_error)?;
    let (filename, output) = match format {
        ReportFormat::Json => ("index.json", render_json(report)?),
        ReportFormat::Html => ("index.html", render_html(report)?),
    };
    std::fs::write(directory.join(filename), output).map_err(io_error)
}

fn io_error(error: std::io::Error) -> Error {
    Error(format!("cannot save report: {error}"))
}
