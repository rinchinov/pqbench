//! One page that embeds the collection-report tree: a layer list and a d3 treemap.

use super::batch::{Collection, TableResult};
use crate::parquet_helpers::Error;

/// Embed the output tree. The treemap loads d3 from a CDN, as `bytemass --d3` does.
///
/// # Errors
/// Returns an error if the tree cannot be serialized.
pub fn render_html(report: &Collection<TableResult>) -> Result<String, Error> {
    let tree = serde_json::to_string(report)?.replace('<', "\\u003c");
    let total = report.tables().len();
    let complete = total - report.failed_tables();
    Ok(include_str!("batch_d3.html")
        .replace(
            "__COVERAGE__",
            &format!("{complete} of {total} tables analyzed. Bytes are compressed column bytes."),
        )
        .replace("__TREE__", &tree))
}
