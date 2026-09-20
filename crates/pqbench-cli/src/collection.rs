//! Thin CLI adapter for nested table analysis.

use pqbench::bytemass::batch::{self, Collection, Outcome, Table};

use crate::bytemass::BytemassArgs;
use crate::CliError;

pub(super) fn run(input: &str, args: &BytemassArgs) -> Result<(), CliError> {
    if let Some(directory) = &args.output_dir {
        if !args.json && !args.d3 {
            return Err("--output-dir requires --json or --d3".into());
        }
        if directory.exists() {
            return Err("--output-dir must name a new directory".into());
        }
    }
    let reader: Box<dyn std::io::Read> = if input == "-" {
        Box::new(std::io::stdin().lock())
    } else {
        Box::new(std::fs::File::open(input)?)
    };
    // Serde errors may quote invalid values, including storage options.
    let collection: Collection<Table> = serde_json::from_reader(reader).map_err(|e| {
        format!(
            "invalid collection document at line {}, column {}",
            e.line(),
            e.column()
        )
    })?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let report = runtime.block_on(batch::analyze(collection, args.table_jobs, args.file_jobs))?;
    let failed = report.failed_tables();
    if let Some(directory) = &args.output_dir {
        let format = if args.d3 {
            batch::ReportFormat::Html
        } else {
            batch::ReportFormat::Json
        };
        batch::save_reports(&report, directory, format)?;
        eprintln!("Saved reports to {}", directory.display());
        if failed > 0 {
            return Err(
                format!("{failed} table(s) failed; saved reports contain their errors").into(),
            );
        }
        return Ok(());
    }
    let output = if args.d3 {
        batch::render_html(&report)?
    } else if args.json {
        batch::render_json(&report)?
    } else {
        let mut output = String::new();
        for table in report.tables() {
            match &table.outcome {
                Outcome::Complete { analysis } => {
                    output.push_str(&format!(
                        "{}: {} files, {} physical rows, {} compressed column bytes\n",
                        table.name,
                        analysis.file_count,
                        analysis.physical_rows,
                        analysis.compressed_column_bytes
                    ));
                }
                Outcome::Failed { error } => {
                    output.push_str(&format!("{}: FAILED: {error}\n", table.name))
                }
            }
        }
        output.push_str(&format!(
            "{} of {} tables analyzed\n",
            report.tables().len() - failed,
            report.tables().len()
        ));
        output
    };
    println!("{output}");
    if failed > 0 {
        return Err(format!(
            "{failed} table(s) failed; report contains successful tables and errors"
        )
        .into());
    }
    Ok(())
}
