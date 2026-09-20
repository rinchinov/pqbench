use std::process::ExitCode;

use clap::{Parser, Subcommand};

mod bench;
mod bytemass;
mod compression;
mod lz;
mod source;

#[cfg(feature = "delta")]
mod delta;
#[cfg(feature = "ducklake")]
mod ducklake;
#[cfg(feature = "iceberg")]
mod iceberg;

/// The CLI's single error channel: any error from the io, parquet, or codec
/// layers, converted via `?`.
pub(crate) type CliError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser)]
#[command(
    name = "pqbench",
    about = "lzbench for parquet",
    after_help = r#"
Examples:
  pqbench lz file.bin -c zstd@3 --samples 10
  pqbench compression data.parquet --per-column
  pqbench bytemass data.parquet
  pqbench bytemass part-1.parquet part-2.parquet
  pqbench bytemass 'data/*.parquet'
  producer | pqbench bytemass --source -
  pqbench bytemass data.parquet --d3 > treemap.html && xdg-open treemap.html
"#
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// lzbench-style compression benchmark over raw file bytes
    Lz(bench::BenchArgs),
    /// lzbench-style codec sweep over encoded parquet pages (NONE-compressed input)
    Compression(compression::CompressionArgs),
    /// export per-column byte masses (on-disk bytes/row)
    #[command(after_help = r#"
Examples:
  pqbench bytemass data.parquet
  producer | pqbench bytemass --source -
  pqbench bytemass data.parquet --d3 > treemap.html && xdg-open treemap.html
"#)]
    Bytemass(bytemass::BytemassArgs),
    /// analyze the active Parquet files in a local Delta snapshot
    #[cfg(feature = "delta")]
    #[command(after_help = r#"Examples:
  pqbench delta ./table --json
  producer | pqbench delta --source -
"#)]
    Delta(delta::DeltaArgs),
    /// analyze the active Parquet files in an Iceberg snapshot
    #[cfg(feature = "iceberg")]
    #[command(after_help = r#"Examples:
  pqbench iceberg table/metadata/v2.metadata.json --json
  producer | pqbench iceberg --source -
"#)]
    Iceberg(iceberg::IcebergArgs),
    /// analyze the active Parquet files in a DuckLake snapshot
    #[cfg(feature = "ducklake")]
    #[command(after_help = r#"Examples:
  pqbench ducklake catalog.sqlite --table events --json
  producer | pqbench ducklake --source - --table events
"#)]
    Ducklake(ducklake::DuckLakeArgs),
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Lz(args) => lz::run(&args),
        Command::Compression(args) => compression::run(&args),
        Command::Bytemass(args) => bytemass::run(&args),
        #[cfg(feature = "delta")]
        Command::Delta(args) => delta::run(&args),
        #[cfg(feature = "iceberg")]
        Command::Iceberg(args) => iceberg::run(&args),
        #[cfg(feature = "ducklake")]
        Command::Ducklake(args) => ducklake::run(&args),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
