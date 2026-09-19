//! Table-format orchestration built on physical Parquet analysis.
//!
//! Enable the `delta` or `iceberg` feature to inspect local table snapshots.

#[cfg(feature = "delta")]
pub mod delta;
#[cfg(feature = "iceberg")]
pub mod iceberg;
