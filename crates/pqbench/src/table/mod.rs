//! Table-format orchestration built on physical Parquet analysis.
//!
//! Enable the `delta` feature to inspect local and object-store Delta snapshots.
//! Enable the `iceberg` feature to inspect local Iceberg snapshots. The Delta
//! feature requires Rust 1.91.1 or newer because of its snapshot dependencies.

#[cfg(feature = "delta")]
pub mod delta;
#[cfg(feature = "iceberg")]
pub mod iceberg;
