//! Table-format orchestration built on physical Parquet analysis.
//!
//! Enable `delta`, `iceberg`, or `ducklake` to inspect a snapshot of that
//! format. The `delta` feature requires Rust 1.91.1 or newer because of the
//! Delta snapshot dependencies.

#[cfg(feature = "delta")]
pub mod delta;
#[cfg(feature = "ducklake")]
pub mod ducklake;
#[cfg(feature = "iceberg")]
pub mod iceberg;
