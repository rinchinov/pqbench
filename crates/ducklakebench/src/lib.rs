//! Compatibility facade for the DuckLake table analysis module.
//!
//! The implementation lives in [`pqbench::table::ducklake`] alongside the
//! Delta table API. This crate keeps the requested standalone library name for
//! callers and for the `ducklakebench` CLI.

pub use pqbench::table::ducklake::*;
