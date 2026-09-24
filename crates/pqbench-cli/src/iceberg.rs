//! List tables from an Iceberg REST catalog.
//!
//! The catalog is the Iceberg REST fixture dialect used by the lakehouse
//! stand: `GET /v1/config`, `GET /v1/namespaces`, `GET /v1/namespaces/{ns}/tables`,
//! then `GET /v1/namespaces/{ns}/tables/{table}` for the metadata location.
//! Listing is sequential: the caller runs one table per later process.
//! `--include` / `--exclude` prune the walk when the leading name is a literal.
//! https://iceberg.apache.org/docs/latest/rest-catalog-spec/

use std::collections::{BTreeMap, BTreeSet};

use pqbench::lake::LakeTable;
use serde::Deserialize;

use crate::catalog::{self, PAGE_CAP};
use crate::document::LakeSource;
use crate::filter::NameFilter;
use crate::CliError;

#[derive(Deserialize)]
struct NamespacesPage {
    namespaces: Vec<Vec<String>>,
    #[serde(default, rename = "next-page-token", alias = "nextPageToken")]
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
struct TablesPage {
    identifiers: Vec<Identifier>,
    #[serde(default, rename = "next-page-token", alias = "nextPageToken")]
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
struct Identifier {
    name: String,
}

#[derive(Deserialize)]
struct LoadedTable {
    #[serde(rename = "metadata-location")]
    metadata_location: String,
}

/// List Iceberg tables, sending each to `on_table` as it is found.
pub(crate) fn list_tables(
    source: &LakeSource,
    filter: &NameFilter,
    mut on_table: impl FnMut(LakeTable) -> Result<(), CliError>,
) -> Result<usize, CliError> {
    let root = source.endpoint.trim_end_matches('/').to_string();
    let token = source.token.clone().filter(|token| !token.is_empty());
    let mut tables = 0usize;
    for namespace in list_namespaces(&root, token.as_deref(), filter)? {
        tables += list_namespace_tables(
            &root,
            token.as_deref(),
            &namespace,
            &source.env,
            filter,
            &mut on_table,
        )?;
    }
    if tables == 0 {
        return Err("catalog listed no Iceberg tables".into());
    }
    Ok(tables)
}

fn list_namespaces(
    root: &str,
    token: Option<&str>,
    filter: &NameFilter,
) -> Result<Vec<Vec<String>>, CliError> {
    let mut namespaces = Vec::new();
    for page in pages::<NamespacesPage>(root, token, "/v1/namespaces", |page| {
        page_token(&page.next_page_token)
    })? {
        for parts in page.namespaces {
            if parts.is_empty() || parts.iter().any(|part| part.is_empty()) {
                return Err("catalog /v1/namespaces listed a nameless namespace".into());
            }
            if filter.keeps_prefix(&parts.join(".")) {
                namespaces.push(parts);
            }
        }
    }
    Ok(namespaces)
}

fn list_namespace_tables(
    root: &str,
    token: Option<&str>,
    namespace: &[String],
    env: &BTreeMap<String, String>,
    filter: &NameFilter,
    on_table: &mut impl FnMut(LakeTable) -> Result<(), CliError>,
) -> Result<usize, CliError> {
    let encoded = encoded_namespace(namespace);
    let mut tables = 0usize;
    for page in pages::<TablesPage>(
        root,
        token,
        &format!("/v1/namespaces/{encoded}/tables"),
        |page| page_token(&page.next_page_token),
    )? {
        for item in page.identifiers {
            if item.name.is_empty() {
                return Err(format!(
                    "catalog listed a nameless table in namespace {}",
                    namespace.join(".")
                )
                .into());
            }
            let table = load_table(root, token, namespace, &item.name, env)?;
            if filter.keeps(&table.name) {
                on_table(table)?;
                tables += 1;
            }
        }
    }
    Ok(tables)
}

fn load_table(
    root: &str,
    token: Option<&str>,
    namespace: &[String],
    name: &str,
    env: &BTreeMap<String, String>,
) -> Result<LakeTable, CliError> {
    let url = format!(
        "{root}/v1/namespaces/{}/tables/{}",
        encoded_namespace(namespace),
        catalog::encode(name)
    );
    let loaded: LoadedTable = catalog::get_json(&url, token)?;
    if loaded.metadata_location.is_empty() {
        return Err(format!(
            "Iceberg table {}.{} is missing metadata-location",
            namespace.join("."),
            name
        )
        .into());
    }
    let full_name = namespace
        .iter()
        .cloned()
        .chain(std::iter::once(name.to_string()))
        .collect::<Vec<_>>()
        .join(".");
    Ok(LakeTable {
        name: full_name,
        uri: loaded.metadata_location,
        env: env.clone(),
        info: None,
    })
}

fn encoded_namespace(namespace: &[String]) -> String {
    catalog::encode(&namespace.join("\u{1f}"))
}

fn page_token(token: &Option<String>) -> Option<String> {
    token
        .as_deref()
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
}

fn pages<P: for<'de> Deserialize<'de>>(
    root: &str,
    token: Option<&str>,
    path: &str,
    next: fn(&P) -> Option<String>,
) -> Result<Vec<P>, CliError> {
    let mut page_token: Option<String> = None;
    let mut seen = BTreeSet::new();
    let mut pages = Vec::new();
    loop {
        if pages.len() >= PAGE_CAP {
            return Err(format!("catalog listed more than {PAGE_CAP} pages at {path}").into());
        }
        let mut url = format!("{root}{path}");
        if let Some(token) = &page_token {
            url.push_str("?pageToken=");
            url.push_str(&catalog::encode(token));
        }
        let page: P = catalog::get_json(&url, token)?;
        let next = next(&page);
        pages.push(page);
        let Some(next) = next else {
            return Ok(pages);
        };
        if !seen.insert(next.clone()) {
            return Err(format!("catalog repeated page token at {path}").into());
        }
        page_token = Some(next);
    }
}
