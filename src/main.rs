use anyhow::Result;
use reqwest::Client;
use rusqlite::{named_params, Connection, OptionalExtension};
use serde_json::Value;
use std::time::Duration;
use tokio::time::sleep;

use crate::types::{MinimalPackageData, RegistryDocument};
use serde::{Deserialize, Serialize};

mod types;

pub fn init_db(conn: &Connection) {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS package (
            id    TEXT PRIMARY KEY,
            content  TEXT NOT NULL
        )",
        (),
    )
    .unwrap();

    conn.execute(
        "CREATE TABLE IF NOT EXISTS last_sync (
            id    TEXT PRIMARY KEY,
            seq   INTEGER NOT NULL
        )",
        (),
    )
    .unwrap();
}

pub fn get_last_seq(conn: &Connection) -> i64 {
    let mut prepared_statement = conn
        .prepare("SELECT id, seq FROM last_sync WHERE id = (:id)")
        .unwrap();

    prepared_statement
        .query_row(named_params! { ":id": "_last" }, |row| {
            Ok(row.get(1).unwrap_or(0))
        })
        .optional()
        .unwrap_or(Some(0))
        .unwrap_or(0)
}

pub fn update_last_seq(conn: &Connection, new_seq: i64) -> Result<usize, rusqlite::Error> {
    let mut prepared_statement = conn
        .prepare("INSERT OR REPLACE INTO last_sync (id, seq) VALUES (:id, :seq)")
        .unwrap();
    prepared_statement.execute(named_params! { ":id": "_last", ":seq": new_seq })
}

pub fn delete_package(conn: &Connection, name: &str) -> Result<usize, rusqlite::Error> {
    let mut prepared_statement = conn
        .prepare("DELETE FROM package WHERE id = (:id)")
        .unwrap();
    prepared_statement.execute(named_params! { ":id": name })
}

pub fn write_package(conn: &Connection, pkg: MinimalPackageData) -> Result<usize, rusqlite::Error> {
    if pkg.versions.len() <= 0 {
        println!("Tried to write pkg {}, but has no versions", pkg.name);
        return delete_package(conn, &pkg.name);
    }

    let mut prepared_statement = conn
        .prepare("INSERT OR REPLACE INTO package (id, content) VALUES (:id, :content)")
        .unwrap();
    prepared_statement.execute(
        named_params! { ":id": pkg.name, ":content": serde_json::to_string(&pkg).unwrap() },
    )
}

pub fn get_package(
    conn: &Connection,
    name: &str,
) -> Result<Option<MinimalPackageData>, rusqlite::Error> {
    let mut prepared_statement = conn
        .prepare("SELECT content FROM package where id = (:id)")
        .unwrap();

    prepared_statement
        .query_row(named_params! { ":id": name }, |row| {
            let content_val: String = row.get(0).unwrap();
            Ok(serde_json::from_str(&content_val).unwrap())
        })
        .optional()
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct Row {
    pub key: Value,
    doc: RegistryDocument,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct AllDocsPage {
    total_rows: i64,
    offset: i64,
    rows: Vec<Row>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct FetchAllQuery {
    limit: usize,
    start_key: Option<String>,
    include_docs: bool,
    skip: usize,
}

async fn fetch_all_docs(
    client: &Client,
    limit: usize,
    start_key: Option<Value>,
    skip: usize,
) -> Result<AllDocsPage> {
    println!("{}, {:?}", limit, start_key);
    let query = FetchAllQuery {
        limit,
        start_key: match start_key.clone() {
            None => None,
            Some(v) => Some(serde_json::to_string(&v)?),
        },
        skip,
        include_docs: true,
    };
    let request = client
        .get("https://replicate.npmjs.com/registry/_all_docs")
        .query(&query);
    println!("{:?}", request);
    let resp: String = request.send().await?.text().await?;
    let decoded: AllDocsPage = serde_json::from_str(&resp)?;
    Ok(decoded)
}

// last sequence before fetching 17565206
#[tokio::main]
async fn main() {
    let db_path = "./registry.db3";
    let conn = Connection::open(db_path).unwrap();
    init_db(&conn);

    let client = reqwest::ClientBuilder::default()
        .gzip(true)
        .build()
        .unwrap();

    let limit: usize = 2;
    let mut total_rows = limit * 2;
    let mut offset: usize = limit;
    let mut start_key: Option<Value> = Some(Value::from("drcd-antd-ui"));

    while offset < total_rows {
        let mut requests = Vec::new();
        for i in 0..1 {
            let skip = i * limit + 1;
            let cloned_client = client.clone();
            let cloned_start_key = start_key.clone();
            let request = tokio::spawn(async move {
                let mut result =
                    fetch_all_docs(&cloned_client, limit, cloned_start_key.clone(), skip).await;
                while let Err(err) = result {
                    println!("{:?}", err);
                    println!("Fetch failed, retrying in 250ms...");
                    sleep(Duration::from_millis(250)).await;
                    println!("Retrying fetch...");
                    result =
                        fetch_all_docs(&cloned_client, limit, cloned_start_key.clone(), skip).await;
                }
                result
            });
            requests.push(request);
        }

        for req in requests {
            println!("Awaiting response for page...");
            let page = req.await.unwrap().unwrap();

            println!("Processing page...");

            for row in page.rows {
                if row.doc.deleted {
                    delete_package(&conn, &row.doc.id).unwrap();
                    println!("Deleted package {}", row.doc.id);
                } else {
                    write_package(&conn, MinimalPackageData::from_doc(row.doc.clone())).unwrap();
                    println!("Wrote package {} to db", row.doc.id);
                }

                start_key = Some(row.key.clone());
            }

            offset = page.offset as usize;
            total_rows = page.total_rows as usize;

            println!("Processed page");
            println!("Fetched {} of {} packages", offset, total_rows);
        }
    }
}
