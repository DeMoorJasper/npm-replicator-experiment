use reqwest::Client;
use rusqlite::{named_params, Connection, OptionalExtension};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DocumentPackageDist {
    tarball: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DocumentPackageVersion {
    #[serde(default)]
    dependencies: HashMap<String, String>,
    #[serde(default, rename = "optionalDependencies")]
    optional_dependencies: HashMap<String, String>,
    dist: DocumentPackageDist,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct RegistryDocument {
    #[serde(rename = "_id")]
    id: String,

    #[serde(default)]
    #[serde(rename = "_deleted")]
    deleted: bool,

    #[serde(default)]
    #[serde(rename = "dist-tags")]
    dist_tags: HashMap<String, String>,

    #[serde(default)]
    versions: BTreeMap<String, DocumentPackageVersion>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MinimalPackageVersionData {
    pub tarball: String,
    pub dependencies: HashMap<String, String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MinimalPackageData {
    pub name: String,
    pub dist_tags: HashMap<String, String>,
    pub versions: BTreeMap<String, MinimalPackageVersionData>,
}

impl MinimalPackageData {
    pub fn from_doc(raw: RegistryDocument) -> MinimalPackageData {
        let mut data = MinimalPackageData {
            name: raw.id,
            dist_tags: raw.dist_tags,
            versions: BTreeMap::new(),
        };
        for (key, value) in raw.versions {
            let mut dependencies = value.dependencies;
            for (name, _version) in value.optional_dependencies {
                dependencies.remove(&name);
            }
            data.versions.insert(
                key,
                MinimalPackageVersionData {
                    tarball: value.dist.tarball,
                    dependencies,
                },
            );
        }
        data
    }
}

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
    skip: i64,
}

async fn fetch_all_docs(
    client: Client,
    limit: usize,
    start_key: Option<Value>,
) -> (Client, AllDocsPage) {
    println!("{}, {:?}", limit, start_key);
    let query = FetchAllQuery {
        limit,
        start_key: match start_key.clone() {
            None => None,
            Some(v) => Some(serde_json::to_string(&v).unwrap()),
        },
        skip: match start_key {
            None => 0,
            Some(_v) => 1,
        },
        include_docs: true,
    };
    let request = client
        .get("https://replicate.npmjs.com/registry/_all_docs")
        .query(&query);
    println!("{:?}", request);
    let resp: String = request.send().await.unwrap().text().await.unwrap();
    let decoded: AllDocsPage = serde_json::from_str(&resp).unwrap();
    (client, decoded)
}

#[tokio::main]
async fn main() {
    let db_path = "./registry.db3";
    let conn = Connection::open(db_path).unwrap();
    init_db(&conn);

    let mut client = reqwest::ClientBuilder::default()
        .gzip(true)
        .build()
        .unwrap();

    let limit: usize = 25;
    let mut last_count: usize = limit;
    let mut start_key: Option<Value> = Some(Value::from("1806a--onetwo"));
    while last_count >= limit {
        let (new_client, docs) = fetch_all_docs(client, limit, start_key.clone()).await;

        for row in docs.rows.iter() {
            if row.doc.deleted {
                delete_package(&conn, &row.doc.id).unwrap();
                println!("Deleted package {}", row.doc.id);
            } else {
                write_package(&conn, MinimalPackageData::from_doc(row.doc.clone())).unwrap();
                println!("Wrote package {} to db", row.doc.id);
            }
            start_key = Some(row.key.clone());
        }

        client = new_client;
        last_count = docs.rows.len();

        println!("Fetched {} of {} packages", docs.offset, docs.total_rows);
    }

    // Sync using the change stream
    // let client = couch_rs::Client::new_no_auth("https://replicate.npmjs.com")?;
    // let npm_db = client.db("registry").await?;

    // last sequence before fetching 17565206
    // let last_seq: i64 = get_last_seq(&conn);
    // println!("Last synced sequence {}", last_seq);
    // let mut stream = npm_db.changes(Some(last_seq.into()));
    // stream.set_infinite(true);

    // while let Some(v) = stream.next().await {
    //     if let Ok(change) = v {
    //         if let Some(doc) = change.doc {
    //             let parsed: RegistryDocument = serde_json::from_value(doc).unwrap();

    //             if parsed.deleted {
    //                 delete_package(&conn, &parsed.id).unwrap();
    //                 println!("Deleted package {}", parsed.id);
    //             } else {
    //                 write_package(&conn, MinimalPackageData::from_doc(parsed.clone())).unwrap();
    //                 println!("Wrote package {} to db", parsed.id);
    //             }
    //         }

    //         let last_seq: i64 = serde_json::from_value(change.seq).unwrap();
    //         update_last_seq(&conn, last_seq).unwrap();
    //     }
    // }
}
