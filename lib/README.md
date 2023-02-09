# surrealdb

The official SurrealDB library for Rust.

[![](https://img.shields.io/badge/status-beta-ff00bb.svg?style=flat-square)](https://github.com/surrealdb/surrealdb) [![](https://img.shields.io/badge/docs-view-44cc11.svg?style=flat-square)](https://surrealdb.com/docs/integration/libraries/rust) [![](https://img.shields.io/badge/license-Apache_License_2.0-00bfff.svg?style=flat-square)](https://github.com/surrealdb/surrealdb)

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/whatissurreal.svg?raw=true">&nbsp;&nbsp;What is SurrealDB?</h2>

SurrealDB is an end-to-end cloud native database for web, mobile, serverless, jamstack, backend, and traditional applications. SurrealDB reduces the development time of modern applications by simplifying your database and API stack, removing the need for most server-side components, allowing you to build secure, performant apps quicker and cheaper. SurrealDB acts as both a database and a modern, realtime, collaborative API backend layer. SurrealDB can run as a single server or in a highly-available, highly-scalable distributed mode - with support for SQL querying from client devices, GraphQL, ACID transactions, WebSocket connections, structured and unstructured data, graph querying, full-text indexing, geospatial querying, and row-by-row permissions-based access.

View the [features](https://surrealdb.com/features), the latest [releases](https://surrealdb.com/releases), the product [roadmap](https://surrealdb.com/roadmap), and [documentation](https://surrealdb.com/docs).

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/features.svg?raw=true">&nbsp;&nbsp;Features</h2>

- [x] Can be used as an embedded database (`Surreal<Db>`)
- [x] Connects to remote servers (`Surreal<ws::Client>` or `Surreal<http::Client>`)
- [x] Allows picking any protocol or storage engine at run-time (`Surreal<Any>`)
- [x] Compiles to WebAssembly
- [x] Supports typed SQL statements
- [x] Invalid SQL queries are never sent to the server, the client uses the same parser the server uses
- [x] Static clients, no need for `once_cell` or `lazy_static`
- [x] Clonable connections with auto-reconnect capabilities, no need for a connection pool
- [x] Range queries
- [x] Consistent API across all supported protocols and storage engines
- [x] Asynchronous, lock-free connections
- [x] TLS support via either [`rustls`](https://crates.io/crates/rustls) or [`native-tls`](https://crates.io/crates/native-tls)

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/installation.svg?raw=true">&nbsp;&nbsp;Installation</h2>

To add this crate as a Rust dependency, simply run

```bash
cargo add surrealdb
```

**IMPORTANT**: The client supports SurrealDB `v1.0.0-beta.8+20221030.c12a1cc` or later. So please make sure you have that or a newer version of the server before proceeding. For now, that means a recent nightly version.

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/features.svg?raw=true">&nbsp;&nbsp;Quick look</h2>

This library enables simple and advanced querying of an embedded or remote database from server-side or client-side (via Wasm) code. By default, all remote connections to SurrealDB are made over WebSockets, and automatically reconnect when the connection is terminated. Connections are automatically closed when they get dropped.

```rust
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::borrow::Cow;
use surrealdb::sql;
use surrealdb::Surreal;
use surrealdb::engine::remote::ws::Ws;
use surrealdb::opt::auth::Root;

#[derive(Serialize, Deserialize)]
struct Name {
    first: Cow<'static, str>,
    last: Cow<'static, str>,
}

#[derive(Serialize, Deserialize)]
struct Person {
    #[serde(skip_serializing)]
    id: Option<String>,
    title: Cow<'static, str>,
    name: Name,
    marketing: bool,
}

#[tokio::main]
async fn main() -> surrealdb::Result<()> {
    let db = Surreal::new::<Ws>("localhost:8000").await?;

    // Signin as a namespace, database, or root user
    db.signin(Root {
        username: "root",
        password: "root",
    })
    .await?;

    // Select a specific namespace and database
    db.use_ns("namespace").use_db("database").await?;

    // Create a new person with a random ID
    let tobie: Person = db
        .create("person")
        .content(Person {
            id: None,
            title: "Founder & CEO".into(),
            name: Name {
                first: "Tobie".into(),
                last: "Morgan Hitchcock".into(),
            },
            marketing: true,
        })
        .await?;

    assert!(tobie.id.is_some());

    // Create a new person with a specific ID
    let mut jaime: Person = db
        .create(("person", "jaime"))
        .content(Person {
            id: None,
            title: "Founder & COO".into(),
            name: Name {
                first: "Jaime".into(),
                last: "Morgan Hitchcock".into(),
            },
            marketing: false,
        })
        .await?;

    assert_eq!(jaime.id.unwrap(), "person:jaime");

    // Update a person record with a specific ID
    jaime = db
        .update(("person", "jaime"))
        .merge(json!({ "marketing": true }))
        .await?;

    assert!(jaime.marketing);

    // Select all people records
    let people: Vec<Person> = db.select("person").await?;

    assert!(!people.is_empty());

    // Perform a custom advanced query
    let sql = r#"
        SELECT marketing, count()
        FROM type::table($table)
        GROUP BY marketing
    "#;

    let groups = db.query(sql)
        .bind(("table", "person"))
        .await?;

    dbg!(groups);

    // Delete all people upto but not including Jaime
    db.delete("person").range(.."jaime").await?;

    // Delete all people
    db.delete("person").await?;

    Ok(())
}
```
