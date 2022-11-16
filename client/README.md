# surrealdb.rs

The official SurrealDB library for Rust.

[![](https://img.shields.io/badge/status-beta-ff00bb.svg?style=flat-square)](https://github.com/surrealdb/surrealdb) [![](https://img.shields.io/badge/docs-view-44cc11.svg?style=flat-square)](https://surrealdb.com/docs/integration/libraries/rust) [![](https://img.shields.io/badge/license-Apache_License_2.0-00bfff.svg?style=flat-square)](https://github.com/surrealdb/surrealdb)

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/whatissurreal.svg?raw=true">&nbsp;&nbsp;What is SurrealDB?</h2>

SurrealDB is an end-to-end cloud native database for web, mobile, serverless, jamstack, backend, and traditional applications. SurrealDB reduces the development time of modern applications by simplifying your database and API stack, removing the need for most server-side components, allowing you to build secure, performant apps quicker and cheaper. SurrealDB acts as both a database and a modern, realtime, collaborative API backend layer. SurrealDB can run as a single server or in a highly-available, highly-scalable distributed mode - with support for SQL querying from client devices, GraphQL, ACID transactions, WebSocket connections, structured and unstructured data, graph querying, full-text indexing, geospatial querying, and row-by-row permissions-based access.

View the [features](https://surrealdb.com/features), the latest [releases](https://surrealdb.com/releases), the product [roadmap](https://surrealdb.com/roadmap), and [documentation](https://surrealdb.com/docs).

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/features.svg?raw=true">&nbsp;&nbsp;Features</h2>

- [x] WebSocket connections
- [x] HTTP connections
- [x] Compiles to WebAssembly
- [x] Supports typed SQL statements
- [x] Invalid SQL queries are never sent to the server, the client uses the same parser the server uses
- [x] Static clients, no need for `once_cell` or `lazy_static`
- [x] Clonable connections with auto-reconnect capabilities, no need for a connection pool
- [x] Range queries
- [x] Consistent API across all supported protocols, just change the scheme on the `connect` method and you are good to go
- [x] Asynchronous, lock-free connections
- [x] TLS support via either [`rustls`](https://crates.io/crates/rustls) or [`native-tls`](https://crates.io/crates/native-tls)
- [ ] FFI bindings for third-party languages

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/installation.svg?raw=true">&nbsp;&nbsp;Installation</h2>

To add this crate as a Rust dependency, simply run

```bash
cargo add surrealdb-rs
```

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/features.svg?raw=true">&nbsp;&nbsp;Quick look</h2>

This library enables simple and advanced querying of a remote database from server-side and client-side (via Wasm) code. By default, all connections to SurrealDB are made over WebSockets, and automatically reconnect when the connection is terminated. Connections are automatically closed when they get dropped.

```rust
use serde::{Serialize, Deserialize};
use serde_json::json;
use std::borrow::Cow;
use surrealdb_rs::{Result, Surreal};
use surrealdb_rs::param::Root;
use surrealdb_rs::protocol::Ws;

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
async fn main() -> Result<()> {
    let client = Surreal::connect::<Ws>("localhost:8000").await?;

    // Signin as a namespace, database, or root user
    client.signin(Root {
        username: "root",
        password: "root",
    }).await?;

    // Select a specific namespace and database
    client.use_ns("test").use_db("test").await?;

    // Create a new person with a random ID
    let tobie: Person = client.create("person")
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

    // Create a new person with a specific ID
    let mut jaime: Person = client.create(("person", "jaime"))
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

    // Update a person record with a specific ID
    jaime = client.update(("person", "jaime"))
        .merge(json!({"marketing": true}))
        .await?;

    // Select all people records
    let people: Vec<Person> = client.select("person").await?;

    // Perform a custom advanced query
    let groups = client
        .query("SELECT marketing, count() FROM type::table($table) GROUP BY marketing")
        .bind("table", "person")
        .await?;

    // Delete all people upto but not including Jaime
    client.delete("person").range(.."jaime").await?;

    Ok(())
}
```
