# SurrealDB Server

The server implementation crate for SurrealDB, containing HTTP/WebSocket server functionality, CLI tooling, and
server-specific features.
This crate should not be used outside of SurrealDB itself.
For a stable interface to the SurrealDB library see [the Rust SDK](https://crates.io/crates/surrealdb)

`surrealdb-server` is the server-side component of SurrealDB that provides:

- **HTTP and WebSocket server endpoints** for database operations
- **Command-line interface (CLI)** for managing and running SurrealDB instances
- **Network layer** including authentication, routing, and middleware
- **Server utilities** for configuration, logging, and monitoring
- **Integration layer** between the core database engine and external interfaces