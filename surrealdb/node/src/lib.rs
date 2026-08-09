//! The `@surrealdb/node-native` NAPI addon: an embedded SurrealDB engine for the
//! JavaScript SDK running on Node.js, Bun, or Deno.
//!
//! The addon exposes a single RPC entry point. A caller encodes an RPC request
//! as CBOR, hands it to [`SurrealNodeEngine::execute`], and receives the
//! CBOR-encoded result — the same request/response shapes the server speaks
//! over WebSocket, so the SDK's engine implementation is a thin transport
//! shim over this crate.

#![recursion_limit = "256"]

mod app;
mod err;
