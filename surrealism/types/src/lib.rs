//! # surrealism-types
//!
//! A language-agnostic serialization framework for WebAssembly (WASM) guest-host communication.
//!
//! ## Overview
//!
//! This crate provides the core types and traits for transferring data across WASM boundaries
//! in a way that can be implemented by any language that compiles to WebAssembly. It defines:
//!
//! - A binary serialization protocol ([`Serializable`](serialize::Serializable))
//! - Memory transfer abstractions ([`Transfer`](transfer::Transfer), [`AsyncTransfer`](transfer::AsyncTransfer))
//! - Memory management interfaces ([`MemoryController`](controller::MemoryController), [`AsyncMemoryController`](controller::AsyncMemoryController))
//! - Function argument marshalling ([`Args`](args::Args))
//!
//! ## Feature Flags
//!
//! - `host`: Enables async traits for host-side (runtime) implementations. Without this flag, all
//!   operations are synchronous, suitable for WASM guest modules.
//!
//! ## Dual-Mode Architecture
//!
//! The crate supports both synchronous (guest) and asynchronous (host) operations:
//!
//! - **Guest mode** (default): Synchronous traits for WASM module code
//! - **Host mode** (`host` feature): Async traits for runtime/Wasmtime code
//!
//! This allows the same types to work efficiently on both sides of the WASM boundary.
//!
//! ## Example
//!
//! ```rust,ignore
//! use surrealism_types::{Serializable, Transfer, MemoryController};
//!
//! // Guest side: Transfer a string to host
//! fn send_string(s: String, controller: &mut dyn MemoryController) -> u32 {
//!     let ptr = s.transfer(controller).unwrap();
//!     *ptr
//! }
//!
//! // Guest side: Receive a string from host
//! fn receive_string(ptr: u32, controller: &mut dyn MemoryController) -> String {
//!     String::receive(ptr.into(), controller).unwrap()
//! }
//! ```
//!
/// Wrapper type for function arguments that implement [`surrealdb_types::SurrealValue`].
pub mod arg;

/// Traits for marshalling function arguments to and from [`surrealdb_types::Value`] vectors.
pub mod args;

/// Memory management abstractions for WASM linear memory allocation and deallocation.
pub mod controller;

/// Error handling utilities for adding context to errors.
pub mod err;

/// Core serialization traits and implementations for the binary wire format.
pub mod serialize;

/// Memory transfer traits for moving data across WASM boundaries.
pub mod transfer;
