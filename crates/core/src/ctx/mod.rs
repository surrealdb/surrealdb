//! This module defines and handles the `context` for the SurrealDB database.
//! ## Concept
//! The context is shared through the code. To understand the context of
//! `context` we must appreciate that there are different layers to the
//! SurrealDB database. Whilst at this point in time there is no definition
//! of all the layers in code, we can illustrate the layers with the following
//! lifecycle of a database request:
//! - we start with an SQL statement
//! - the SQL statement is then parsed into an operation
//! - we then go down to the key value store . . .
//!
//! Here we can see that the database request is handled by different layers.
//! The `context` is the shared state. Each layer can clone the `context` but it
//! must be noted that the values of the `context` are not cloned. A
//! simple example of using the `context` is to keep track of the duration of
//! the request, or if the process has been cancelled or not.

// Copyright 2017 Thomas de Zeeuw
//
// https://docs.rs/io-context/0.2.0/io_context/
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT
// or http://opensource.org/licenses/MIT>, at your option. This file may not be
// used, copied, modified, or distributed except according to those terms.

pub use self::canceller::Canceller;
pub use self::context::{Context, MutableContext};

pub mod cancellation;
pub mod canceller;
pub mod context;
pub mod reason;
