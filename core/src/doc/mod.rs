//! This module defines the lifecycle of everything that happens in a document.
//! A document is a row that has the following:
//! - `Thing`: name of the table and ID of the record
//! - `current`: value after the transaction
//! - `initial`: value before the transaction
//! - `id`: traditionally an integer but can be an object or collection such as an array
pub(crate) use self::document::*;

mod document; // The entry point for a document to be processed

#[cfg(not(target_arch = "wasm32"))]
mod compute; // The point at which a document is processed
mod process; // The point at which a document is processed

mod create; // Processes a CREATE statement for this document
mod delete; // Processes a DELETE statement for this document
mod insert; // Processes a INSERT statement for this document
mod relate; // Processes a RELATE statement for this document
mod select; // Processes a SELECT statement for this document
mod update; // Processes a UPDATE statement for this document

mod allow; // Checks whether the query can access this document
mod alter; // Modifies and updates the fields in this document
mod changefeeds; // Processes any change feeds relevant for this document
mod check; // Checks whether the WHERE clauses matches this document
mod clean; // Ensures records adhere to the table schema
mod edges; // Attempts to store the edge data for this document
mod empty; // Checks whether the specified document actually exists
mod erase; // Removes all content and field data for this document
mod event; // Processes any table events relevant for this document
mod field; // Processes any schema-defined fields for this document
mod index; // Attempts to store the index data for this document
mod lives; // Processes any live queries relevant for this document
mod merge; // Merges any field changes for an INSERT statement
mod pluck; // Pulls the projected expressions from the document
mod purge; // Deletes this document, and any edges or indexes
mod relation; // Checks whether the record is the right kind for the table
mod reset; // Resets internal fields which were set for this document
mod store; // Writes the document content to the storage engine
mod table; // Processes any foreign tables relevant for this document
