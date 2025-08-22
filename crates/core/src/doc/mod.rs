//! This module defines the lifecycle of everything that happens in a document.
//! A document is a row that has the following:
//! - `Thing`: name of the table and ID of the record
//! - `current`: value after the transaction
//! - `initial`: value before the transaction
//! - `id`: traditionally an integer but can be an object or collection such as an array

pub(crate) use self::document::*;

mod document; // The entry point for a document to be processed

mod process; // The point at which a document is processed

mod create; // Processes a CREATE statement for this document
mod delete; // Processes a DELETE statement for this document
mod insert; // Processes a INSERT statement for this document
mod relate; // Processes a RELATE statement for this document
mod select; // Processes a SELECT statement for this document
mod update; // Processes a UPDATE statement for this document
mod upsert; // Processes a UPSERT statement for this document

mod alter; // Modifies and updates the fields in this document
mod changefeeds; // Processes any change feeds relevant for this document
mod check; // Data and condition checking for this document
mod compute; // Compute computed fields for this document
mod edges; // Attempts to store the edge data for this document
mod event; // Processes any table events relevant for this document
mod field; // Processes any schema-defined fields for this document
mod index; // Attempts to store the index data for this document
mod lives; // Processes any live queries relevant for this document
mod pluck; // Pulls the projected expressions from the document
mod purge; // Deletes this document, and any edges or indexes
mod store; // Writes the document content to the storage engine
mod table; // Processes any foreign tables relevant for this document'

/// Error result used when a function can result in the value being processed
/// being ignored.
#[derive(Debug)]
pub enum IgnoreError {
	Ignore,
	Error(anyhow::Error),
}

impl From<anyhow::Error> for IgnoreError {
	fn from(value: anyhow::Error) -> Self {
		IgnoreError::Error(value)
	}
}
