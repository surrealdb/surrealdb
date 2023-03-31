pub(crate) use self::document::*;

#[cfg(not(target_arch = "wasm32"))]
mod compute;

mod document; // The entry point for a document to be processed

mod create; // Processes a CREATE statement for this document
mod delete; // Processes a DELETE statement for this document
mod insert; // Processes a INSERT statement for this document
mod relate; // Processes a RELATE statement for this document
mod select; // Processes a SELECT statement for this document
mod update; // Processes a UPDATE statement for this document

mod allow; // Checks whether the query can access this document
mod alter; // Modifies and updates the fields in this document
mod check; // Checks whether the WHERE clauses matches this document
mod clean; // Ensures records adhere to the table schema
mod edges; // Attempts to store the edge data for this document
mod empty; // Checks whether the specified document actually exists
mod erase; // Removes all content and field data for this document
mod event; // Processes any table events relevant for this document
mod exist; // Checks whether the specified document actually exists
mod field; // Processes any schema-defined fields for this document
mod index; // Attempts to store the index data for this document
mod lives; // Processes any live queries relevant for this document
mod merge; // Merges any field changes for an INSERT statement
mod pluck; // Pulls the projected expressions from the document
mod purge; // Deletes this document, and any edges or indexes
mod reset; // Resets internal fields which were set for this document
mod store; // Writes the document content to the storage engine
mod table; // Processes any foreign tables relevant for this document
