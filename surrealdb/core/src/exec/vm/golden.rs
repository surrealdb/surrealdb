//! Expected outcomes the unit tests of this module compare against, kept as
//! text beside them in `goldens/`.
//!
//! A test that runs a corpus of statements records the outcome of each under a
//! key and calls [`Golden::check`], which compares the whole record with the
//! file of the same name. A file holds one entry per key, in the order the test
//! records them:
//!
//! ```text
//! ### <key>
//! <body>
//! ```
//!
//! A mismatch reports each entry that differs, with the expected and the actual
//! body. Setting `UPDATE_VM_GOLDENS` rewrites the files from the outcomes the
//! tests record, and the rewritten files are the diff to review.
