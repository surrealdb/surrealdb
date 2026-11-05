# surrealdb-idx

The SurrealDB index engines: the structures that back a `DEFINE INDEX`, and the
reads and writes that maintain them. The full-text index and its analyzer, the
HNSW and DiskANN vector graphs, the count index, the unique/non-unique entry
index, and the document-id mapping they all share.

What is *not* here is the decision to use an index. Choosing an index for a
query, and iterating one on behalf of a statement, is the planner's job and
lives one layer up with the executor that calls it. These engines are reached
downward, through a transaction and an index environment handle; they never
reach back.

**This crate is an internal implementation detail of SurrealDB.** Its API is
unstable and changes without notice; depend on the `surrealdb` SDK or
`surrealdb-core` instead.
