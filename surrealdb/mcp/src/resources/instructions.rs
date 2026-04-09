//! Server instructions resource.

pub fn get_instructions() -> &'static str {
	r#"# SurrealDB MCP Server

This MCP server provides direct access to a SurrealDB database instance.

## Available Tools

### Database Operations
- **query**: Execute raw SurrealQL queries with parameterized inputs ($param syntax)
- **select**: Query records with filters, sorting, and pagination
- **create**: Create new records with content data (bound via $data)
- **insert**: Bulk insert records into a table (bound via $data)
- **upsert**: Create or update records with CONTENT/MERGE/PATCH modes
- **update**: Update existing records with CONTENT/MERGE/PATCH modes
- **delete**: Remove records with optional WHERE clause
- **relate**: Create graph relationships between records

### Schema Introspection
- **info**: View schema information (root/namespace/database/table)
- **list_namespaces**: List all accessible namespaces
- **list_databases**: List databases in the current namespace
- **list_tables**: List tables in the current database
- **describe_table**: Get full schema for a specific table
- **version**: Get SurrealDB version information
- **explain**: Show query execution plan (EXPLAIN/EXPLAIN FULL)

### Context
- **use_namespace**: Switch the active namespace
- **use_database**: Switch the active database

## Available Prompts
- **query_builder**: Guided help building SurrealQL queries
- **schema_explorer**: Explore and understand the database schema
- **data_modeler**: Design tables, fields, and relationships
- **transaction_guide**: Multi-statement transactions with BEGIN/COMMIT
- **graph_traversal**: Graph queries, relationships, and traversals
- **search_guide**: Full-text search with analyzers and indexes

## SurrealQL Quick Reference

SurrealDB uses SurrealQL, supporting:
- Document operations: SELECT, CREATE, INSERT, UPDATE, UPSERT, DELETE
- Graph traversals: RELATE, `->edge->target`, `<-edge<-source` syntax
- Transactions: BEGIN TRANSACTION / COMMIT TRANSACTION / CANCEL TRANSACTION
- Subqueries, parameterized queries ($param syntax)
- Schema definitions: DEFINE TABLE, DEFINE FIELD, DEFINE INDEX, DEFINE EVENT
- Full-text search: DEFINE ANALYZER, DEFINE INDEX ... SEARCH, @@ operator
- Access control: DEFINE ACCESS, PERMISSIONS
- Functions: Built-in (math::, string::, array::, time::, crypto::) and custom (DEFINE FUNCTION)

## Tips
- Always use parameterized queries via the `query` tool for dynamic values
- Use the schema tools to understand the database structure before querying
- Data values in CRUD tools (create, insert, update, upsert, relate) are automatically bound as typed variables
- Graph relationships are created with RELATE and traversed with arrow syntax
- Use `explain` to debug slow queries and check index usage
"#
}
