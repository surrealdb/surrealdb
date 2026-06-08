//! MCP prompt templates for common SurrealDB tasks.

pub mod query_builder;
pub mod schema_explorer;

use rmcp::model::{GetPromptResult, Prompt, PromptArgument, PromptMessage, PromptMessageRole};

/// List all available prompts.
pub fn list_prompts() -> Vec<Prompt> {
	vec![
		Prompt::new(
			"query_builder",
			Some("Build a SurrealQL query from a natural language description"),
			Some(vec![
				PromptArgument::new("description")
					.with_description("Natural language description of what you want to query")
					.with_required(true),
			]),
		),
		Prompt::new(
			"schema_explorer",
			Some("Explore and understand the database schema"),
			Some(vec![
				PromptArgument::new("focus")
					.with_description("Optional focus area: a table name, or 'all' for full schema")
					.with_required(false),
			]),
		),
		Prompt::new(
			"data_modeler",
			Some("Help design tables, fields, and relationships for a data model"),
			Some(vec![
				PromptArgument::new("requirements")
					.with_description("Description of the data model requirements")
					.with_required(true),
			]),
		),
		Prompt::new(
			"transaction_guide",
			Some("Guide for multi-statement transactions using BEGIN/COMMIT/CANCEL"),
			Some(vec![
				PromptArgument::new("goal")
					.with_description("What you want to accomplish atomically")
					.with_required(true),
			]),
		),
		Prompt::new(
			"graph_traversal",
			Some("Guide for graph queries, relationships, and traversals in SurrealDB"),
			Some(vec![
				PromptArgument::new("scenario")
					.with_description("Description of the graph relationship or traversal needed")
					.with_required(true),
			]),
		),
		Prompt::new(
			"search_guide",
			Some("Guide for full-text search with analyzers, indexes, and the @@ operator"),
			Some(vec![
				PromptArgument::new("use_case")
					.with_description("What you want to search for and how")
					.with_required(true),
			]),
		),
	]
}

/// Get a specific prompt by name.
pub fn get_prompt(name: &str, arguments: &serde_json::Value) -> Option<GetPromptResult> {
	// Missing or non-string args render as an empty placeholder in the
	// prompt template; required-ness is enforced by the MCP client schema.
	let arg = |key: &str| arguments.get(key).and_then(|v| v.as_str()).unwrap_or("");

	match name {
		"query_builder" => Some(query_builder::get_prompt(arguments)),
		"schema_explorer" => Some(schema_explorer::get_prompt(arguments)),
		"data_modeler" => Some(get_data_modeler_prompt(arg("requirements"))),
		"transaction_guide" => Some(get_transaction_prompt(arg("goal"))),
		"graph_traversal" => Some(get_graph_prompt(arg("scenario"))),
		"search_guide" => Some(get_search_prompt(arg("use_case"))),
		_ => None,
	}
}

fn get_data_modeler_prompt(requirements: &str) -> GetPromptResult {
	GetPromptResult::new(vec![PromptMessage::new_text(
		PromptMessageRole::User,
		format!(
			r#"Help me design a SurrealDB data model for: {requirements}

Please:
1. Suggest table definitions with appropriate fields and types
2. Add indexes for common query patterns
3. Use graph relationships (RELATE) where appropriate
4. Include field-level permissions if applicable
5. Provide complete SurrealQL DEFINE statements

Use the `query` tool to execute the DEFINE statements. Then use `info` with target=<table> to verify each table's schema.

SurrealDB capabilities to consider:
- Document fields for structured data
- Graph edges (RELATE) for relationships
- Record links for references
- Computed fields (VALUE clause)
- Changefeeds for auditing"#
		),
	)])
	.with_description("Data modeling assistant for SurrealDB")
}

fn get_transaction_prompt(goal: &str) -> GetPromptResult {
	GetPromptResult::new(vec![PromptMessage::new_text(
		PromptMessageRole::User,
		format!(
			r#"Help me write a SurrealDB transaction to: {goal}

Use the `query` tool with a multi-statement transaction. SurrealQL transaction syntax:

BEGIN TRANSACTION;
-- your statements here
COMMIT TRANSACTION;

Or to roll back on any error:
BEGIN TRANSACTION;
-- statements
CANCEL TRANSACTION;

Key rules:
- Wrap related mutations in a single transaction for atomicity
- Use THROW to abort with a custom error message
- Use $param syntax for variable binding
- Transactions work with SELECT, CREATE, UPDATE, DELETE, RELATE"#
		),
	)])
	.with_description("Transaction guide for SurrealDB")
}

fn get_graph_prompt(scenario: &str) -> GetPromptResult {
	GetPromptResult::new(vec![PromptMessage::new_text(
		PromptMessageRole::User,
		format!(
			r#"Help me with graph operations in SurrealDB for: {scenario}

First use `list` with kind='tables' and `info` with target=<table> to understand existing schema.

SurrealDB graph syntax:
- Create edges: RELATE person:john->knows->person:bob CONTENT {{ since: time::now() }}
- Traverse outbound: SELECT ->knows->person FROM person:john
- Traverse inbound: SELECT <-knows<-person FROM person:bob
- Multi-hop: SELECT ->knows->person->lives_in->city FROM person:john
- With FETCH: SELECT * FROM person FETCH ->knows->person
- Filter edges: SELECT ->knows[WHERE since > '2024-01-01']->person FROM person:john

Graph patterns:
- Social networks: person->follows->person, person->likes->post
- Hierarchies: employee->reports_to->employee
- Access control: user->has_role->role->has_permission->permission"#
		),
	)])
	.with_description("Graph traversal guide for SurrealDB")
}

fn get_search_prompt(use_case: &str) -> GetPromptResult {
	GetPromptResult::new(vec![PromptMessage::new_text(
		PromptMessageRole::User,
		format!(
			r#"Help me set up full-text search in SurrealDB for: {use_case}

Use the `query` tool to execute these steps:

1. Define an analyzer:
   DEFINE ANALYZER my_analyzer TOKENIZERS blank, class FILTERS lowercase, snowball(english);

2. Define a search index:
   DEFINE INDEX my_search ON TABLE my_table FIELDS content SEARCH ANALYZER my_analyzer BM25;

3. Query with the @@ operator:
   SELECT * FROM my_table WHERE content @@ 'search terms';

4. Get relevance scores:
   SELECT *, search::score(0) AS score FROM my_table WHERE content @@0@@ 'search terms' ORDER BY score DESC;

Advanced features:
- Multiple field indexes for cross-field search
- search::highlight() for matched term highlighting
- search::offsets() for term positions
- Custom analyzers with edgengram, ngram tokenizers"#
		),
	)])
	.with_description("Full-text search guide for SurrealDB")
}

#[cfg(test)]
mod tests {
	use rmcp::model::PromptMessageContent;

	use super::*;

	const ALL_PROMPT_NAMES: &[&str] = &[
		"query_builder",
		"schema_explorer",
		"data_modeler",
		"transaction_guide",
		"graph_traversal",
		"search_guide",
	];

	fn prompt_text(result: &GetPromptResult) -> String {
		result
			.messages
			.iter()
			.filter_map(|m| match &m.content {
				PromptMessageContent::Text {
					text,
				} => Some(text.as_str()),
				_ => None,
			})
			.collect::<Vec<_>>()
			.join("\n")
	}

	#[test]
	fn list_prompts_returns_exactly_expected_names() {
		let prompts = list_prompts();
		let mut names: Vec<&str> = prompts.iter().map(|p| p.name.as_ref()).collect();
		names.sort_unstable();
		let mut expected = ALL_PROMPT_NAMES.to_vec();
		expected.sort_unstable();
		assert_eq!(names, expected);
	}

	#[test]
	fn get_prompt_unknown_returns_none() {
		assert!(get_prompt("does_not_exist", &serde_json::Value::Null).is_none());
	}

	#[test]
	fn every_prompt_returns_non_empty_message() {
		for name in ALL_PROMPT_NAMES {
			let result = get_prompt(name, &serde_json::json!({}))
				.unwrap_or_else(|| panic!("prompt `{name}` should resolve"));
			assert!(!result.messages.is_empty(), "{name}: no messages");
			let text = prompt_text(&result);
			assert!(!text.is_empty(), "{name}: empty text");
		}
	}

	#[test]
	fn no_prompt_references_removed_tool_names() {
		// Regression guard for when the tool surface was consolidated. These
		// names must never reappear in any rendered prompt message.
		let stale = [
			"list_namespaces",
			"list_databases",
			"describe_table",
			"use_namespace",
			"use_database",
			"explain",
		];
		for name in ALL_PROMPT_NAMES {
			let result = get_prompt(name, &serde_json::json!({}))
				.unwrap_or_else(|| panic!("prompt `{name}` should resolve"));
			let text = prompt_text(&result);
			for needle in stale {
				assert!(
					!text.contains(needle),
					"prompt `{name}` still references removed tool `{needle}`"
				);
			}
		}
	}
}
