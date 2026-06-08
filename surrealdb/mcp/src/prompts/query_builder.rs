//! Query builder prompt template.

use rmcp::model::{GetPromptResult, PromptMessage, PromptMessageRole};

pub fn get_prompt(arguments: &serde_json::Value) -> GetPromptResult {
	let description =
		arguments.get("description").and_then(|v| v.as_str()).unwrap_or("retrieve data");

	GetPromptResult::new(vec![PromptMessage::new_text(
		PromptMessageRole::User,
		format!(
			r#"I need help building a SurrealQL query to: {description}

Please:
1. First use `list` with kind='tables' and `info` with target=<table> to understand the schema
2. Build the appropriate SurrealQL query using parameterized inputs ($param syntax)
3. Explain the query structure
4. Execute it using the `query` tool

Important SurrealQL features to consider:
- Record links: `record_field.linked_field` for traversals
- Graph traversals: `->edge->target` and `<-edge<-source`
- Subqueries: `(SELECT ... FROM ...)`
- Array operations: `array::len()`, `array::flatten()`
- String functions: `string::concat()`, `string::contains()`
- Math functions: `math::sum()`, `math::mean()`
- Time functions: `time::now()`, `time::format()`
- Conditional: IF ... THEN ... ELSE ... END"#
		),
	)])
	.with_description("SurrealQL query builder")
}
