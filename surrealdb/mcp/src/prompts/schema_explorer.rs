//! Schema exploration prompt template.

use rmcp::model::{GetPromptResult, PromptMessage, PromptMessageRole};

pub fn get_prompt(arguments: &serde_json::Value) -> GetPromptResult {
	let focus = arguments.get("focus").and_then(|v| v.as_str()).unwrap_or("all");

	let instruction = if focus == "all" {
		"Explore the full database schema by:\n\
		 1. Using `list` with kind='namespaces' to see available namespaces\n\
		 2. Using `list` with kind='databases' to see databases in the current namespace\n\
		 3. Using `list` with kind='tables' to see all tables\n\
		 4. Using `info` with target=<table> for each table to understand its fields, indexes, and permissions\n\
		 5. Summarize the data model, including relationships between tables"
			.to_string()
	} else {
		format!(
			"Explore the schema for table '{focus}' by:\n\
			 1. First use `list` with kind='tables' to confirm the table exists\n\
			 2. Using `info` with target='{focus}' to get the full schema\n\
			 3. Identifying all fields, their types, and constraints\n\
			 4. Listing indexes with `list` kind='indexes', table='{focus}'\n\
			 5. Checking permissions configuration\n\
			 6. Looking for graph relationships (RELATE edges)\n\
			 7. Summarize the table structure and suggest common query patterns"
		)
	};

	GetPromptResult::new(vec![PromptMessage::new_text(PromptMessageRole::User, instruction)])
		.with_description("Database schema exploration")
}
