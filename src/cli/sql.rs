use anyhow::{Result, anyhow};
use clap::Args;
use futures::StreamExt;
use rustyline::error::ReadlineError;
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::{Completer, Editor, Helper, Highlighter, Hinter};
use serde::Serialize;
use serde_json::ser::PrettyFormatter;
use surrealdb::engine::any::{self, connect};
use surrealdb::method::{Stats, WithStats};
use surrealdb::opt::Config;
use surrealdb::{Notification, Response, Value};

use crate::cli::abstraction::auth::{CredentialsBuilder, CredentialsLevel};
use crate::cli::abstraction::{
	AuthArguments, DatabaseConnectionArguments, LevelSelectionArguments,
};
use crate::cnf::PKG_VERSION;
use crate::core::dbs::Capabilities as CoreCapabilities;
use crate::core::sql::{Expr, Param, TopLevelExpr};
use crate::core::val;
use crate::dbs::DbsCapabilities;

#[derive(Args, Debug)]
pub struct SqlCommandArguments {
	#[command(flatten)]
	conn: DatabaseConnectionArguments,
	#[command(flatten)]
	auth: AuthArguments,
	#[command(flatten)]
	level: LevelSelectionArguments,
	/// Whether database responses should be pretty printed
	#[arg(long)]
	pretty: bool,
	/// Whether to emit results in JSON
	#[arg(long)]
	json: bool,
	/// Whether omitting semicolon causes a newline
	#[arg(long)]
	multi: bool,
	/// Whether to show welcome message
	#[arg(long, env = "SURREAL_HIDE_WELCOME")]
	hide_welcome: bool,
	#[command(flatten)]
	#[command(next_help_heading = "Capabilities")]
	capabilities: DbsCapabilities,
}

pub async fn init(
	SqlCommandArguments {
		auth: AuthArguments {
			username,
			password,
			token,
			auth_level,
		},
		conn: DatabaseConnectionArguments {
			endpoint,
		},
		level: LevelSelectionArguments {
			namespace,
			database,
		},
		pretty,
		json,
		multi,
		hide_welcome,
		capabilities,
		..
	}: SqlCommandArguments,
) -> Result<()> {
	// Capabilities configuration for local engines
	let capabilities = capabilities.into_cli_capabilities();
	let config = Config::new().capabilities(capabilities.clone().into());
	let is_local = any::__into_endpoint(&endpoint)?.parse_kind()?.is_local();
	// If username and password are specified, and we are connecting to a remote
	// SurrealDB server, then we need to authenticate. If we are connecting
	// directly to a datastore (i.e. surrealkv://local.skv or tikv://...), then we
	// don't need to authenticate because we use an embedded (local) SurrealDB
	// instance with auth disabled.
	let client = if username.is_some() && password.is_some() && !is_local {
		debug!("Connecting to the database engine with authentication");
		let creds = CredentialsBuilder::default()
			.with_username(username.as_deref())
			.with_password(password.as_deref())
			.with_namespace(namespace.as_deref())
			.with_database(database.as_deref());

		let client = connect(endpoint).await?;

		debug!("Signing in to the database engine at '{:?}' level", auth_level);
		match auth_level {
			CredentialsLevel::Root => client.signin(creds.root()?).await?,
			CredentialsLevel::Namespace => client.signin(creds.namespace()?).await?,
			CredentialsLevel::Database => client.signin(creds.database()?).await?,
		};

		client
	} else if token.is_some() && !is_local {
		let client = connect(endpoint).await?;
		client.authenticate(token.unwrap()).await?;

		client
	} else {
		debug!("Connecting to the database engine without authentication");
		connect((endpoint, config)).await?
	};

	// Create a new terminal REPL
	let mut rl = Editor::new().unwrap();
	// Set custom input validation
	rl.set_helper(Some(InputValidator {
		multi,
		capabilities: &capabilities,
	}));
	// Load the command-line history
	let _ = rl.load_history("history.txt");
	// Configure the prompt
	let mut prompt = "> ".to_owned();

	// Keep track of current namespace/database.
	let is_not_empty = |s: &&str| !s.is_empty();
	let namespace = namespace.as_deref().map(str::trim).filter(is_not_empty);
	let database = database.as_deref().map(str::trim).filter(is_not_empty);
	match (namespace, database) {
		(Some(namespace), Some(database)) => {
			client.use_ns(namespace).use_db(database).await?;
			prompt = format!("{namespace}/{database}> ");
		}
		(Some(namespace), None) => {
			client.use_ns(namespace).await?;
			prompt = format!("{namespace}> ");
		}
		_ => {}
	}

	if !hide_welcome {
		let hints = [
			(true, "Different statements within a query should be separated by a (;) semicolon."),
			(
				!multi,
				"To create a multi-line query, end your lines with a (\\) backslash, and press enter.",
			),
			(true, "To exit, send a SIGTERM or press CTRL+C"),
		]
		.iter()
		.filter(|(show, _)| *show)
		.map(|(_, hint)| format!("#    - {hint}"))
		.collect::<Vec<String>>()
		.join("\n");

		eprintln!(
			"
#
#  Welcome to the SurrealDB SQL shell
#
#  How to use this shell:
{hints}
#
#  Consult https://surrealdb.com/docs/cli/sql for further instructions
#
#  SurrealDB version: {}
#
		",
			*PKG_VERSION
		);
	}

	// Loop over each command-line input
	loop {
		// Prompt the user to input SQL and check the input.
		let line = match rl.readline(&prompt) {
			// The user typed a query
			Ok(line) => {
				// Filter out all new lines
				let line = filter_line_continuations(&line);
				// Add the entry to the history
				if let Err(e) = rl.add_history_entry(line.as_str()) {
					eprintln!("{e}");
				}
				line
			}
			// The user typed CTRL-C or CTRL-D
			Err(ReadlineError::Interrupted | ReadlineError::Eof) => {
				break;
			}
			// There was en error
			Err(e) => {
				eprintln!("Error: {e:?}");
				break;
			}
		};
		// Move on if the line is empty
		if line.trim().is_empty() {
			continue;
		}
		// Complete the request
		match crate::core::syn::parse_with_capabilities(&line, &capabilities) {
			Ok(mut query) => {
				let mut namespace = None;
				let mut database = None;
				let mut vars = Vec::new();
				let init_length = query.expressions.len();
				// Capture `use` and `set/let` statements from the query
				for statement in query.expressions.iter() {
					match statement {
						TopLevelExpr::Use(stmt) => {
							if let Some(ns) = &stmt.ns {
								namespace = Some(ns.clone());
							}
							if let Some(db) = &stmt.db {
								database = Some(db.clone());
							}
						}
						TopLevelExpr::Expr(Expr::Let(stmt)) => vars.push(stmt.name.clone()),
						_ => {}
					}
				}

				for var in &vars {
					query
						.expressions
						.push(TopLevelExpr::Expr(Expr::Param(Param::from(var.clone()))))
				}

				// Extract the namespace and database from the current prompt
				let (prompt_ns, prompt_db) = split_prompt(&prompt);
				// The namespace should be set before the database can be set
				if namespace.is_none() && prompt_ns.is_empty() && database.is_some() {
					eprintln!(
						"There was a problem with the database: Specify a namespace to use\n"
					);
					continue;
				}
				// Run the query provided
				let mut result = client.query(query).with_stats().await;

				if let Ok(WithStats(res)) = &mut result {
					for (i, n) in vars.into_iter().enumerate() {
						if let Result::<Value, _>::Ok(v) = res.take(init_length + i) {
							let _ = client.set(n.into_string(), v).await;
						}
					}
				}

				let result = process(pretty, json, result);
				let result_is_error = result.is_err();
				print(result);
				if result_is_error {
					continue;
				}

				// Process the last `use` statements, if any
				if namespace.is_some() || database.is_some() {
					// Use the namespace provided in the query if any, otherwise use the one in the
					// prompt
					let namespace = namespace.as_deref().unwrap_or(prompt_ns);
					// Use the database provided in the query if any, otherwise use the one in the
					// prompt
					let database = database.as_deref().unwrap_or(prompt_db);
					// If the database is empty we should only use the namespace
					if database.is_empty() {
						if client.use_ns(namespace).await.is_ok() {
							prompt = format!("{namespace}> ");
						}
					}
					// Otherwise we should use both the namespace and database
					else if client.use_ns(namespace).use_db(database).await.is_ok() {
						prompt = format!("{namespace}/{database}> ");
					}
				}
			}
			Err(e) => {
				eprintln!("{e}\n");
			}
		}
	}
	// Save the inputs to the history
	let _ = rl.save_history("history.txt");
	// All ok
	Ok(())
}

fn process(
	pretty: bool,
	json: bool,
	res: surrealdb::Result<WithStats<Response>>,
) -> Result<String> {
	// Check query response for an error
	let mut response = res?;
	// Get the number of statements the query contained
	let num_statements = response.num_statements();
	// Prepare a single value from the query response
	let mut vec = Vec::<(Stats, Value)>::with_capacity(num_statements);
	for index in 0..num_statements {
		let (stats, result) = response.take(index).ok_or_else(|| {
			anyhow!("Expected some result for a query with index {index}, but found none")
		})?;
		let output = result.unwrap_or_else(|e| Value::from_inner(val::Value::from(e.to_string())));
		vec.push((stats, output));
	}

	tokio::spawn(async move {
		let mut stream = match response.into_inner().stream::<Value>(()) {
			Ok(stream) => stream,
			Err(error) => {
				print(Err(error));
				return;
			}
		};
		while let Some(Notification {
			query_id,
			action,
			data,
			..
		}) = stream.next().await
		{
			let message = match (json, pretty) {
				// Don't prettify the SurrealQL response
				(false, false) => {
					let value = val::Value::from(map! {
						String::from("id") => val::Value::from(val::Uuid::from(query_id)),
						String::from("action") => format!("{action:?}").to_ascii_uppercase().into(),
						String::from("result") => data.into_inner(),
					});
					value.to_string()
				}
				// Yes prettify the SurrealQL response
				(false, true) => format!(
					"-- Notification (action: {action:?}, live query ID: {query_id})\n{data:#}"
				),
				// Don't pretty print the JSON response
				(true, false) => {
					let value = val::Value::from(map! {
						String::from("id") => val::Value::from(val::Uuid::from(query_id)),
						String::from("action") => format!("{action:?}").to_ascii_uppercase().into(),
						String::from("result") => data.into_inner(),
					});
					if let Some(x) = value.into_json_value() {
						x.to_string()
					} else {
						"Value cannot be encoded into json".to_string()
					}
				}
				// Yes prettify the JSON response
				(true, true) => {
					let mut buf = Vec::new();
					let mut serializer = serde_json::Serializer::with_formatter(
						&mut buf,
						PrettyFormatter::with_indent(b"\t"),
					);
					data.into_inner().into_json_value().serialize(&mut serializer).unwrap();
					let output = String::from_utf8(buf).unwrap();
					format!(
						"-- Notification (action: {action:?}, live query ID: {query_id})\n{output:#}"
					)
				}
			};
			print(Ok(format!("\n{message}")));
		}
	});

	// Check if we should emit JSON and/or prettify
	Ok(match (json, pretty) {
		// Don't prettify the SurrealQL response
		(false, false) => {
			vec.into_iter().map(|(_, x)| x.into_inner()).collect::<val::Value>().to_string()
		}
		// Yes prettify the SurrealQL response
		(false, true) => vec
			.into_iter()
			.enumerate()
			.map(|(index, (stats, value))| {
				let query_num = index + 1;
				let execution_time = stats.execution_time.unwrap_or_default();
				format!("-- Query {query_num} (execution time: {execution_time:?})\n{value:#}",)
			})
			.collect::<Vec<String>>()
			.join("\n"),
		// Don't pretty print the JSON response
		(true, false) => {
			let value =
				val::Value::from(vec.into_iter().map(|(_, x)| x.into_inner()).collect::<Vec<_>>());
			if let Some(x) = value.into_json_value() {
				serde_json::to_string(&x).unwrap()
			} else {
				"Value cannot be serialized to json".to_owned()
			}
		}
		// Yes prettify the JSON response
		(true, true) => vec
			.into_iter()
			.enumerate()
			.map(|(index, (stats, value))| {
				let mut buf = Vec::new();
				let mut serializer = serde_json::Serializer::with_formatter(
					&mut buf,
					PrettyFormatter::with_indent(b"\t"),
				);
				let output = if let Some(x) = value.into_inner().into_json_value() {
					x.serialize(&mut serializer).unwrap();
					String::from_utf8(buf).unwrap()
				} else {
					"Value cannot be serialized to json".to_owned()
				};
				let query_num = index + 1;
				let execution_time = stats.execution_time.unwrap_or_default();
				format!("-- Query {query_num} (execution time: {execution_time:?}\n{output:#}",)
			})
			.collect::<Vec<String>>()
			.join("\n"),
	})
}

fn print(result: Result<String>) {
	match result {
		Ok(v) => {
			println!("{v}\n");
		}
		Err(e) => {
			eprintln!("{e}\n");
		}
	}
}

#[derive(Completer, Helper, Highlighter, Hinter)]
struct InputValidator<'a> {
	/// If omitting semicolon causes newline.
	multi: bool,
	capabilities: &'a CoreCapabilities,
}

#[expect(clippy::if_same_then_else)]
impl Validator for InputValidator<'_> {
	fn validate(&self, ctx: &mut ValidationContext) -> rustyline::Result<ValidationResult> {
		use ValidationResult::{Incomplete, Invalid, Valid};
		// Filter out all new line characters
		let input = filter_line_continuations(ctx.input());
		// Trim all whitespace from the user input
		let input = input.trim();
		// Process the input to check if we can send the query
		let result = if self.multi && !input.ends_with(';') {
			Incomplete // The line doesn't end with a ; and we are in multi mode
		} else if self.multi && input.is_empty() {
			Incomplete // The line was empty and we are in multi mode
		} else if input.ends_with('\\') {
			Incomplete // The line ends with a backslash
		} else if input.is_empty() {
			Valid(None) // Ignore empty lines
		} else {
			match crate::core::syn::parse_with_capabilities(input, self.capabilities) {
				Err(e) => Invalid(Some(format!(" --< {e}"))),
				_ => Valid(None),
			}
		};
		// Validation complete
		Ok(result)
	}
}

fn filter_line_continuations(line: &str) -> String {
	line.replace("\\\n", "").replace("\\\r\n", "")
}

fn split_prompt(prompt: &str) -> (&str, &str) {
	let selection = prompt.split_once('>').unwrap().0;
	selection.split_once('/').unwrap_or((selection, ""))
}
