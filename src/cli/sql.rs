use crate::cli::abstraction::{
	AuthArguments, DatabaseConnectionArguments, DatabaseSelectionOptionalArguments,
};
use crate::err::Error;
use clap::Args;
use rustyline::error::ReadlineError;
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::{Completer, Editor, Helper, Highlighter, Hinter};
use serde::Serialize;
use serde_json::ser::PrettyFormatter;
use surrealdb::engine::any::connect;
use surrealdb::opt::auth::Root;
use surrealdb::sql::{self, Statement, Value};
use surrealdb::Response;

#[derive(Args, Debug)]
pub struct SqlCommandArguments {
	#[command(flatten)]
	conn: DatabaseConnectionArguments,
	#[command(flatten)]
	auth: AuthArguments,
	#[command(flatten)]
	sel: Option<DatabaseSelectionOptionalArguments>,
	/// Whether database responses should be pretty printed
	#[arg(long)]
	pretty: bool,
	/// Whether to emit results in JSON
	#[arg(long)]
	json: bool,
	/// Whether omitting semicolon causes a newline
	#[arg(long)]
	multi: bool,
}

pub async fn init(
	SqlCommandArguments {
		auth: AuthArguments {
			username,
			password,
		},
		conn: DatabaseConnectionArguments {
			endpoint,
		},
		sel,
		pretty,
		json,
		multi,
		..
	}: SqlCommandArguments,
) -> Result<(), Error> {
	// Initialize opentelemetry and logging
	crate::o11y::builder().with_log_level("warn").init();

	let root = Root {
		username: &username,
		password: &password,
	};
	// Connect to the database engine
	#[cfg(feature = "has-storage")]
	let address = (endpoint, root);
	#[cfg(not(feature = "has-storage"))]
	let address = endpoint;
	let client = connect(address).await?;
	// Sign in to the server
	client.signin(root).await?;
	// Create a new terminal REPL
	let mut rl = Editor::new().unwrap();
	// Set custom input validation
	rl.set_helper(Some(InputValidator {
		multi,
	}));
	// Load the command-line history
	let _ = rl.load_history("history.txt");
	// Configure the prompt
	let mut prompt = "> ".to_owned();
	// Use namespace / database if specified
	if let Some(DatabaseSelectionOptionalArguments {
		namespace,
		database,
	}) = sel
	{
		match (&namespace, &database) {
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
		// Complete the request
		match sql::parse(&line) {
			Ok(query) => {
				let mut has_errored = false;
				for statement in query.iter() {
					match statement {
						Statement::Use(stmt) => {
							// Extract the namespace and database from the current prompt
							let (ns, db) = split_prompt(&prompt);
							// Use the namespace provided in the query if any, otherwise use the one in the prompt
							let namespace = stmt.ns.as_deref().unwrap_or(ns.trim());
							// The namespace should be set before the database can be set
							if namespace.is_empty() {
								eprintln!("There was a problem with the database: Specify a namespace to use\n");
								has_errored = true;
								break;
							}
							// Use the database provided in the query if any, otherwise use the one in the prompt
							let database = stmt.db.as_deref().unwrap_or(db.trim());
							// If the database is empty we should only use the namespace
							if database.is_empty() {
								if let Err(e) = client.use_ns(namespace).await {
									eprintln!("{e}\n");
									has_errored = true;
									break;
								}
								prompt = format!("{namespace}> ");
								continue;
							}
							// Otherwise we should use both the namespace and database
							if let Err(e) = client.use_ns(namespace).use_db(database).await {
								eprintln!("{e}\n");
								has_errored = true;
								break;
							}
							prompt = format!("{namespace}/{database}> ");
						}
						Statement::Set(stmt) => {
							if let Err(e) = client.set(&stmt.name, &stmt.what).await {
								eprintln!("{e}\n");
								has_errored = true;
								break;
							}
						}
						_ => {}
					}
				}
				// Don't run the query if we encountered some errors
				if has_errored {
					continue;
				}
				let res = client.query(query).await;
				// Get the request response
				match process(pretty, json, res) {
					Ok(v) => {
						println!("{v}\n");
					}
					Err(e) => {
						eprintln!("{e}\n");
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
	// Everything OK
	Ok(())
}

fn process(pretty: bool, json: bool, res: surrealdb::Result<Response>) -> Result<String, Error> {
	// Check query response for an error
	let mut response = res?;
	// Get the number of statements the query contained
	let num_statements = response.num_statements();
	// Prepare a single value from the query response
	let value = if num_statements > 1 {
		let mut output = Vec::<Value>::with_capacity(num_statements);
		for index in 0..num_statements {
			output.push(match response.take(index) {
				Ok(v) => v,
				Err(e) => e.to_string().into(),
			});
		}
		Value::from(output)
	} else {
		response.take(0)?
	};
	// Check if we should emit JSON and/or prettify
	Ok(match (json, pretty) {
		// Don't prettify the SurrealQL response
		(false, false) => value.to_string(),
		// Yes prettify the SurrealQL response
		(false, true) => format!("{value:#}"),
		// Don't pretty print the JSON response
		(true, false) => serde_json::to_string(&value.into_json()).unwrap(),
		// Yes prettify the JSON response
		(true, true) => {
			let mut buf = Vec::new();
			let mut serializer = serde_json::Serializer::with_formatter(
				&mut buf,
				PrettyFormatter::with_indent(b"\t"),
			);
			value.into_json().serialize(&mut serializer).unwrap();
			String::from_utf8(buf).unwrap()
		}
	})
}

#[derive(Completer, Helper, Highlighter, Hinter)]
struct InputValidator {
	/// If omitting semicolon causes newline.
	multi: bool,
}

#[allow(clippy::if_same_then_else)]
impl Validator for InputValidator {
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
		} else if let Err(e) = sql::parse(input) {
			Invalid(Some(format!(" --< {e}")))
		} else {
			Valid(None)
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
