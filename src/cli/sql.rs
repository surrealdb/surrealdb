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
				let mut namespace = None;
				let mut database = None;
				let mut vars = Vec::new();
				// Capture `use` and `set/let` statements from the query
				for statement in query.iter() {
					match statement {
						Statement::Use(stmt) => {
							if let Some(ns) = &stmt.ns {
								namespace = Some(ns.clone());
							}
							if let Some(db) = &stmt.db {
								database = Some(db.clone());
							}
						}
						Statement::Set(stmt) => {
							vars.push((stmt.name.clone(), stmt.what.clone()));
						}
						_ => {}
					}
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
				let res = client.query(query).await;
				match process(pretty, json, res) {
					Ok(v) => {
						println!("{v}\n");
					}
					Err(e) => {
						eprintln!("{e}\n");
						continue;
					}
				}
				// Persist the variables extracted from the query
				for (key, value) in vars {
					let _ = client.set(key, value).await;
				}
				// Process the last `use` statements, if any
				if namespace.is_some() || database.is_some() {
					// Use the namespace provided in the query if any, otherwise use the one in the prompt
					let namespace = namespace.as_deref().unwrap_or(prompt_ns);
					// Use the database provided in the query if any, otherwise use the one in the prompt
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
