use crate::err::Error;
use rustyline::error::ReadlineError;
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::{Completer, Editor, Helper, Highlighter, Hinter};
use surrealdb::engine::any::connect;
use surrealdb::error::Api as ApiError;
use surrealdb::opt::auth::Root;
use surrealdb::sql::{self, Statement, Value};
use surrealdb::{Error as SurrealError, Response};

#[tokio::main]
pub async fn init(matches: &clap::ArgMatches) -> Result<(), Error> {
	// Initialize opentelemetry and logging
	crate::o11y::builder().with_log_level("warn").init();
	// Parse all other cli arguments
	let username = matches.value_of("user").unwrap();
	let password = matches.value_of("pass").unwrap();
	let endpoint = matches.value_of("conn").unwrap();
	let mut ns = matches.value_of("ns").map(str::to_string);
	let mut db = matches.value_of("db").map(str::to_string);
	let mut ns_db_dirty = true;
	// If we should pretty-print responses
	let pretty = matches.is_present("pretty");
	// If omitting semicolon causes a newline
	// Connect to the database engine
	let client = connect(endpoint).await?;
	// Sign in to the server if the specified database engine supports it
	let root = Root {
		username,
		password,
	};
	if let Err(error) = client.signin(root).await {
		match error {
			// Authentication not supported by this engine, we can safely continue
			SurrealError::Api(ApiError::AuthNotSupported) => {}
			error => {
				return Err(error.into());
			}
		}
	}
	// Create a new terminal REPL
	let mut rl = Editor::new().unwrap();
	// Set custom input validation
	rl.set_helper(Some(InputValidator {
		multi: matches.is_present("multi"),
	}));
	// Load the command-line history
	let _ = rl.load_history("history.txt");
	// Configure the prompt
	let mut prompt = "> ".to_owned();
	// Loop over each command-line input
	loop {
		if ns_db_dirty {
			// Use namespace / database if specified
			match (&ns, &db) {
				(Some(namespace), Some(database)) => {
					match client.use_ns(namespace).use_db(database).await {
						Ok(()) => {
							prompt = format!("{namespace}/{database}> ");
						}
						Err(error) => eprintln!("{error}"),
					}
				}
				(Some(namespace), None) => match client.use_ns(namespace).await {
					Ok(()) => {
						prompt = format!("{namespace}> ");
					}
					Err(error) => eprintln!("{error}"),
				},
				(None, Some(database)) => match client.use_db(database).await {
					Ok(()) => {
						prompt = format!("/{database}> ");
					}
					Err(error) => eprintln!("{error}"),
				},
				(None, None) => {}
			}
		}

		// Prompt the user to input SQL and check the input.
		let line = match rl.readline(&prompt) {
			// The user typed a query
			Ok(line) => {
				let line = filter_line_continuations(&line);
				// Ignore all empty lines
				if line.is_empty() {
					continue;
				}
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
				for statement in query.iter() {
					match statement {
						Statement::Use(stmt) => {
							if let Some(namespace) = &stmt.ns {
								ns = Some(namespace.clone());
							}
							if let Some(database) = &stmt.db {
								db = Some(database.clone());
							}
							ns_db_dirty = true;
						}
						Statement::Set(stmt) => {
							if let Err(e) = client.set(&stmt.name, &stmt.what).await {
								eprintln!("{e}\n");
							}
						}
						_ => {}
					}
				}
				let res = client.query(query).await;
				// Get the request response
				match process(pretty, res) {
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

fn process(pretty: bool, res: surrealdb::Result<Response>) -> Result<String, Error> {
	// Check query response for an error
	let mut response = res?;
	// Get the number of statements the query contained
	let num_statements = response.num_statements();
	// Prepare a single value from the query response
	let value = if num_statements > 1 {
		let mut output = Vec::<Value>::with_capacity(num_statements);
		for index in 0..num_statements {
			output.push(response.take(index)?);
		}
		Value::from(output)
	} else {
		response.take(0)?
	};
	// Check if we should prettify
	Ok(match pretty {
		// Don't prettify the response
		false => value.to_string(),
		// Yes prettify the response
		true => format!("{value:#}"),
	})
}

#[derive(Completer, Helper, Highlighter, Hinter)]
struct InputValidator {
	/// If omitting semicolon causes newline.
	multi: bool,
}

impl Validator for InputValidator {
	fn validate(&self, ctx: &mut ValidationContext) -> rustyline::Result<ValidationResult> {
		use ValidationResult::{Incomplete, Invalid, Valid};
		let input = filter_line_continuations(ctx.input());
		let result = if (self.multi && !input.trim().ends_with(';'))
			|| input.ends_with('\\')
			|| input.is_empty()
		{
			Incomplete
		} else if let Err(e) = sql::parse(&input) {
			Invalid(Some(format!(" --< {e}")))
		} else {
			Valid(None)
		};
		Ok(result)
	}
}

fn filter_line_continuations(line: &str) -> String {
	line.replace("\\\n", "").replace("\\\r\n", "")
}
