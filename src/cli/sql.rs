use crate::err::Error;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use surrealdb::engine::any::connect;
use surrealdb::error::Api as ApiError;
use surrealdb::opt::auth::Root;
use surrealdb::sql;
use surrealdb::sql::Statement;
use surrealdb::sql::Value;
use surrealdb::Error as SurrealError;
use surrealdb::Response;

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
	// If we should pretty-print responses
	let pretty = matches.is_present("pretty");
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
	let mut rl = DefaultEditor::new().unwrap();
	// Load the command-line history
	let _ = rl.load_history("history.txt");
	// Configure the prompt
	let mut prompt = "> ".to_owned();
	// Loop over each command-line input
	loop {
		// Use namespace / database if specified
		if let (Some(namespace), Some(database)) = (&ns, &db) {
			match client.use_ns(namespace).use_db(database).await {
				Ok(()) => {
					prompt = format!("{namespace}/{database}> ");
				}
				Err(error) => eprintln!("{error}"),
			}
		}
		// Prompt the user to input SQL
		let readline = rl.readline(&prompt);
		// Check the user input
		match readline {
			// The user typed a query
			Ok(line) => {
				// Ignore all empty lines
				if line.is_empty() {
					continue;
				}
				// Add the entry to the history
				if let Err(e) = rl.add_history_entry(line.as_str()) {
					eprintln!("{e}");
				}
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
								}
								Statement::Set(stmt) => {
									if let Err(e) = client.set(&stmt.name, &stmt.what).await {
										eprintln!("{e}");
										eprintln!();
									}
								}
								_ => {}
							}
						}
						let res = client.query(query).await;
						// Get the request response
						match process(pretty, res) {
							Ok(v) => {
								println!("{v}");
								println!();
							}
							Err(e) => {
								eprintln!("{e}");
								eprintln!();
							}
						}
					}
					Err(e) => {
						eprintln!("{e}");
						eprintln!();
					}
				}
			}
			// The user types CTRL-C
			Err(ReadlineError::Interrupted) => {
				break;
			}
			// The user typed CTRL-D
			Err(ReadlineError::Eof) => {
				break;
			}
			// There was en error
			Err(e) => {
				eprintln!("Error: {e:?}");
				break;
			}
		}
	}
	// Save the inputs to the history
	let _ = rl.save_history("history.txt");
	// Everything OK
	Ok(())
}

fn process(pretty: bool, res: surrealdb::Result<Response>) -> Result<String, Error> {
	use surrealdb::error::Api;
	use surrealdb::Error;
	// Extract `Value` from the response
	let value = match res?.take::<Option<Value>>(0) {
		Ok(value) => value.unwrap_or_default(),
		Err(Error::Api(Api::FromValue {
			value,
			..
		})) => value,
		Err(Error::Api(Api::LossyTake(mut res))) => match res.take::<Vec<Value>>(0) {
			Ok(mut value) => value.pop().unwrap_or_default(),
			Err(Error::Api(Api::FromValue {
				value,
				..
			})) => value,
			Err(error) => return Err(error.into()),
		},
		Err(error) => return Err(error.into()),
	};
	// Check if we should prettify
	Ok(match pretty {
		// Don't prettify the response
		false => value.to_string(),
		// Yes prettify the response
		true => format!("{value:#}"),
	})
}
