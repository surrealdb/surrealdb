use crate::err::Error;
use clap::Args;
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
use crate::cli::abstraction::{AuthArguments, DatabaseConnectionArguments, DatabaseSelectionArguments};

#[derive(Args, Debug)]
pub struct SqlCommandArguments {
	#[command(flatten)]
	conn: DatabaseConnectionArguments,
	#[command(flatten)]
	auth: AuthArguments,
	#[command(flatten)]
	sel: DatabaseSelectionArguments,
	#[arg(help = "Whether database responses should be pretty printed")]
	#[arg(long = "pretty")]
	#[arg(default_value_t = false)]
	pretty: bool,
}

pub async fn init(
	SqlCommandArguments {
		auth: AuthArguments {
			username,
			password,
		},
		conn: DatabaseConnectionArguments {
			connection_url: endpoint,
		},
		sel: DatabaseSelectionArguments {
			namespace: mut ns,
			database: mut db,
		},
		pretty,
		..
	}: SqlCommandArguments,
) -> Result<(), Error> {
	// Initialize opentelemetry and logging
	crate::o11y::builder().with_log_level("warn").init();

	// Connect to the database engine
	let client = connect(endpoint).await?;
	// Sign in to the server if the specified database engine supports it
	let root = Root {
		username: &username,
		password: &password,
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
		match client.use_ns(&ns).use_db(&db).await {
			Ok(()) => {
				prompt = format!("{ns}/{db}> ");
			}
			Err(error) => eprintln!("{error}"),
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
										ns = namespace.clone();
									}
									if let Some(database) = &stmt.db {
										db = database.clone();
									}
								}
								Statement::Set(stmt) => {
									if let Err(e) = client.set(&stmt.name, &stmt.what).await {
										eprintln!("{e}");
									}
								}
								_ => {}
							}
						}
						let res = client.query(query).await;
						// Get the request response
						match process(pretty, res) {
							Ok(v) => println!("{v}"),
							Err(e) => eprintln!("{e}"),
						}
					}
					Err(e) => eprintln!("{e}"),
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
	if !value.is_none_or_null() {
		// Check if we should prettify
		return Ok(match pretty {
			// Don't prettify the response
			false => value.to_string(),
			// Yes prettify the response
			true => format!("{value:#}"),
		});
	}
	Ok(String::new())
}
