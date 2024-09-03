use arbitrary::{Arbitrary, Unstructured};
use std::env;
use std::fs::File;
use std::io::Read;
use std::process::Command;
use std::sync::Arc;
use surrealdb::engine::remote::ws::Ws;
use surrealdb::Surreal;
use surrealdb::{dbs::Session, kvs::Datastore, sql::Query};
use tokio::process::Command as TokioCommand;
use tokio::sync::Notify;
use tokio::time::Duration;

const SURREAL_ADDRESS: &str = "127.0.0.1:8000";

#[tokio::main]
async fn main() {
	let args: Vec<String> = env::args().collect();

	if args.len() < 2 {
		eprintln!("Usage: {} [-s] [-b] [-r] <path>", args[0]);
		eprintln!("  <path>  Local path to a test case provided by OSS-Fuzz.");
		eprintln!("  -s      The test case is a query string instead of a pre-parsed query.");
		eprintln!("  -b      Print a full backtrace after crashes. Includes stack overflows.");
		eprintln!("  -r      Spawn a local SurrealDB server to attempt to reproduce remotely.");
		std::process::exit(1);
	}

	let flag_string = args.iter().any(|arg| arg == "-s");
	let flag_backtrace = args.iter().any(|arg| arg == "-b");
	let flag_remote = args.iter().any(|arg| arg == "-r");
	let test_case_path = args.iter().skip(1).find(|arg| !arg.starts_with("--")).unwrap();
	let mut test_case = File::open(test_case_path).unwrap();

	if flag_backtrace {
		env::set_var("RUST_BACKTRACE", "full");
		env::set_var("RUST_LIB_BACKTRACE", "full");
		unsafe { backtrace_on_stack_overflow::enable() };
	}

	let query = if flag_string {
		let mut query_string = String::new();
		test_case.read_to_string(&mut query_string).unwrap();
		surrealdb::sql::parse(&query_string).unwrap()
	} else {
		let mut buffer = Vec::new();
		test_case.read_to_end(&mut buffer).unwrap();
		let unstructured: &mut Unstructured = &mut Unstructured::new(&buffer);
		<Query as Arbitrary>::arbitrary(unstructured).unwrap()
	};

	println!("Test case: {}", test_case_path);
	let query_string = format!("{}", query);
	println!("Original query string: {}", query_string);

	if flag_remote {
		let ready = Arc::new(Notify::new());
		let ready_clone = ready.clone();
		tokio::spawn(async move { run_server(ready_clone).await });
		ready.notified().await;

		let db = Surreal::new::<Ws>(SURREAL_ADDRESS).await.unwrap();
		db.use_ns("test").use_db("test").await.unwrap();

		println!("Attempting to remotely parse query string...");
		if let Err(err) = db.query(&query_string).await {
			println!("Failed to remotely parse query string: {}", err);
		};
		println!("Attempting to remotely execute query object...");
		if let Err(err) = db.query(query.clone()).await {
			println!("Failed to remotely execute query object: {}", err);
		};
	}

	println!("Attempting to locally parse query string...");
	match surrealdb::sql::parse(&query_string) {
		Ok(ast) => println!("Parsed query string: {}", ast),
		Err(err) => println!("Failed to locally parse query string: {}", err),
	};

	println!("Attempting to locally execute query object...");
	let ds = Datastore::new("memory").await.unwrap();
	let ses = Session::owner().with_ns("test").with_db("test");
	if let Err(err) = ds.process(query, &ses, None).await {
		println!("Failed to locally execute query object: {}", err);
	};
}

async fn run_server(ready: Arc<Notify>) {
	loop {
		let binary_name = "surreal";
		let args = ["start", "--bind", SURREAL_ADDRESS, "--log", "none", "--no-banner"];
		let mut cmd = Command::new(&binary_name);
		cmd.args(&args);

		let mut child = match TokioCommand::from(cmd).spawn() {
			Ok(child) => child,
			Err(err) => {
				eprintln!("Failed to start process: {:?}", err);
				continue;
			}
		};
		ready.notify_one();

		let status = child.wait().await;
		match status {
			Ok(exit_status) => {
				if !exit_status.success() {
					eprintln!("Child process exited with an error: {:?}", exit_status);
				}
			}
			Err(err) => eprintln!("Failed to wait for child process: {:?}", err),
		}
		tokio::time::sleep(Duration::from_millis(200)).await;
	}
}
