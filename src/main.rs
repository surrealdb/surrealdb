//! This binary is the web-platform server for [SurrealDB](https://surrealdb.com) the
//! ultimate cloud database for tomorrow's applications. SurrealDB is a scalable,
//! distributed, collaborative, document-graph database for the realtime web.
//!
//! This binary can be used to start a database server instance using an embedded
//! in-memory datastore, or an embedded datastore persisted to disk. In addition, it
//! can be used in distributed mode by connecting to a distributed [TiKV](https://tikv.org)
//! key-value store.

#![deny(clippy::mem_forget)]
#![forbid(unsafe_code)]

#[macro_use]
extern crate log;

#[macro_use]
mod mac;

mod cli;
mod cnf;
mod dbs;
mod env;
mod err;
mod iam;
mod net;
mod o11y;
mod rpc;

use std::process::ExitCode;

fn main() -> ExitCode {
	cli::init() // Initiate the command line
}

#[cfg(test)]
mod tests {
	// cargo test --package surreal --bin surreal --no-default-features --features storage-mem -- --nocapture

	use assert_cmd::prelude::*;
	use rand::{thread_rng, Rng};
	use std::process::{Command, Stdio};

	/// Child is a (maybe running) CLI process. It can be killed by dropping it
	struct Child {
		inner: Option<std::process::Child>,
	}

	impl Child {
		/// Send some thing to the child's stdin
		fn input(mut self, input: &str) -> Self {
			let stdin = self.inner.as_mut().unwrap().stdin.as_mut().unwrap();
			use std::io::Write;
			stdin.write_all(input.as_bytes()).unwrap();
			self
		}

		/// Read the child's stdout concatenated with its stderr. Returns Ok if the child
		/// returns successfully, Err otherwise.
		fn output(mut self) -> Result<String, String> {
			let output = self.inner.take().unwrap().wait_with_output().unwrap();

			let mut buf = String::from_utf8(output.stdout).unwrap();
			buf.push_str(&String::from_utf8(output.stderr).unwrap());

			if output.status.success() {
				Ok(buf)
			} else {
				Err(buf)
			}
		}
	}

	impl Drop for Child {
		fn drop(&mut self) {
			if let Some(inner) = self.inner.as_mut() {
				let _ = inner.kill();
			}
		}
	}

	/// Run the CLI with the given args
	fn run(args: &str) -> Child {
		let mut cmd = Command::cargo_bin(env!("CARGO_PKG_NAME")).unwrap();
		cmd.stdin(Stdio::piped());
		cmd.stdout(Stdio::piped());
		cmd.stderr(Stdio::piped());
		cmd.args(args.split_ascii_whitespace());
		Child {
			inner: Some(cmd.spawn().unwrap()),
		}
	}

	#[test]
	fn version() {
		assert!(run("version").output().is_ok());
	}

	#[test]
	fn help() {
		assert!(run("help").output().is_ok());
	}

	#[test]
	fn nonexistent_subcommand() {
		assert!(run("nonexistent").output().is_err());
	}

	#[test]
	fn nonexistent_option() {
		assert!(run("version --turbo").output().is_err());
	}

	#[test]
	fn start() {
		let port: u16 = thread_rng().gen_range(13000..14000);
		let addr = format!("127.0.0.1:{port}");
		let _server = run(&format!(
			"start --bind {addr} --user root --pass root memory --no-banner --log warn"
		));

		std::thread::sleep(std::time::Duration::from_millis(10));

		assert!(run(&format!("isready --conn http://{addr}")).output().is_ok());

		assert_eq!(
			run(&format!(
				"sql --conn http://{addr} --user root --pass root --ns test --db test --multi"
			))
			.input("CREATE thing:one;\n")
			.output(),
			Ok("[{ id: thing:one }]\n\n".to_owned())
		);

		{
			let output = run(&format!(
				"export --conn http://{addr} --user root --pass root --ns test --db test -"
			))
			.output()
			.unwrap();
			assert!(output.contains("DEFINE TABLE thing SCHEMALESS PERMISSIONS NONE;"));
			assert!(output.contains("UPDATE thing:one CONTENT { id: thing:one };"));
		}
	}
}
