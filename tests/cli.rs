mod cli_integration {
	// cargo test --package surreal --bin surreal --no-default-features --features storage-mem --test cli -- cli_integration --nocapture

	use rand::{thread_rng, Rng};
	use serial_test::serial;
	use std::fs;
	use std::path::Path;
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

		fn kill(mut self) -> Self {
			self.inner.as_mut().unwrap().kill().unwrap();
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
		let mut path = std::env::current_exe().unwrap();
		assert!(path.pop());
		if path.ends_with("deps") {
			assert!(path.pop());
		}

		// Note: Cargo automatically builds this binary for integration tests.
		path.push(format!("{}{}", env!("CARGO_PKG_NAME"), std::env::consts::EXE_SUFFIX));

		let mut cmd = Command::new(path);
		cmd.stdin(Stdio::piped());
		cmd.stdout(Stdio::piped());
		cmd.stderr(Stdio::piped());
		cmd.args(args.split_ascii_whitespace());
		Child {
			inner: Some(cmd.spawn().unwrap()),
		}
	}

	fn tmp_file(name: &str) -> String {
		let path = Path::new(env!("OUT_DIR")).join(name);
		path.to_string_lossy().into_owned()
	}

	#[test]
	#[serial]
	fn version() {
		assert!(run("version").output().is_ok());
	}

	#[test]
	#[serial]
	fn help() {
		assert!(run("help").output().is_ok());
	}

	#[test]
	#[serial]
	fn nonexistent_subcommand() {
		assert!(run("nonexistent").output().is_err());
	}

	#[test]
	#[serial]
	fn nonexistent_option() {
		assert!(run("version --turbo").output().is_err());
	}

	#[test]
	#[serial]
	fn start() {
		let mut rng = thread_rng();

		let port: u16 = rng.gen_range(13000..14000);
		let addr = format!("127.0.0.1:{port}");

		let pass = rng.gen::<u64>().to_string();

		let start_args =
			format!("start --bind {addr} --user root --pass {pass} memory --no-banner --log info");

		println!("starting server with args: {start_args}");

		let _server = run(&start_args);

		std::thread::sleep(std::time::Duration::from_millis(500));

		assert!(run(&format!("isready --conn http://{addr}")).output().is_ok());

		// Create a record
		{
			let args =
				format!("sql --conn http://{addr} --user root --pass {pass} --ns N --db D --multi");
			assert_eq!(
				run(&args).input("CREATE thing:one;\n").output(),
				Ok("[{ id: thing:one }]\n\n".to_owned()),
				"failed to send sql: {args}"
			);
		}

		// Export to stdout
		{
			let args =
				format!("export --conn http://{addr} --user root --pass {pass} --ns N --db D -");
			let output = run(&args).output().expect("failed to run stdout export: {args}");
			assert!(output.contains("DEFINE TABLE thing SCHEMALESS PERMISSIONS NONE;"));
			assert!(output.contains("UPDATE thing:one CONTENT { id: thing:one };"));
		}

		// Export to file
		let exported = {
			let exported = tmp_file("exported.surql");
			let args = format!(
				"export --conn http://{addr} --user root --pass {pass} --ns N --db D {exported}"
			);
			run(&args).output().expect("failed to run file export: {args}");
			exported
		};

		// Import the exported file
		{
			let args = format!(
				"import --conn http://{addr} --user root --pass {pass} --ns N --db D2 {exported}"
			);
			run(&args).output().expect("failed to run import: {args}");
		}

		// Query from the import (pretty-printed this time)
		{
			let args = format!(
				"sql --conn http://{addr} --user root --pass {pass} --ns N --db D2 --pretty"
			);
			assert_eq!(
				run(&args).input("SELECT * FROM thing;\n").output(),
				Ok("[\n\t{\n\t\tid: thing:one\n\t}\n]\n\n".to_owned()),
				"failed to send sql: {args}"
			);
		}

		// Unfinished backup CLI
		{
			let file = tmp_file("backup.db");
			let args = format!("backup --user root --pass {pass} http://{addr} {file}");
			run(&args).output().expect("failed to run backup: {args}");

			// TODO: Once backups are functional, update this test.
			assert_eq!(fs::read_to_string(file).unwrap(), "Save");
		}

		// Multi-statement (and multi-line) query including error(s) over WS
		{
			let args = format!(
				"sql --conn ws://{addr} --user root --pass {pass} --ns N3 --db D3 --multi --pretty"
			);
			let output = run(&args)
				.input(
					r#"CREATE thing:success; \
				CREATE thing:fail SET bad=rand('evil'); \
				SELECT * FROM sleep(10ms) TIMEOUT 1ms; \
				CREATE thing:also_success;
				"#,
				)
				.output()
				.unwrap();

			assert!(output.contains("thing:success"), "missing success in {output}");
			assert!(output.contains("rgument"), "missing argument error in {output}");
			assert!(
				output.contains("time") && output.contains("out"),
				"missing timeout error in {output}"
			);
			assert!(output.contains("thing:also_success"), "missing also_success in {output}")
		}

		// Multi-statement (and multi-line) transaction including error(s) over WS
		{
			let args = format!(
				"sql --conn ws://{addr} --user root --pass {pass} --ns N4 --db D4 --multi --pretty"
			);
			let output = run(&args)
				.input(
					r#"BEGIN; \
				CREATE thing:success; \
				CREATE thing:fail SET bad=rand('evil'); \
				SELECT * FROM sleep(10ms) TIMEOUT 1ms; \
				CREATE thing:also_success; \
				COMMIT;
				"#,
				)
				.output()
				.unwrap();

			assert_eq!(
				output.lines().filter(|s| s.contains("transaction")).count(),
				3,
				"missing failed txn errors in {output:?}"
			);
			assert!(output.contains("rgument"), "missing argument error in {output}");
		}

		// Pass neither ns nor db
		{
			let args = format!("sql --conn http://{addr} --user root --pass {pass}");
			let output = run(&args)
				.input("USE NS N5 DB D5; CREATE thing:one;\n")
				.output()
				.expect("neither ns nor db");
			assert!(output.contains("thing:one"), "missing thing:one in {output}");
		}

		// Pass only ns
		{
			let args = format!("sql --conn http://{addr} --user root --pass {pass} --ns N5");
			let output = run(&args)
				.input("USE DB D5; SELECT * FROM thing:one;\n")
				.output()
				.expect("only ns");
			assert!(output.contains("thing:one"), "missing thing:one in {output}");
		}

		// Pass only db and expect an error
		{
			let args = format!("sql --conn http://{addr} --user root --pass {pass} --db D5");
			run(&args).output().expect_err("only db");
		}
	}

	#[test]
	#[serial]
	fn start_tls() {
		let mut rng = thread_rng();

		let port: u16 = rng.gen_range(13000..14000);
		let addr = format!("127.0.0.1:{port}");

		let pass = rng.gen::<u128>().to_string();

		// Test the crt/key args but the keys are self signed so don't actually connect.
		let crt_path = tmp_file("crt.crt");
		let key_path = tmp_file("key.pem");

		let cert = rcgen::generate_simple_self_signed(Vec::new()).unwrap();
		fs::write(&crt_path, cert.serialize_pem().unwrap()).unwrap();
		fs::write(&key_path, cert.serialize_private_key_pem().into_bytes()).unwrap();

		let start_args = format!(
			"start --bind {addr} --user root --pass {pass} memory --log info --web-crt {crt_path} --web-key {key_path}"
		);

		println!("starting server with args: {start_args}");

		let server = run(&start_args);

		std::thread::sleep(std::time::Duration::from_millis(750));

		let output = server.kill().output().unwrap_err();
		assert!(output.contains("Started web server"), "couldn't start web server: {output}");
	}
}
