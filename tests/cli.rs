mod cli_integration {
	// cargo test --package surreal --bin surreal --no-default-features --features storage-mem --test cli -- cli_integration --nocapture

	use rand::{thread_rng, Rng};
	use std::error::Error;
	use std::fs;
	use std::path::Path;
	use std::process::{Command, Stdio};
	use tokio::time;

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

	async fn start_server(
		auth: bool,
		tls: bool,
		wait_is_ready: bool,
	) -> Result<(String, Child), Box<dyn Error>> {
		let mut rng = thread_rng();

		let port: u16 = rng.gen_range(13000..14000);
		let addr = format!("127.0.0.1:{port}");

		let mut extra_args = String::default();
		if tls {
			// Test the crt/key args but the keys are self signed so don't actually connect.
			let crt_path = tmp_file("crt.crt");
			let key_path = tmp_file("key.pem");

			let cert = rcgen::generate_simple_self_signed(Vec::new()).unwrap();
			fs::write(&crt_path, cert.serialize_pem().unwrap()).unwrap();
			fs::write(&key_path, cert.serialize_private_key_pem().into_bytes()).unwrap();

			extra_args.push_str(format!(" --web-crt {crt_path} --web-key {key_path}").as_str());
		}
		if !auth {
			extra_args.push_str(" --no-auth");
		}

		let start_args = format!("start --bind {addr} memory --no-banner --log info {extra_args}");

		println!("starting server with args: {start_args}");

		let server = run(&start_args);

		if !wait_is_ready {
			return Ok((addr, server));
		}

		// Wait 5 seconds for the server to start
		let mut interval = time::interval(time::Duration::from_millis(500));
		println!("Waiting for server to start...");
		for _i in 0..10 {
			interval.tick().await;

			if run(&format!("isready --conn http://{addr}")).output().is_ok() {
				println!("Server ready!");
				return Ok((addr, server));
			}
		}

		let server_out = server.kill().output().err().unwrap();
		println!("server output: {server_out}");
		Err("server failed to start".into())
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

	#[tokio::test]
	#[ignore = "only runs in CI"]
	async fn all_commands() {
		let (addr, _server) = start_server(false, false, true).await.unwrap();

		// Create a record
		{
			let args = format!("sql --conn http://{addr} --ns N --db D --multi");
			assert_eq!(
				run(&args).input("CREATE thing:one;\n").output(),
				Ok("[{ id: thing:one }]\n\n".to_owned()),
				"failed to send sql: {args}"
			);
		}

		// Export to stdout
		{
			let args = format!("export --conn http://{addr} --ns N --db D -");
			let output = run(&args).output().expect("failed to run stdout export: {args}");
			assert!(output.contains("DEFINE TABLE thing SCHEMALESS PERMISSIONS NONE;"));
			assert!(output.contains("UPDATE thing:one CONTENT { id: thing:one };"));
		}

		// Export to file
		let exported = {
			let exported = tmp_file("exported.surql");
			let args = format!("export --conn http://{addr} --ns N --db D {exported}");
			run(&args).output().expect("failed to run file export: {args}");
			exported
		};

		// Import the exported file
		{
			let args = format!("import --conn http://{addr} --ns N --db D2 {exported}");
			run(&args).output().expect("failed to run import: {args}");
		}

		// Query from the import (pretty-printed this time)
		{
			let args = format!("sql --conn http://{addr} --ns N --db D2 --pretty");
			assert_eq!(
				run(&args).input("SELECT * FROM thing;\n").output(),
				Ok("[\n\t{\n\t\tid: thing:one\n\t}\n]\n\n".to_owned()),
				"failed to send sql: {args}"
			);
		}

		// Unfinished backup CLI
		{
			let file = tmp_file("backup.db");
			let args = format!("backup http://{addr} {file}");
			run(&args).output().expect("failed to run backup: {args}");

			// TODO: Once backups are functional, update this test.
			assert_eq!(fs::read_to_string(file).unwrap(), "Save");
		}

		// Multi-statement (and multi-line) query including error(s) over WS
		{
			let args = format!("sql --conn ws://{addr} --ns N3 --db D3 --multi --pretty");
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
			let args = format!("sql --conn ws://{addr} --ns N4 --db D4 --multi --pretty");
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
			let args = format!("sql --conn http://{addr}");
			let output = run(&args)
				.input("USE NS N5 DB D5; CREATE thing:one;\n")
				.output()
				.expect("neither ns nor db");
			assert!(output.contains("thing:one"), "missing thing:one in {output}");
		}

		// Pass only ns
		{
			let args = format!("sql --conn http://{addr} --ns N5");
			let output = run(&args)
				.input("USE DB D5; SELECT * FROM thing:one;\n")
				.output()
				.expect("only ns");
			assert!(output.contains("thing:one"), "missing thing:one in {output}");
		}

		// Pass only db and expect an error
		{
			let args = format!("sql --conn http://{addr} --db D5");
			run(&args).output().expect_err("only db");
		}
	}

	#[tokio::test]
	#[ignore = "only runs in CI"]
	async fn start_tls() {
		let (_, server) = start_server(false, true, false).await.unwrap();

		std::thread::sleep(std::time::Duration::from_millis(2000));
		let output = server.kill().output().err().unwrap();

		// Test the crt/key args but the keys are self signed so don't actually connect.
		assert!(output.contains("Started web server"), "couldn't start web server: {output}");
	}

	#[tokio::test]
	#[ignore = "only runs in CI"]
	async fn with_kv_auth() {
		let (addr, _server) = start_server(true, false, true).await.unwrap();
		let creds = format!("--user root --pass surrealdb");
		let sql_args = format!("sql --conn http://{addr} --multi --pretty");

		// Can query /sql over HTTP
		{
			let args = format!("{sql_args} {creds}");
			let input = "INFO FOR KV;";
			let output = run(&args).input(input).output();
			assert!(output.is_ok(), "failed to query over HTTP: {}", output.err().unwrap());
		}

		// Can query /sql over WS
		{
			let args = format!("sql --conn ws://{addr} --multi --pretty {creds}");
			let input = "INFO FOR KV;";
			let output = run(&args).input(input).output();
			assert!(output.is_ok(), "failed to query over WS: {}", output.err().unwrap());
		}

		// KV user can do exports
		let exported = {
			let exported = tmp_file("exported.surql");
			let args = format!("export --conn http://{addr} {creds} --ns N --db D {exported}");

			run(&args).output().expect(format!("failed to run export: {args}").as_str());
			exported
		};

		// KV user can do imports
		{
			let args = format!("import --conn http://{addr} {creds} --ns N --db D2 {exported}");
			run(&args).output().expect(format!("failed to run import: {args}").as_str());
		}

		// KV user can do backups
		{
			let file = tmp_file("backup.db");
			let args = format!("backup {creds} http://{addr} {file}");
			run(&args).output().expect(format!("failed to run backup: {args}").as_str());

			// TODO: Once backups are functional, update this test.
			assert_eq!(fs::read_to_string(file).unwrap(), "Save");
		}
	}

	#[tokio::test]
	#[ignore = "only runs in CI"]
	async fn with_anon_auth() {
		let (addr, _server) = start_server(true, false, true).await.unwrap();
		let creds = ""; // Anonymous user
		let sql_args = format!("sql --conn http://{addr} --multi --pretty");

		// Can query /sql over HTTP
		{
			let args = format!("{sql_args} {creds}");
			let input = "";
			assert!(
				run(&args).input(input).output().is_ok(),
				"anonymous user should be able to query"
			);
		}

		// Can query /sql over HTTP
		{
			let args = format!("sql --conn ws://{addr} --multi --pretty {creds}");
			let input = "";
			assert!(
				run(&args).input(input).output().is_ok(),
				"anonymous user should be able to query"
			);
		}

		// Can't do exports
		{
			let args = format!("export --conn http://{addr} {creds} --ns N --db D -");

			assert!(
				run(&args).output().err().unwrap().contains("Forbidden"),
				"anonymous user shouldn't be able to export"
			);
		}

		// Can't do imports
		{
			let tmp_file = tmp_file("exported.surql");
			let args = format!("import --conn http://{addr} {creds} --ns N --db D2 {tmp_file}");

			assert!(
				run(&args).output().err().unwrap().contains("Forbidden"),
				"anonymous user shouldn't be able to import"
			);
		}

		// Can't do backups
		{
			let args = format!("backup {creds} http://{addr}");
			// TODO(sgirones): Once backups are functional, update this test.
			// assert!(run(&args).output().err().unwrap().contains("Forbidden"), "anonymous user shouldn't be able to backup");
			assert!(run(&args).output().is_ok(), "anonymous user can do backups");
		}
	}
}
