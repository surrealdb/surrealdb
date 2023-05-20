mod cli_integration {
	// cargo test --package surreal --bin surreal --no-default-features --features storage-mem --test cli -- cli_integration --nocapture

	use rand::{thread_rng, Rng};
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
		let mut rng = thread_rng();

		let port: u16 = rng.gen_range(13000..14000);
		let addr = format!("127.0.0.1:{port}");

		let pass = rng.gen::<u64>().to_string();

		let start_args =
			format!("start --bind {addr} --user root --pass {pass} memory --no-banner --log info");

		println!("starting server with args: {start_args}");

		let _server = run(&start_args);

		std::thread::sleep(std::time::Duration::from_millis(10));

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

		// Export to stdout.
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

		// Query from the import
		{
			let args = format!(
				"sql --conn http://{addr} --user root --pass {pass} --ns N --db D2 --multi"
			);
			assert_eq!(
				run(&args).input("SELECT * FROM thing;\n").output(),
				Ok("[{ id: thing:one }]\n\n".to_owned()),
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
	}

	#[test]
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

		std::thread::sleep(std::time::Duration::from_millis(50));

		let output = server.kill().output().unwrap_err();
		assert!(output.contains("Started web server"));
	}
}
