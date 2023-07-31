#![allow(dead_code)]
use rand::{thread_rng, Rng};
use std::error::Error;
use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use tokio::time;

pub const USER: &str = "root";
pub const PASS: &str = "root";

/// Child is a (maybe running) CLI process. It can be killed by dropping it
pub struct Child {
	inner: Option<std::process::Child>,
}

impl Child {
	/// Send some thing to the child's stdin
	pub fn input(mut self, input: &str) -> Self {
		let stdin = self.inner.as_mut().unwrap().stdin.as_mut().unwrap();
		use std::io::Write;
		stdin.write_all(input.as_bytes()).unwrap();
		self
	}

	pub fn kill(mut self) -> Self {
		self.inner.as_mut().unwrap().kill().unwrap();
		self
	}

	/// Read the child's stdout concatenated with its stderr. Returns Ok if the child
	/// returns successfully, Err otherwise.
	pub fn output(mut self) -> Result<String, String> {
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

pub fn run_internal<P: AsRef<Path>>(args: &str, current_dir: Option<P>) -> Child {
	let mut path = std::env::current_exe().unwrap();
	assert!(path.pop());
	if path.ends_with("deps") {
		assert!(path.pop());
	}

	// Note: Cargo automatically builds this binary for integration tests.
	path.push(format!("{}{}", env!("CARGO_PKG_NAME"), std::env::consts::EXE_SUFFIX));

	let mut cmd = Command::new(path);
	if let Some(dir) = current_dir {
		cmd.current_dir(&dir);
	}
	cmd.stdin(Stdio::piped());
	cmd.stdout(Stdio::piped());
	cmd.stderr(Stdio::piped());
	cmd.args(args.split_ascii_whitespace());
	Child {
		inner: Some(cmd.spawn().unwrap()),
	}
}

/// Run the CLI with the given args
pub fn run(args: &str) -> Child {
	run_internal::<String>(args, None)
}

/// Run the CLI with the given args inside a temporary directory
pub fn run_in_dir<P: AsRef<Path>>(args: &str, current_dir: P) -> Child {
	run_internal(args, Some(current_dir))
}

pub fn tmp_file(name: &str) -> String {
	let path = Path::new(env!("OUT_DIR")).join(name);
	path.to_string_lossy().into_owned()
}

pub async fn start_server(
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

	let start_args = format!("start --bind {addr} memory --no-banner --log trace --user {USER} --pass {PASS} {extra_args}");

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
