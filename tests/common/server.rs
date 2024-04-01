use rand::{thread_rng, Rng};
use std::error::Error;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::{env, fs};
use tokio::time;
use tokio_stream::StreamExt;
use tracing::{debug, error, info};

pub const USER: &str = "root";
pub const PASS: &str = "root";
pub const NS: &str = "testns";
pub const DB: &str = "testdb";

/// Child is a (maybe running) CLI process. It can be killed by dropping it
pub struct Child {
	inner: Option<std::process::Child>,
	stdout_path: String,
	stderr_path: String,
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

	pub fn finish(&mut self) -> Result<&mut Self, String> {
		let a = self
			.inner
			.as_mut()
			.map(|child| child.kill().map_err(|e| format!("failed to kill: {}", e)))
			.unwrap_or(Err("no inner".to_string()));
		a.map(|_ok| self)
	}

	pub fn send_signal(&self, signal: nix::sys::signal::Signal) -> nix::Result<()> {
		nix::sys::signal::kill(
			nix::unistd::Pid::from_raw(self.inner.as_ref().unwrap().id() as i32),
			signal,
		)
	}

	pub fn status(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
		self.inner.as_mut().unwrap().try_wait()
	}

	pub fn stdout(&self) -> String {
		std::fs::read_to_string(&self.stdout_path).expect("Failed to read the stdout file")
	}

	pub fn stderr(&self) -> String {
		std::fs::read_to_string(&self.stderr_path).expect("Failed to read the stderr file")
	}

	/// Read the child's stdout concatenated with its stderr. Returns Ok if the child
	/// returns successfully, Err otherwise.
	pub fn output(&mut self) -> Result<String, String> {
		let status = self.inner.as_mut().map(|child| child.wait().unwrap()).unwrap();

		let mut buf = self.stdout();
		buf.push_str(&self.stderr());

		if status.success() {
			Ok(buf)
		} else {
			Err(buf)
		}
	}
}

impl Drop for Child {
	fn drop(&mut self) {
		if let Some(inner) = self.inner.as_mut() {
			println!("Server process dropped! Assuming error happend");
			let _ = inner.kill();
			let stdout =
				std::fs::read_to_string(&self.stdout_path).expect("Failed to read the stdout file");
			println!("Server STDOUT: \n{}", stdout);
			let stderr =
				std::fs::read_to_string(&self.stderr_path).expect("Failed to read the stderr file");
			println!("Server STDERR: \n{}", stderr);
		}
		let _ = std::fs::remove_file(&self.stdout_path);
		let _ = std::fs::remove_file(&self.stderr_path);
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

	// Use local files instead of pipes to avoid deadlocks. See https://github.com/rust-lang/rust/issues/45572
	let stdout_path = tmp_file("server-stdout.log");
	let stderr_path = tmp_file("server-stderr.log");
	debug!("Redirecting output. args=`{args}` stdout={stdout_path} stderr={stderr_path})");
	let stdout = Stdio::from(File::create(&stdout_path).unwrap());
	let stderr = Stdio::from(File::create(&stderr_path).unwrap());

	cmd.env_clear();
	cmd.stdin(Stdio::piped());
	cmd.stdout(stdout);
	cmd.stderr(stderr);
	cmd.args(args.split_ascii_whitespace());

	Child {
		inner: Some(cmd.spawn().unwrap()),
		stdout_path,
		stderr_path,
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
	let path = Path::new(env!("OUT_DIR")).join(format!("{}-{}", rand::random::<u32>(), name));
	path.to_string_lossy().into_owned()
}

pub struct StartServerArguments {
	pub path: Option<String>,
	pub auth: bool,
	pub tls: bool,
	pub wait_is_ready: bool,
	pub enable_auth_level: bool,
	pub tick_interval: time::Duration,
	pub temporary_directory: Option<String>,
	pub args: String,
}

impl Default for StartServerArguments {
	fn default() -> Self {
		Self {
			path: None,
			auth: true,
			tls: false,
			wait_is_ready: true,
			enable_auth_level: false,
			tick_interval: time::Duration::new(1, 0),
			temporary_directory: None,
			args: "--allow-all".to_string(),
		}
	}
}

pub async fn start_server_without_auth() -> Result<(String, Child), Box<dyn Error>> {
	start_server(StartServerArguments {
		auth: false,
		..Default::default()
	})
	.await
}

pub async fn start_server_with_auth_level() -> Result<(String, Child), Box<dyn Error>> {
	start_server(StartServerArguments {
		enable_auth_level: true,
		..Default::default()
	})
	.await
}

pub async fn start_server_with_defaults() -> Result<(String, Child), Box<dyn Error>> {
	start_server(StartServerArguments::default()).await
}

pub async fn start_server(
	StartServerArguments {
		path,
		auth,
		tls,
		wait_is_ready,
		enable_auth_level,
		tick_interval,
		temporary_directory,
		args,
	}: StartServerArguments,
) -> Result<(String, Child), Box<dyn Error>> {
	let mut rng = thread_rng();

	let path = path.unwrap_or("memory".to_string());

	let mut extra_args = args.clone();
	if tls {
		// Test the crt/key args but the keys are self signed so don't actually connect.
		let crt_path = tmp_file("crt.crt");
		let key_path = tmp_file("key.pem");

		let cert = rcgen::generate_simple_self_signed(Vec::new()).unwrap();
		fs::write(&crt_path, cert.serialize_pem().unwrap()).unwrap();
		fs::write(&key_path, cert.serialize_private_key_pem().into_bytes()).unwrap();

		extra_args.push_str(format!(" --web-crt {crt_path} --web-key {key_path}").as_str());
	}

	if auth {
		extra_args.push_str(" --auth");
	}

	if enable_auth_level {
		extra_args.push_str(" --auth-level-enabled");
	}

	if !tick_interval.is_zero() {
		let sec = tick_interval.as_secs();
		extra_args.push_str(format!(" --tick-interval {sec}s").as_str());
	}

	if let Some(path) = temporary_directory {
		extra_args.push_str(format!(" --temporary-directory {path}").as_str());
	}

	'retry: for _ in 0..3 {
		let port: u16 = rng.gen_range(13000..24000);
		let addr = format!("127.0.0.1:{port}");

		let start_args = format!("start --bind {addr} {path} --no-banner --log trace --user {USER} --pass {PASS} {extra_args}");

		info!("starting server with args: {start_args}");

		// Configure where the logs go when running the test
		let server = run_internal::<String>(&start_args, None);

		if !wait_is_ready {
			return Ok((addr, server));
		}

		// Wait 5 seconds for the server to start
		let mut interval = time::interval(time::Duration::from_millis(1000));
		info!("Waiting for server to start...");
		for _i in 0..10 {
			interval.tick().await;

			let out = server.stderr();
			if out.contains("Address already in use") {
				continue 'retry;
			}
			if !out.contains("Started web server on") {
				continue;
			}

			if run(&format!("isready --conn http://{addr}")).output().is_ok() {
				info!("Server ready!");
				return Ok((addr, server));
			}
		}

		let server_out = server.kill().output().err().unwrap();
		if !server_out.contains("Address already in use") {
			error!("server output: {server_out}");
			break;
		}
	}
	Err("server failed to start".into())
}
