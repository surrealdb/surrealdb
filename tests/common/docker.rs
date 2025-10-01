use std::process::Command;
use std::time::{Duration, SystemTime};

use http::{HeaderMap, StatusCode, header};
use reqwest::Client;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

pub struct DockerContainer {
	id: String,
	running: bool,
}

impl DockerContainer {
	pub fn start(version: &str, file_path: &str, user: &str, pass: &str, port: u16) -> Self {
		let docker_image = format!("surrealdb/surrealdb:{version}");
		info!("Start Docker image {docker_image} with file {file_path}");
		let mut args = Arguments::new(["run", "-p", &format!("127.0.0.1:{port}:8000"), "-d"]);
		args.add([docker_image]);
		args.add(["start", "--user", user, "--pass", pass]);
		args.add([format!("file:{file_path}")]);
		let id = Self::docker(args);
		Self {
			id,
			running: true,
		}
	}

	pub fn logs(&self) {
		info!("Logging Docker container {}", self.id);
		Self::docker(Arguments::new(["logs", &self.id]));
	}
	pub fn stop(&mut self) {
		if self.running {
			info!("Stopping Docker container {}", self.id);
			Self::docker(Arguments::new(["stop", &self.id]));
			self.running = false;
		}
	}

	pub fn extract_data_dir(&self, file_path: &str) {
		let container_src_path = format!("{}:{file_path}", self.id);
		info!("Extract directory from Docker container {}", container_src_path);
		Self::docker(Arguments::new(["cp", &container_src_path, file_path]));
	}

	fn docker(args: Arguments) -> String {
		let mut command = Command::new("docker");

		let output = command.args(args.0).output().unwrap();
		let std_out = String::from_utf8(output.stdout).unwrap().trim().to_string();
		if !output.stderr.is_empty() {
			let message = String::from_utf8(output.stderr).unwrap();
			let first_line = message.lines().next().unwrap().trim();
			if first_line.starts_with("Unable to find image") && first_line.ends_with("locally") {
				info!("{message}");
			} else {
				error!("{message}");
			}
		}
		assert_eq!(output.status.code(), Some(0), "Docker command failure: {command:?}");
		std_out
	}
}

impl Drop for DockerContainer {
	fn drop(&mut self) {
		// Be sure the container is stopped
		self.stop();
		// Delete the container
		info!("Delete Docker container {}", self.id);
		Self::docker(Arguments::new(["rm", &self.id]));
	}
}

struct Arguments(Vec<String>);

impl Arguments {
	fn new<I, S>(args: I) -> Self
	where
		I: IntoIterator<Item = S>,
		S: Into<String>,
	{
		let mut a = Self(vec![]);
		a.add(args);
		a
	}

	fn add<I, S>(&mut self, args: I)
	where
		I: IntoIterator<Item = S>,
		S: Into<String>,
	{
		for arg in args {
			self.0.push(arg.into());
		}
	}
}
