use super::Cli;
use crate::err::Error;
use clap::Args;
use clap::CommandFactory;
use clap_complete::Shell;
use path_absolutize::Absolutize;
use std::fs::File;
use std::io;
use std::path::PathBuf;
use sysinfo::{get_current_pid, ProcessExt, ProcessRefreshKind, RefreshKind, System, SystemExt};

#[derive(Args, Debug, Clone)]
pub struct ShellCompletionArguments {
	#[arg(value_enum)]
	pub shell: Option<Shell>,
	pub location: Option<PathBuf>,
}

pub async fn init(
	ShellCompletionArguments {
		shell,
		location,
	}: ShellCompletionArguments,
) -> Result<(), Error> {
	if let Some(shell) = shell.or(Shell::from_env()).or(detect_shell()?) {
		let mut cmd = Cli::command();
		let name = cmd.get_name().to_string();

		if let Some(location) = &location {
			let location = location.absolutize()?;
			if let Some(parent) = location.parent() {
				tracing::info!(location = %parent.display(), "Parent location");
				std::fs::create_dir_all(parent)?;
			}
			tracing::info!(location = %location.display(), "Autocompletion generate location");
			clap_complete::generate(shell, &mut cmd, name, &mut File::create(location.clone())?);
		} else {
			tracing::info!(location = "<stderr>", "Autocompletion generate location");
			clap_complete::generate(shell, &mut cmd, name, &mut io::stderr());
		}
	}
	Ok(())
}

fn detect_shell() -> Result<Option<Shell>, Error> {
	let sys =
		System::new_with_specifics(RefreshKind::new().with_processes(ProcessRefreshKind::new()));
	let mut current_iter_pid = Some(get_current_pid()?);

	loop {
		match current_iter_pid
			.and_then(|pid| sys.process(pid))
			.and_then(|process| process.parent())
			.and_then(|pid| sys.process(pid))
		{
			None => break,
			Some(process) => {
				if let Some(shell) = process.exe().file_name() {
					match shell.to_str().map(|x| x.to_ascii_lowercase()) {
						Some(n) if n.contains("bash") => return Ok(Some(Shell::Bash)),
						Some(n) if n.contains("fish") => return Ok(Some(Shell::Fish)),
						Some(n) if n.contains("zsh") => return Ok(Some(Shell::Zsh)),
						Some(n) if n.contains("elvish") => return Ok(Some(Shell::Elvish)),
						Some(n) if n.contains("pwsh") || n.contains("powershell") => return Ok(Some(Shell::PowerShell)),
						_ => {},
					}
				}
				current_iter_pid = Some(process.pid());
			}
		}
	}
	Ok(None)
}
