//! `surreal upgrade`: upgrading is the Surreal CLI's job, so this command only
//! says how to get there. It downloads nothing and always exits non-zero, so a
//! script that relied on it to upgrade fails instead of carrying on with the
//! version it already had.

use std::path::Path;
use std::process::ExitCode;

use clap::Args;

use crate::cli::launcher;

/// The options `upgrade` accepts. They are parsed so that a script passing them
/// reaches the explanation rather than a usage error, and have no effect.
#[derive(Args, Debug)]
#[allow(dead_code, reason = "accepted for compatibility; the values are never used")]
pub struct UpgradeCommandArguments {
	#[arg(long, hide = true)]
	nightly: bool,
	#[arg(long, hide = true)]
	alpha: bool,
	#[arg(long, hide = true)]
	beta: bool,
	#[arg(long, hide = true)]
	version: Option<String>,
	#[arg(long, hide = true)]
	dry_run: bool,
}

pub fn init(_: UpgradeCommandArguments) -> ExitCode {
	let message = match launcher::invoked_as() {
		Some(_) => launcher_message(),
		None => {
			let removal = match std::env::current_exe() {
				Ok(exe) => remove_command(&exe, cfg!(windows), needs_sudo(&exe)),
				Err(_) => remove_unknown_command(cfg!(windows)),
			};
			standalone_message(&removal, cfg!(windows))
		}
	};
	eprintln!("{message}");
	ExitCode::FAILURE
}

/// The message when the Surreal CLI ran this executable.
fn launcher_message() -> String {
	"Upgrading is done by the Surreal CLI; run `surreal upgrade db`.".to_owned()
}

/// The message for a standalone install, given the command that removes it.
fn standalone_message(removal: &str, windows: bool) -> String {
	format!(
		"Upgrading SurrealDB has moved to the Surreal CLI, which runs the database as `surreal db`.

  1. Remove this executable:    {removal}
  2. Install the Surreal CLI:   {}
  3. Run the database:          surreal db start …

After that, `surreal upgrade` upgrades the CLI and the database together.",
		launcher::install_command(windows)
	)
}

/// The command that removes the executable at `exe`.
///
/// On Windows the path is a single-quoted PowerShell string, which expands
/// nothing (a `$` or `()` in a path stays literal), with `-LiteralPath` so
/// wildcard characters are not patterns either.
fn remove_command(exe: &Path, windows: bool, sudo: bool) -> String {
	let exe = exe.to_string_lossy();
	if windows {
		format!("Remove-Item -LiteralPath '{}'", exe.replace('\'', "''"))
	} else {
		let sudo = if sudo {
			"sudo "
		} else {
			""
		};
		format!("{sudo}rm {}", shell_quote(&exe))
	}
}

/// The removal command when the running executable's path cannot be read: it
/// asks the shell where `surreal` is.
fn remove_unknown_command(windows: bool) -> String {
	if windows {
		"Remove-Item (Get-Command surreal).Source".to_owned()
	} else {
		"rm \"$(command -v surreal)\"".to_owned()
	}
}

/// Whether removing `exe` needs root: this process is not root, and
/// [`may_remove`] says it cannot remove the file itself. When the file or its
/// directory cannot be inspected, the command is printed without `sudo`.
#[cfg(unix)]
fn needs_sudo(exe: &Path) -> bool {
	use std::os::unix::fs::MetadataExt;

	use nix::unistd::{AccessFlags, Uid, access};

	// `S_ISVTX`. Its value is the same on every unix, while the type nix gives
	// it differs (`u16` on macOS, `u32` on Linux), so a plain constant compares
	// with `mode()` without a conversion one platform needs and the other's
	// clippy rejects.
	const STICKY: u32 = 0o1000;

	let uid = Uid::effective();
	if uid.is_root() {
		return false;
	}
	let Some(parent) = exe.parent() else {
		return false;
	};
	let (Ok(dir), Ok(file)) = (parent.metadata(), exe.metadata()) else {
		return false;
	};
	let dir_writable = access(parent, AccessFlags::W_OK).is_ok();
	let dir_sticky = dir.mode() & STICKY != 0;
	!may_remove(uid.as_raw(), dir_writable, dir_sticky, dir.uid(), file.uid())
}

/// Whether the non-root user `uid` may remove a file owned by `file_owner` from
/// a directory owned by `dir_owner`. It takes write access to the directory and,
/// when the directory is sticky (as `/tmp` is), owning the file or the
/// directory as well.
#[cfg(unix)]
fn may_remove(
	uid: u32,
	dir_writable: bool,
	dir_sticky: bool,
	dir_owner: u32,
	file_owner: u32,
) -> bool {
	dir_writable && (!dir_sticky || uid == file_owner || uid == dir_owner)
}

#[cfg(not(unix))]
fn needs_sudo(_: &Path) -> bool {
	false
}

/// `value` as a single POSIX shell word: unchanged when it holds only
/// characters the shell leaves alone, otherwise single-quoted.
fn shell_quote(value: &str) -> String {
	let plain = !value.is_empty()
		&& value.chars().all(|c| c.is_ascii_alphanumeric() || "/._-+:@%,=".contains(c));
	if plain {
		value.to_owned()
	} else {
		format!("'{}'", value.replace('\'', r"'\''"))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn launcher_message_points_at_the_launcher() {
		assert_eq!(
			launcher_message(),
			"Upgrading is done by the Surreal CLI; run `surreal upgrade db`."
		);
	}

	#[test]
	fn standalone_message_on_unix() {
		let removal = remove_command(Path::new("/usr/local/bin/surreal"), false, false);
		assert_eq!(
			standalone_message(&removal, false),
			"Upgrading SurrealDB has moved to the Surreal CLI, which runs the database as `surreal db`.

  1. Remove this executable:    rm /usr/local/bin/surreal
  2. Install the Surreal CLI:   curl -sSf https://download.surrealdb.com/cli/launcher/install.sh | sh
  3. Run the database:          surreal db start …

After that, `surreal upgrade` upgrades the CLI and the database together."
		);
	}

	/// On Windows the removal and install steps are PowerShell.
	#[test]
	fn standalone_message_on_windows() {
		let removal =
			remove_command(Path::new(r"C:\Program Files\SurrealDB\surreal.exe"), true, false);
		let message = standalone_message(&removal, true);
		assert!(message.contains(
			"  1. Remove this executable:    Remove-Item -LiteralPath 'C:\\Program Files\\SurrealDB\\surreal.exe'\n"
		));
		assert!(message.contains(
			"  2. Install the Surreal CLI:   irm https://download.surrealdb.com/cli/launcher/install.ps1 | iex\n"
		));
	}

	/// A PowerShell path is single-quoted, so nothing in it expands, and a quote
	/// inside it is doubled.
	#[test]
	fn windows_removal_is_literal() {
		assert_eq!(
			remove_command(Path::new(r"C:\Users\O'Brien\$env\surreal.exe"), true, false),
			r"Remove-Item -LiteralPath 'C:\Users\O''Brien\$env\surreal.exe'"
		);
	}

	/// An executable of our own, in a directory of our own, is removed without
	/// `sudo`.
	#[cfg(unix)]
	#[test]
	fn no_sudo_for_an_executable_in_a_directory_of_our_own() {
		let dir = tempfile::tempdir().unwrap();
		let exe = dir.path().join("surreal");
		std::fs::write(&exe, b"").unwrap();
		assert!(!needs_sudo(&exe));
	}

	/// A sticky directory of our own still lets us remove what is in it.
	#[cfg(unix)]
	#[test]
	fn no_sudo_in_a_sticky_directory_of_our_own() {
		use std::os::unix::fs::PermissionsExt;

		let dir = tempfile::tempdir().unwrap();
		std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o1777)).unwrap();
		let exe = dir.path().join("surreal");
		std::fs::write(&exe, b"").unwrap();
		assert!(!needs_sudo(&exe));
	}

	#[cfg(unix)]
	#[test]
	fn removal_needs_a_writable_directory() {
		assert!(may_remove(501, true, false, 0, 0));
		assert!(!may_remove(501, false, false, 501, 501));
	}

	/// In a sticky directory, such as `/tmp`, a writable directory is not
	/// enough: the file or the directory must be ours.
	#[cfg(unix)]
	#[test]
	fn removal_from_a_sticky_directory_needs_ownership() {
		assert!(!may_remove(501, true, true, 0, 0));
		assert!(may_remove(501, true, true, 0, 501));
		assert!(may_remove(501, true, true, 501, 0));
	}

	#[test]
	fn removal_prefixes_sudo_when_asked() {
		assert_eq!(
			remove_command(Path::new("/usr/local/bin/surreal"), false, true),
			"sudo rm /usr/local/bin/surreal"
		);
	}

	#[test]
	fn removal_quotes_a_path_the_shell_would_split() {
		assert_eq!(
			remove_command(Path::new("/Users/me/My Tools/surreal"), false, false),
			"rm '/Users/me/My Tools/surreal'"
		);
		assert_eq!(
			remove_command(Path::new("/tmp/it's/surreal"), false, false),
			r"rm '/tmp/it'\''s/surreal'"
		);
	}
}
