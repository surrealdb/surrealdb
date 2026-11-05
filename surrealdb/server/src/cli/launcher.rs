//! How this executable relates to the Surreal CLI.
//!
//! The Surreal CLI is a separate launcher, also installed as `surreal`, that runs
//! this executable as its `db` component. It installs and upgrades the database,
//! and runs the extension tooling as its own `ext` component. When it runs this
//! executable it sets [`INVOKED_AS_ENV`] to the command line the user typed
//! (`surreal db`), which is how this executable names itself in help and in what
//! the commands that moved to the Surreal CLI (`upgrade` and `extension`) print
//! instead. Without that variable this is a standalone install, and those
//! messages say how to move to the Surreal CLI.

/// Set by the Surreal CLI to the command it ran this executable as.
pub(crate) const INVOKED_AS_ENV: &str = "SURREAL_INVOKED_AS";

/// The executable's own name, used when [`INVOKED_AS_ENV`] is not set.
pub(crate) const BIN_NAME: &str = "surreal";

const INSTALL_SH: &str = "curl -sSf https://download.surrealdb.com/cli/launcher/install.sh | sh";
const INSTALL_PS1: &str = "irm https://download.surrealdb.com/cli/launcher/install.ps1 | iex";

/// The command line the Surreal CLI ran this executable as, or `None` for a
/// standalone install. An empty or blank value counts as unset.
pub(crate) fn invoked_as() -> Option<String> {
	let value = std::env::var(INVOKED_AS_ENV).ok()?;
	let value = value.trim();
	(!value.is_empty()).then(|| value.to_owned())
}

/// The name usage lines start with: the launcher's command line, else
/// [`BIN_NAME`].
pub(crate) fn bin_name() -> String {
	invoked_as().unwrap_or_else(|| BIN_NAME.to_owned())
}

/// The one-line command that installs the Surreal CLI on this platform.
pub(crate) fn install_command(windows: bool) -> &'static str {
	if windows {
		INSTALL_PS1
	} else {
		INSTALL_SH
	}
}
