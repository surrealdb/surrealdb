use crate::cnf::PKG_NAME;
use crate::cnf::PKG_VERS;
use crate::err::Error;

pub(crate) fn os() -> &'static str {
	get_cfg!(target_os: "windows", "macos", "ios", "linux", "android", "freebsd", "openbsd", "netbsd");
	target_os()
}

pub(crate) fn arch() -> &'static str {
	get_cfg!(target_arch: "x86", "x86_64", "mips", "powerpc", "powerpc64", "arm", "aarch64");
	target_arch()
}

pub fn init(_: &clap::ArgMatches) -> Result<(), Error> {
	println!("{} {} for {} on {}", PKG_NAME, *PKG_VERS, os(), arch());
	Ok(())
}
