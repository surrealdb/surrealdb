use crate::cnf::PKG_NAME;
use crate::cnf::PKG_VERS;
use crate::err::Error;

pub fn init(_: &clap::ArgMatches) -> Result<(), Error> {
	get_cfg!(target_os: "windows", "macos", "ios", "linux", "android", "freebsd", "openbsd", "netbsd");
	get_cfg!(target_arch: "x86", "x86_64", "mips", "powerpc", "powerpc64", "arm", "aarch64");
	println!("{} {} for {} on {}", PKG_NAME, *PKG_VERS, target_os(), target_arch());
	Ok(())
}
