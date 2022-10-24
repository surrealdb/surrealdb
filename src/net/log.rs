use log::Level;
use std::fmt;

struct OptFmt<T>(Option<T>);

impl<T: fmt::Display> fmt::Display for OptFmt<T> {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		if let Some(ref t) = self.0 {
			fmt::Display::fmt(t, f)
		} else {
			f.write_str("-")
		}
	}
}

const NAME: &str = "surrealdb::net";

pub fn write() -> warp::filters::log::Log<impl Fn(warp::filters::log::Info) + Copy> {
	warp::log::custom(|info| {
		log!(
			target: NAME,
			Level::Info,
			"{} {} {} {:?} {} \"{}\" {:?}",
			OptFmt(info.remote_addr()),
			info.method(),
			info.path(),
			info.version(),
			info.status().as_u16(),
			OptFmt(info.user_agent()),
			info.elapsed(),
		);
	})
}
