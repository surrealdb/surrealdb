use std::fmt;
use tracing::Level;

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

pub fn write() -> warp::filters::log::Log<impl Fn(warp::filters::log::Info) + Copy> {
	warp::log::custom(|info| {
		event!(
			Level::INFO,
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
