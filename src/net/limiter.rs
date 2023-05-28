use crate::err::Error;
use once_cell::sync::OnceCell;
use std::{
	collections::HashMap,
	net::Ipv6Addr,
	sync::Mutex,
	time::{Duration, Instant},
};
use surrealdb::dbs::{Auth, Session};

pub static LIM: OnceCell<Limiter> = OnceCell::new();

// TODO: Make configurable.
const RATE_LIMIT: Duration = Duration::from_secs(5);
const BURST: usize = 2;

pub fn init() -> Result<(), Error> {
	let _ = LIM.set(Default::default());
	// All ok
	Ok(())
}

#[derive(Debug, Eq, PartialEq, Hash)]
enum BlockableUnit {
	/// IPv4 address or IPv6 /48 prefixes
	Ip(Box<str>),
	/// Authed access to a namespace
	Namespace(Box<str>),
}

struct Limits {
	/// How long previous request(s) are counted against the client
	rate_limited_until: Instant,
	/// How many extra requests have been allowed (counted towards a limit)
	burst_used: usize,
	// TODO: Concurrent connections to this particular server
	//local_concurrency: usize,
	// TODO: Concurrent connections to all servers (reported by KVS)
	//total_concurrency: usize,
}

#[derive(Default)]
pub struct Limiter {
	// TODO: Prune ocasionally
	limits: Mutex<HashMap<BlockableUnit, Limits>>,
}

impl Limiter {
	/// Returns whether a new connection by this
	/// session should be blocked.
	pub fn should_allow(&self, session: &Session) -> bool {
		let blockable_unit = match (&*session.au, session.ip.as_deref()) {
			(Auth::Kv, _) => {
				// If you have the root password, you are never rate-limited
				return true;
			}
			(Auth::Ns(ns) | Auth::Db(ns, _), _) => BlockableUnit::Namespace(Box::from(ns.as_str())),
			(_, Some(ip_port)) => {
				let ip = ip_port.rsplit_once(':').map(|(ip, _port)| ip).unwrap_or(ip_port);
				let ip = if let Ok(ipv6) = ip.parse::<Ipv6Addr>() {
					let mut octets = ipv6.octets();
					// Ignore parts of the address that are easily spoofed
					octets[6..].iter_mut().for_each(|o| *o = 0);
					Ipv6Addr::from(octets).to_string().into_boxed_str()
				} else {
					Box::from(ip)
				};
				BlockableUnit::Ip(ip)
			}
			_ => {
				// It's fine not to have namespace auth but lack of IP means something
				// wrong involving warp
				debug_assert!(false, "no IP in session");
				return false;
			}
		};

		// TODO: asynchronously consult the KVs for heavy-hitters.

		let now = Instant::now();

		print!("{blockable_unit:?}");

		let mut limits = self.limits.lock().unwrap();
		let limits = limits.entry(blockable_unit).or_insert(Limits {
			rate_limited_until: now,
			burst_used: 0,
		});

		let ok = if now > limits.rate_limited_until {
			// Limit has fully expired
			limits.burst_used = 0;
			true
		} else if limits.burst_used < BURST {
			// Allowable burst
			limits.burst_used += 1;
			true
		} else {
			// Excessive burst
			false
		};

		if ok {
			limits.rate_limited_until += RATE_LIMIT;
		}

		// TODO: Check concurrent connections

		println!(" => {ok}");

		ok
	}
}
