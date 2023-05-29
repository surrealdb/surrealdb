use crate::err::Error;
use once_cell::sync::OnceCell;
use std::{
	collections::HashMap,
	net::Ipv6Addr,
	num::NonZeroU16,
	sync::Mutex,
	time::{Duration, Instant},
};
use surrealdb::dbs::{Auth, Session};

static IP_LIM: OnceCell<Limiter> = OnceCell::new();
static NS_LIM: OnceCell<Limiter> = OnceCell::new();

pub fn init(
	rate_limit_ip: Option<NonZeroU16>,
	rate_limit_ns: Option<NonZeroU16>,
	burst_limit_ip: u16,
	burst_limit_ns: u16,
) -> Result<(), Error> {
	let _ = IP_LIM.set(Limiter::new(rate_limit_ip, burst_limit_ip));
	let _ = NS_LIM.set(Limiter::new(rate_limit_ns, burst_limit_ns));
	// All ok
	Ok(())
}

#[derive(Debug)]
struct Limits {
	/// How long previous request(s) are counted against the client
	rate_limited_until: Instant,
	/// How many extra requests have been allowed (counted towards a limit)
	burst_used: u16,
	// TODO: Concurrent connections to this particular server
	//local_concurrency: usize,
	// TODO: Concurrent connections to all servers (reported by KVS)
	//total_concurrency: usize,
}

#[derive(Debug)]
pub struct Limiter {
	inner: Mutex<Inner>,
	dur_per_req: Duration,
	prune_interval: Duration,
	burst_limit: u16,
}

/// Returns whether a new connection by this
/// session should be blocked
pub fn should_allow(session: &Session) -> bool {
	if let Some((blockable_unit, is_ns)) = blockable_unit(session) {
		let lim = if is_ns {
			&NS_LIM
		} else {
			&IP_LIM
		};

		let lim = lim.get().unwrap();

		lim.should_allow(blockable_unit)
	} else {
		true
	}
}

/// # Return
/// - IPv4 address e.g. 1.2.3.4
/// - IPv6 /48 prefixes e.g. 5629:1e38:8843::
/// - Namespace name (which never contain '.' or ':'
///   so they don't overlap with the above)
/// - `None` if should not be blocked
///
/// The `bool` is `true` iff the string is a namespace
fn blockable_unit(session: &Session) -> Option<(BoxCowStr, bool)> {
	match (&*session.au, session.ip.as_deref()) {
		(Auth::Kv, _) => {
			// If you have the root password, you are never rate-limited
			None
		}
		(Auth::Ns(ns) | Auth::Db(ns, _) | Auth::Sc(ns, _, _), _) => {
			Some((BoxCowStr::Borrowed(ns.as_str()), true))
		}
		(Auth::No, Some(ip_port)) => {
			let ip = ip_port
				.rsplit_once(':')
				.map(|(ip, _port)| ip.trim_start_matches('[').trim_end_matches(']'))
				.unwrap_or(ip_port);
			let string = if let Ok(ipv6) = ip.parse::<Ipv6Addr>() {
				let mut octets = ipv6.octets();
				// Ignore parts of the address that are easily spoofed
				octets[6..].iter_mut().for_each(|o| *o = 0);
				BoxCowStr::Owned(Ipv6Addr::from(octets).to_string().into_boxed_str())
			} else {
				BoxCowStr::Borrowed(ip)
			};
			Some((string, false))
		}
		(Auth::No, None) => {
			// It's fine not to have auth but lack of IP means something
			// wrong involving warp
			debug_assert!(false, "no IP in session");
			None
		}
	}
}

#[derive(Debug)]
struct Inner {
	limits: HashMap<Box<str>, Limits>,
	last_prune: Instant,
}

impl Limiter {
	fn new(rate_limit: Option<NonZeroU16>, burst_limit: u16) -> Self {
		let dur_per_req = if let Some(rate_limit) = rate_limit {
			Duration::from_nanos(1_000_000_000 / rate_limit.get() as u64)
		} else {
			Duration::ZERO
		};
		Self {
			inner: Mutex::new(Inner {
				limits: Default::default(),
				last_prune: Instant::now(),
			}),
			dur_per_req,
			prune_interval: Duration::from_nanos(
				(dur_per_req.as_nanos() as u64).saturating_mul(1 + burst_limit as u64),
			),
			burst_limit,
		}
	}

	/// Returns whether a new connection by this blockable unit should be allowed
	fn should_allow(&self, blockable_unit: BoxCowStr) -> bool {
		self.should_allow_at(blockable_unit, Instant::now())
	}

	/// Allows mocking the time in a test
	fn should_allow_at(&self, blockable_unit: BoxCowStr, now: Instant) -> bool {
		if self.dur_per_req == Duration::ZERO {
			return true;
		}

		// TODO: asynchronously consult the KVs for heavy-hitters.

		let mut inner = self.inner.lock().unwrap();

		let limits = loop {
			// Best case this does one hashmap lookup and noallocation
			//
			// TODO: Once stable, use the `raw_entry` API to only do one lookup and no reallocation
			// in the worst case (See https://github.com/rust-lang/rust/issues/56167).
			if let Some(ret) = inner.limits.get_mut(blockable_unit.as_ref()) {
				break ret;
			} else {
				let old = inner.limits.insert(
					Box::from(blockable_unit.as_ref()),
					Limits {
						rate_limited_until: now,
						burst_used: 0,
					},
				);
				debug_assert!(old.is_none(), "get_mut should have found blockable unit");
			}
		};

		let ok = if now > limits.rate_limited_until {
			// Limit has fully expired
			limits.burst_used = 0;
			limits.rate_limited_until = now;
			true
		} else if limits.burst_used <= self.burst_limit {
			// Allowable burst
			limits.burst_used += 1;
			limits.rate_limited_until += self.dur_per_req;
			true
		} else {
			// Excessive burst
			false
		};

		// TODO: Check concurrent connections

		// See if we can prune some elements.
		if (now - inner.last_prune) > self.prune_interval {
			inner.last_prune = now;
			inner.limits.retain(|_, l| l.rate_limited_until > now);
		}

		ok
	}
}

// Like `Cow<'a, str>` but `Box<str>` instead of `String` for size efficiency.
enum BoxCowStr<'a> {
	Owned(Box<str>),
	Borrowed(&'a str),
}

impl<'a> AsRef<str> for BoxCowStr<'a> {
	fn as_ref(&self) -> &str {
		match self {
			Self::Owned(owned) => &*owned,
			Self::Borrowed(borrowed) => borrowed,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::{blockable_unit, Limiter};
	use rand::{thread_rng, Rng};
	use std::{
		net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6},
		num::NonZeroU16,
		sync::Arc,
		time::{Duration, Instant},
	};
	use surrealdb::dbs::{Auth, Session};

	const RATE_LIMIT: u64 = 3;
	const RATE_LIMIT_BURST: usize = 5;

	fn limiter() -> Limiter {
		Limiter::new(NonZeroU16::new(RATE_LIMIT as u16), RATE_LIMIT_BURST as u16)
	}

	fn example_sessions() -> Vec<Session> {
		vec![
			Session {
				ip: Some("0.0.0.0".to_owned()),
				..Default::default()
			},
			Session {
				au: Arc::new(Auth::Ns("ns".to_owned())),
				..Default::default()
			},
			Session {
				au: Arc::new(Auth::Db("ns".to_owned(), "db".to_owned())),
				..Default::default()
			},
			Session {
				au: Arc::new(Auth::Sc("ns".to_owned(), "db".to_owned(), "sc".to_owned())),
				..Default::default()
			},
		]
	}

	#[test]
	fn disabled() {
		for session in example_sessions() {
			let limiter = Limiter::new(None, thread_rng().gen());
			let now = Instant::now();

			for _ in 0..10000 {
				assert!(limiter.should_allow_at(blockable_unit(&session).unwrap().0, now));
			}
		}
	}

	#[test]
	fn root() {
		assert!(blockable_unit(&Session {
			au: Arc::new(Auth::Kv),
			..Default::default()
		})
		.is_none());
	}

	#[test]
	fn rate() {
		for session in example_sessions() {
			// Returns true iff requests at this rate are all allowed
			let is_allowed = |rate: f64| {
				let limiter = limiter();
				let mut now = Instant::now();

				for _ in 0..RATE_LIMIT_BURST * 1000 {
					if !limiter.should_allow_at(blockable_unit(&session).unwrap().0, now) {
						return false;
					}
					now += Duration::from_nanos(1 + (1_000_000_000 as f64 / rate) as u64)
				}
				true
			};

			for ten_times_rate in
				(RATE_LIMIT.saturating_sub(10) + 1) * 10..=RATE_LIMIT.saturating_add(10) * 10
			{
				let rate = ten_times_rate as f64 * 0.1;
				assert_eq!(is_allowed(rate), rate <= RATE_LIMIT as f64, "rate: {:.1}", rate);
			}
		}
	}

	#[test]
	fn burst() {
		for session in example_sessions() {
			let limiter = limiter();
			let mut now = Instant::now();

			for i in 0..RATE_LIMIT_BURST * 1000 {
				// Essentially zero time has passed
				assert_eq!(
					limiter.should_allow_at(blockable_unit(&session).unwrap().0, now),
					i <= RATE_LIMIT_BURST,
					"{i}"
				);
				now += Duration::from_nanos(1);
			}
		}
	}

	#[test]
	fn expiry() {
		let limiter = limiter();

		let mut rng = thread_rng();
		let mut now = Instant::now();

		for _ in 0..1000 {
			let sock_addr = SocketAddrV4::new(
				Ipv4Addr::new(rng.gen(), rng.gen(), rng.gen(), rng.gen()),
				rng.gen(),
			);
			let session = Session {
				ip: Some(sock_addr.to_string()),
				..Default::default()
			};

			assert!(limiter.should_allow_at(blockable_unit(&session).unwrap().0, now));

			now += Duration::from_secs(1);

			let len = limiter.inner.lock().unwrap().limits.len();
			assert!(len < 100, "{}", len);
		}
	}

	#[test]
	fn ipv6() {
		let limiter = limiter();

		let mut rng = thread_rng();
		let now = Instant::now();

		let insert = |ipv6: Ipv6Addr| {
			let sock_addr = SocketAddrV6::new(ipv6, thread_rng().gen(), 0, 0);
			let session = Session {
				ip: Some(sock_addr.to_string()),
				..Default::default()
			};

			limiter.should_allow_at(blockable_unit(&session).unwrap().0, now);
		};

		let mut octets = rng.gen::<[u8; 16]>();
		insert(Ipv6Addr::from(octets));

		assert_eq!(limiter.inner.lock().unwrap().limits.len(), 1);

		// Same /48 so doesn't make a new entry
		octets[12] = octets[12].wrapping_add(1);
		insert(Ipv6Addr::from(octets));

		let inner = limiter.inner.lock().unwrap();
		assert_eq!(inner.limits.len(), 1, "{inner:?}");
		drop(inner);

		// Different /48 so does make a new entry
		octets[3] = octets[3].wrapping_add(1);
		insert(Ipv6Addr::from(octets));

		let inner = limiter.inner.lock().unwrap();
		assert_eq!(inner.limits.len(), 2, "{inner:?}");
	}
}
