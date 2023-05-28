use crate::{
	cnf::{RATE_LIMIT, RATE_LIMIT_BURST},
	err::Error,
};
use once_cell::sync::OnceCell;
use std::{
	collections::HashMap,
	net::Ipv6Addr,
	sync::Mutex,
	time::{Duration, Instant},
};
use surrealdb::dbs::{Auth, Session};

pub static LIM: OnceCell<Limiter> = OnceCell::new();

pub fn init() -> Result<(), Error> {
	#[cfg(test)]
	let _ = LIM.set(Limiter {
		inner: None,
	});
	#[cfg(not(test))]
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

pub struct Limiter {
	inner: Option<Mutex<Inner>>,
}

impl Default for Limiter {
	fn default() -> Self {
		Self {
			inner: Some(Mutex::new(Inner {
				limits: Default::default(),
				last_prune: Instant::now(),
			})),
		}
	}
}

struct Inner {
	limits: HashMap<BlockableUnit, Limits>,
	last_prune: Instant,
}

impl Limiter {
	/// Returns whether a new connection by this
	/// session should be blocked
	pub fn should_allow(&self, session: &Session) -> bool {
		self.should_allow_at(session, Instant::now())
	}

	/// Allows mocking the time in a test
	fn should_allow_at(&self, session: &Session, now: Instant) -> bool {
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
				return true;
			}
		};

		// TODO: asynchronously consult the KVs for heavy-hitters.

		let mut inner = if let Some(inner) = &self.inner {
			inner.lock().unwrap()
		} else {
			// Rate limiting disabled e.g. during tests.
			return true;
		};

		let limits = inner.limits.entry(blockable_unit).or_insert(Limits {
			rate_limited_until: now,
			burst_used: 0,
		});

		const RATE_LIMIT_DURATION_PER_REQ: Duration =
			Duration::from_nanos(1_000_000_000 / RATE_LIMIT);

		let ok = if now > limits.rate_limited_until {
			// Limit has fully expired
			limits.burst_used = 0;
			limits.rate_limited_until = now;
			true
		} else if limits.burst_used <= RATE_LIMIT_BURST {
			// Allowable burst
			limits.burst_used += 1;
			limits.rate_limited_until += RATE_LIMIT_DURATION_PER_REQ;
			true
		} else {
			// Excessive burst
			false
		};

		// TODO: Check concurrent connections

		// See if we can prune some elements.
		const PRUNE_INTERVAL: Duration = Duration::from_nanos(
			(RATE_LIMIT_DURATION_PER_REQ.as_nanos() as u64)
				.saturating_mul(1 + RATE_LIMIT_BURST as u64),
		);
		if (now - inner.last_prune) > PRUNE_INTERVAL {
			inner.last_prune = now;
			inner.limits.retain(|_, l| l.rate_limited_until > now);
		}

		ok
	}

	/// Returns number of tracked blockable units
	#[cfg(test)]
	fn len(&self) -> usize {
		self.inner.as_ref().map(|inner| inner.lock().unwrap().limits.len()).unwrap_or(0)
	}
}

#[cfg(test)]
mod tests {
	use super::Limiter;
	use crate::cnf::{RATE_LIMIT, RATE_LIMIT_BURST};
	use rand::{thread_rng, Rng};
	use std::{
		net::Ipv4Addr,
		time::{Duration, Instant},
	};
	use surrealdb::dbs::Session;

	#[test]
	fn rate() {
		let session = Session {
			ip: Some("0.0.0.0".to_owned()),
			..Default::default()
		};

		// Returns true iff requests at this rate are all allowed
		let is_allowed = |rate: f64| {
			let limiter = Limiter::default();
			let mut now = Instant::now();

			for _ in 0..RATE_LIMIT_BURST * 1000 {
				if !limiter.should_allow_at(&session, now) {
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

	#[test]
	fn burst() {
		let limiter = Limiter::default();
		let mut now = Instant::now();

		let session = Session {
			ip: Some("0.0.0.0".to_owned()),
			..Default::default()
		};

		for i in 0..RATE_LIMIT_BURST * 1000 {
			// Essentially zero time has passed
			assert_eq!(limiter.should_allow_at(&session, now), i <= RATE_LIMIT_BURST, "{i}");
			now += Duration::from_nanos(1);
		}
	}

	#[test]
	fn expiry() {
		let limiter = Limiter::default();

		let mut rng = thread_rng();
		let mut now = Instant::now();

		for _ in 0..1000 {
			let session = Session {
				ip: Some(Ipv4Addr::new(rng.gen(), rng.gen(), rng.gen(), rng.gen()).to_string()),
				..Default::default()
			};

			assert!(limiter.should_allow_at(&session, now));

			now += Duration::from_secs(1);

			assert!(limiter.len() < 100, "{}", limiter.len());
		}

		println!("blockable units remaining: {}", limiter.len());
	}
}
