use std::{collections::HashMap, net::IpAddr, time::Instant, sync::Mutex};

use argon2::Block;
use chrono::Duration;
use once_cell::sync::OnceCell;
use surrealdb::dbs::{Session, Auth};

pub static LIM: OnceCell<Limiter> = OnceCell::new();

// TODO: Make configurable.
const RATE_LIMIT: Duration = Duration::from_secs(1);
const BURST: usize = 2;

pub async fn init() -> Result<(), Error> {
	let _ = LIM.set(Default::default());
	// All ok
	Ok(())
}


#[derive(Debug, Eq, PartialEq, Hash)]
enum BlockableUnit {
    /// IPv4 address or IPv6 address
    // (TODO: only store /48 prefix for IPv6)
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
struct Limiter {
    limits: Mutex<HashMap<BlockableUnit, Limits>>
}

impl Limiter {
    /// Returns whether a new connection by this
    /// session should be blocked.
    pub(crate) fn should_allow(&mut self, session: &Session) -> bool {
        let blockable_unit = match (&session.au, &session.ip) {
            (Auth::Ns(ns) | Auth::Db(ns, _), _) => BlockableUnit::Namespace(Box::from(ns.as_str())),
            (_, Some(ip)) => BlockableUnit::Ip(Box::from(ip.as_str())),
            _ => {
                // It's fine not to have namespace auth but lack of IP means something
                // wrong involving warp.
                debug_assert!(false, "no IP in session");
                return false;
            }
        };

        // TODO: asynchronously consult the KVs for heavy-hitters.

        let now = Instant::now();

        let limits = self.limits.lock().unwrap();
        let limits = limits.entry(blockable_unit).or_insert(Limits{
            rate_limited_until: now,
            burst_used: 0
        });

        let ok = if now > limits.rate_limited_until {
            // Limit has fully expired.
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

        ok
    }
}