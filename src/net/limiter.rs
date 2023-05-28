use std::{collections::HashMap, net::IpAddr, time::Instant, sync::Mutex};

use argon2::Block;
use chrono::Duration;
use surrealdb::dbs::{Session, Auth};

const RATE_LIMIT: Duration = Duration::from_secs(1);
const BURST: usize = 2;

#[derive(Debug, Eq, PartialEq, Hash)]
enum BlockableUnit {
    /// IPv4 address or IPv6 address
    // (TODO: /48 prefix only for IPv6)
    Ip(Box<str>),
    /// Authed access to a namespace
    Namespace(Box<str>),
}

struct Limits {
    /// How long previous request(s) are counted against the client
    rate_limited_until: Instant,
    /// How many extra requests have been allowed (counted towards a limit).
    burst_used: usize,
    // TODO: Concurrent connections to this server
    //local_concurrency: usize,
}

struct Limiter {
    limits: Mutex<HashMap<BlockableUnit, Limits>>
}

impl Limiter {
    /// Returns whether a new connection by this
    /// session should be blocked.
    fn should_block(&mut self, session: &Session) -> bool {
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

        // TODO: consult the KVs for heavy-hitters.

        let now = Instant::now();

        let limits = self.limits.lock().unwrap();
        let limits = limits.entry(blockable_unit).or_insert(Limits{
            rate_limited_until: now,
            burst_used: 
        }
    }
}