#![cfg(feature = "kv-redis")]

use crate::err::Error;
use crate::kvs::Key;
use crate::kvs::Val;
use fred::pool::RedisPool;
use fred::prelude::*;
use fred::types::{ConnectHandle, PerformanceConfig, Scanner};
use futures::StreamExt;
use std::collections::BTreeMap;
use std::ops::Range;

pub struct Datastore {
	pool: RedisPool,
	connect_handle: Vec<ConnectHandle>,
}

pub struct Transaction {
	// Is the transaction complete?
	ok: bool,
	// Is the transaction read+write?
	rw: bool,
	client: RedisPool,
}

impl Datastore {
	/// Open a new database
	pub async fn new(path: &str) -> Result<Datastore, Error> {
		let config = match RedisConfig::from_url(format!("redis://{path}").as_str()) {
			Ok(x) => x,
			Err(e) => return Err(Error::Ds(e.to_string())),
		};
		let perf = PerformanceConfig::default();
		let policy = ReconnectPolicy::default();
		let pool = RedisPool::new(config, Some(perf), Some(policy), 1)?;
		let connect_handle = pool.connect();
		match pool.wait_for_connect().await {
			Ok(_) => Ok(Datastore {
				pool,
				connect_handle,
			}),
			Err(e) => Err(Error::Ds(e.to_string())),
		}
	}
	/// Start a new transaction
	pub async fn transaction(&self, write: bool, _: bool) -> Result<Transaction, Error> {
		Ok(Transaction {
			ok: false,
			rw: write,
			client: self.pool.clone(),
		})
	}
}

impl Transaction {
	/// Check if closed
	pub fn closed(&self) -> bool {
		self.ok
	}
	/// Cancel a transaction
	pub async fn cancel(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Mark this transaction as done
		self.ok = true;
		// Continue
		Ok(())
	}
	/// Commit a transaction
	pub async fn commit(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.rw {
			return Err(Error::TxReadonly);
		}
		// Mark this transaction as done
		self.ok = true;
		// Continue
		Ok(())
	}
	/// Check if a key exists
	pub async fn exi<K>(&mut self, key: K) -> Result<bool, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check the key
		let key = key.into();
		let res = self.client.exists(key.as_slice()).await?;
		// Return result
		Ok(res)
	}
	/// Fetch a key from the database
	pub async fn get<K>(&mut self, key: K) -> Result<Option<Val>, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}

		// Get the key
		let key = key.into();
		let res = self.client.get(key.as_slice()).await?;
		// Return result
		Ok(res)
	}
	/// Insert or update a key in the database
	pub async fn set<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.rw {
			return Err(Error::TxReadonly);
		}

		// Set the key
		let key = key.into();
		let val = val.into();
		self.client.set(key.as_slice(), val.as_slice(), None, None, false).await?;
		// Return result
		Ok(())
	}
	/// Insert a key if it doesn't exist in the database
	pub async fn put<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.rw {
			return Err(Error::TxReadonly);
		}

		let key = key.into();
		let val = val.into();
		let ret: Option<()> = self
			.client
			.set(key.as_slice(), val.as_slice(), None, Some(SetOptions::NX), false)
			.await?;

		/*  Null reply: (nil) if the SET operation was not performed because the user specified the NX or XX option but the condition was not met.
		   If the command is issued with the GET option, the above does not apply. It will instead reply as follows, regardless if the SET was actually performed

		   Reference: https://redis.io/commands/set/#return
		*/
		// Return result
		match ret {
			Some(_) => Ok(()),
			None => Err(Error::TxKeyAlreadyExists),
		}
	}
	/// Insert a key if it doesn't exist in the database
	pub async fn putc<K, V>(&mut self, key: K, val: V, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.rw {
			return Err(Error::TxReadonly);
		}

		// Get the check
		let chk = chk.map(Into::into);
		//
		let ret: Option<()> = self
			.client
			.eval(
				r#"
			if redis.call('get', KEYS[1]) == ARGV[2] then
				return redis.call('set', KEYS[1], ARGS[1], 'nx')
			end
		"#,
				vec![key.into().as_slice()],
				vec![Some(val.into().as_slice()), chk.as_deref()],
			)
			.await?;

		// Return result
		match ret {
			Some(_) => Ok(()),
			None => Err(Error::TxConditionNotMet),
		}
	}
	/// Delete a key
	pub async fn del<K>(&mut self, key: K) -> Result<(), Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.rw {
			return Err(Error::TxReadonly);
		}
		// Remove the key
		self.client.del(key.into().as_slice()).await?;
		// Return result
		Ok(())
	}
	/// Delete a key
	pub async fn delc<K, V>(&mut self, key: K, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.rw {
			return Err(Error::TxReadonly);
		}
		// Get the check
		let chk = chk.map(Into::into);

		let ret: u8 = self
			.client
			.eval(
				r#"
			if redis.call('get', KEYS[1]) == ARGV[1] then
				return redis.call('del', KEYS[1])
			else
				return 0
			end
		"#,
				vec![key.into().as_slice()],
				vec![chk.as_deref()],
			)
			.await?;

		// Return result
		if ret > 0 {
			Ok(())
		} else {
			Err(Error::TxConditionNotMet)
		}
	}
	/// Retrieve a range of keys from the databases
	pub async fn scan<K>(&mut self, rng: Range<K>, limit: u32) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key>,
	{
		fn longest_common_prefix<'a>(a: &'a [u8], b: &'a [u8]) -> &'a [u8] {
			if a.is_empty() || b.is_empty() {
				return &[];
			}

			let mut index = 0;
			for i in 0..a.len() {
				match (a.get(i), b.get(i)) {
					(Some(a), Some(b)) if a == b => {
						index = i;
					}
					_ => return &a[..i],
				}
			}
			&a[..=index]
		}

		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}

		let beg = rng.start.into();
		let end = rng.end.into();

		// maybe replace it as a trie?
		let mut kv: BTreeMap<Key, Val> = BTreeMap::new();

		let res: Option<Val> = self.client.get(beg.as_slice()).await?;
		if let Some(val) = res {
			kv.insert(beg.to_vec(), val);
		}

		let lcp = longest_common_prefix(&beg, &end);

		// a fast path to break if we have a common prefix, that means we can still search for the subset
		// but if we don't have a common prefix, the pattern will still compile to search everything
		// this of course degenerates into a full linear scan and will be painfully slow...since
		// SCAN will fully-iterate through any strings that the matches the pattern anyway...
		if !lcp.is_empty() {
			let pattern = {
				// base case: there is no glob character
				// and we will always have to add a globstar in the end to search prefix
				let mut buf = Vec::with_capacity(lcp.len() + 1);
				// escape redis glob pattern characters by prepending a backslash before it
				for c in lcp.iter() {
					if let b'*' | b'?' | b'[' | b']' | b'-' | b'^' = *c {
						buf.push(b'\\')
					};

					buf.push(*c);
				}
				buf.push(b'*');

				// fred expect the scan function to accept the first parameter string as anything that is string,
				// while we can't be sure about whether Key (an alias of Vec<u8>) is going to be
				// UTF-8 compliant or not, it's better not to assume it is and unintentionally
				// modified the whole pattern
				unsafe { String::from_utf8_unchecked(buf) }
			};

			// notice the so-called 'count' is just an optional hint
			let mut cursor = self.client.scan(pattern, Some(limit), None);

			let beg = beg.as_slice();
			let end = end.as_slice();

			while let Some(Ok(mut page)) = cursor.next().await {
				if let Some(keys) = page.take_results() {
					let client = page.create_client();

					for key in keys.into_iter().filter(|key| {
						let key = key.as_bytes();
						key >= beg && key < end
					}) {
						let value: Val = client.get(&key).await?;
						kv.insert(key.as_bytes().to_vec(), value);
					}
				}
				let _ = page.next();
			}
		}

		// at this point the scan is either invalid (due to missing a common prefix) or complete,
		// we can just consume into a KV pair iterator, and return as is
		Ok(kv.into_iter().take(limit as usize).collect())
	}
}
