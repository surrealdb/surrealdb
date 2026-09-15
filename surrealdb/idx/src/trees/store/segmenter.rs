//! A datastore's cache of loaded segmentation dictionaries.
//!
//! A morphological dictionary is expensive to load and large to hold — Korean's
//! is the biggest — so every analyzer naming a language shares one instance,
//! keyed by language rather than by the analyzer that asked for it. This
//! mirrors [`crate::trees::store::mapper::Mappers`], which caches analyzer
//! mapper files on the same terms.
//!
//! Unlike a mapper, a dictionary's identity is the language alone: where it is
//! read from is fixed for the datastore by configuration, so two analyzers in
//! one datastore naming the same language can never want different bytes. The
//! scope is the datastore rather than the process because the configuration is:
//! one process can hold several datastores, and the key can come from a
//! datastore URL's query parameters as well as the environment.
//!
//! # Nothing is evicted
//!
//! Where this deliberately departs from `Mappers`: removing the last analyzer
//! that named a language does not drop its dictionary, whereas `REMOVE
//! ANALYZER` does prune the mapper cache. A language's dictionary stays for the
//! datastore's lifetime once loaded, so in a process serving several tenants
//! one tenant's analyzer pins it for all of them.
//!
//! That is deliberate rather than an omission. The cache is keyed by language,
//! not by the definition that asked for it, so there is no cheap way to know
//! that the last user is gone; and reloading is expensive enough that holding
//! is the better trade for a language a datastore has used once.

use std::path::PathBuf;
#[cfg(not(target_family = "wasm"))]
use std::sync::Mutex;
use std::sync::{Arc, OnceLock};

use anyhow::{Result, bail};
use dashmap::DashMap;
#[cfg(not(target_family = "wasm"))]
use tokio::sync::watch;

use crate::expr::tokenizer::{SegmentLanguage, Tokenizer};
use crate::ft::analyzer::segmenter::Segmenter;
use crate::{Error, catalog};

/// What a load produced, in a form every waiter on it can be handed a copy of.
///
/// The error is the message rather than the error, because `anyhow::Error` is
/// not `Clone` and one outcome goes to many waiters. Everything this layer
/// produces is an [`Error::AnalyzerError`], so rebuilding one from the message
/// loses nothing a caller can observe.
#[cfg(not(target_family = "wasm"))]
type Outcome = Result<Segmenter, String>;

/// One language's dictionary, and the load producing it.
#[derive(Default)]
struct Slot {
	/// The dictionary, once a load has succeeded. Success is final — nothing
	/// evicts it — so a caller that finds it here looks no further.
	loaded: OnceLock<Segmenter>,
	/// The load in flight. Whoever finds this empty becomes the loader and
	/// installs a receiver here; everyone else waits on that one rather than
	/// starting a load of its own, and is handed its outcome whether it
	/// succeeded or failed.
	///
	/// Cleared when the load finishes, which is what bounds a failure to the
	/// callers it actually reached: they share the one failed attempt, and only
	/// a caller arriving afterwards starts a new one. Were each waiter to
	/// retry in turn, a burst against a dictionary that fails late in loading
	/// would pay for that load once per caller, and callers cancelled
	/// meanwhile would leave those attempts running with nobody to receive
	/// them.
	///
	/// The lock is never held across an await: it is taken to read the
	/// receiver, or to install one, and released before waiting.
	#[cfg(not(target_family = "wasm"))]
	inflight: Mutex<Option<watch::Receiver<Option<Outcome>>>>,
}

#[derive(Default)]
pub struct Segmenters {
	/// One slot per language. Languages are independent: a load for one never
	/// waits on, or is retried by, a caller wanting another.
	slots: DashMap<SegmentLanguage, Arc<Slot>>,
	/// Where dictionaries are read from. Fixed for the datastore, so it is held
	/// here rather than threaded through every analyzer that needs one.
	dictionary_path: Option<PathBuf>,
}

impl Segmenters {
	pub fn new(dictionary_path: Option<PathBuf>) -> Self {
		Self {
			slots: DashMap::new(),
			dictionary_path,
		}
	}

	/// Checks that the dictionary `az` names can be loaded, loading it.
	///
	/// What `DEFINE`/`ALTER ANALYZER` call: they only need the definition
	/// rejected when its dictionary cannot be had, and warming the cache on the
	/// way is free.
	///
	/// **The guarantee is node-local.** The statement runs on whichever node
	/// executes the DDL, and a dictionary is resolved from that node's build
	/// features, its configured directory and its filesystem — none of which
	/// the definition records. Success here says the coordinator can segment
	/// this language; it says nothing about any other node, which may still
	/// fail every write to an indexed table and every query against it. The log
	/// line each node emits when it resolves a dictionary is what makes that
	/// divergence observable.
	pub async fn validate(&self, az: &catalog::AnalyzerDefinition) -> Result<()> {
		self.for_definition(az).await.map(|_| ())
	}

	/// Resolves the segmenting tokenizer `az` declares, loading its dictionary.
	///
	/// Called both when an analyzer definition is stored and when one is used,
	/// so that a definition naming a dictionary this node cannot load is
	/// rejected by `DEFINE`/`ALTER ANALYZER` rather than accepted and then
	/// failing at the first `DEFINE INDEX` or query. The two callers share this
	/// one function so the rule they enforce cannot drift apart.
	///
	/// At most one `SEGMENT` is allowed per analyzer: a segmenter claims every
	/// span it is handed, so a second one would see only the first one's output
	/// and its language would silently never apply.
	pub(crate) async fn for_definition(
		&self,
		az: &catalog::AnalyzerDefinition,
	) -> Result<Option<Segmenter>> {
		let Some(tokenizers) = &az.tokenizers else {
			return Ok(None);
		};
		let mut found: Option<SegmentLanguage> = None;
		for t in tokenizers {
			let Tokenizer::Segment(language) = t else {
				continue;
			};
			if let Some(first) = found {
				bail!(Error::AnalyzerError(format!(
					"An analyzer can declare at most one SEGMENT tokenizer, but this one declares \
					 both SEGMENT({first}) and SEGMENT({language})"
				)));
			}
			found = Some(*language);
		}
		match found {
			Some(language) => self.get(language).await.map(Some),
			None => Ok(None),
		}
	}

	/// Returns the segmenter for `language`, loading its dictionary on first
	/// use.
	///
	/// A dictionary is large enough — Korean's is the biggest — that letting
	/// every caller in a burst load its own would multiply peak memory by the
	/// number of concurrent callers, so exactly one load happens per language
	/// and the rest wait for it.
	///
	/// They wait *asynchronously*, and the load itself runs on the blocking
	/// pool. Reading and parsing a dictionary is slow, synchronous work reached
	/// from async callers — `DEFINE`/`ALTER ANALYZER` and analyzer construction
	/// — so doing it inline, or making waiters park on a synchronous lock,
	/// would hold runtime workers for the length of the load and can stall the
	/// runtime outright when enough callers arrive at once.
	///
	/// A failure is not cached: it is handed to everyone waiting on the load
	/// that produced it, and the next caller to arrive starts a fresh one, so a
	/// dictionary that was briefly unreadable can still be loaded later.
	pub(crate) async fn get(&self, language: SegmentLanguage) -> Result<Segmenter> {
		// Take the slot's handle and drop the map entry before awaiting: a shard
		// guard held across the load would block unrelated languages that hash
		// to the same shard, and must not be held across an await point anyway.
		let slot = {
			let entry = self.slots.entry(language).or_insert_with(|| Arc::new(Slot::default()));
			Arc::clone(entry.value())
		};
		if let Some(segmenter) = slot.loaded.get() {
			return Ok(segmenter.clone());
		}
		Self::load(slot, language, self.dictionary_path.clone()).await
	}

	/// Produces `language`'s dictionary, joining the load already in flight
	/// rather than starting a second one.
	///
	/// **The load outlives the caller that started it.** Every caller reaching
	/// here is cancellable — statement execution runs under the transaction
	/// timeout, which drops the future it is given — and the load must not be
	/// cancellable with it. Work already handed to the blocking pool cannot be
	/// stopped and runs to completion regardless, so abandoning it saves
	/// nothing and leaves the next caller to start another. A dictionary slower
	/// to load than the timeout would then never be cached, and each attempt
	/// would add one more copy of it being built concurrently: the exact
	/// peak-memory multiplication this cache exists to prevent. Running the
	/// load in a task of its own costs a cancelled caller nothing but its own
	/// wait, and leaves the result for whoever asks next.
	#[cfg(not(target_family = "wasm"))]
	async fn load(
		slot: Arc<Slot>,
		language: SegmentLanguage,
		dictionary_path: Option<PathBuf>,
	) -> Result<Segmenter> {
		let mut rx = {
			let mut inflight = slot.inflight.lock().unwrap_or_else(|e| e.into_inner());
			match inflight.as_ref() {
				Some(rx) => rx.clone(),
				None => {
					let (tx, rx) = watch::channel(None);
					*inflight = Some(rx.clone());
					let slot = Arc::clone(&slot);
					tokio::spawn(async move {
						let outcome = match tokio::task::spawn_blocking(move || {
							Segmenter::new(language, dictionary_path.as_deref())
						})
						.await
						{
							Ok(Ok(segmenter)) => {
								// Publish before retiring the load, so a caller
								// that short-circuits on the cache cannot look
								// between the two and find neither.
								let _ = slot.loaded.set(segmenter.clone());
								Ok(segmenter)
							}
							Ok(Err(e)) => Err(e.to_string()),
							Err(e) => Err(format!("Dictionary load panicked: {e}")),
						};
						// Publishing and retiring under one lock is what makes
						// the two indivisible to a caller: it either finds this
						// load and is handed its outcome, or finds none and
						// starts its own. It can never join a load that has
						// already published and wait on it forever.
						let mut inflight = slot.inflight.lock().unwrap_or_else(|e| e.into_inner());
						let _ = tx.send(Some(outcome));
						*inflight = None;
					});
					rx
				}
			}
		};
		// The receiver was cloned at the value it held, so an outcome sent
		// before the clone is read here and one sent after wakes the wait.
		// Either way this observes exactly the load it joined.
		// Each `borrow` is read into an owned value before the next statement,
		// so no read guard is alive across the await.
		let published = rx.borrow().clone();
		let outcome = match published {
			Some(outcome) => outcome,
			None => {
				rx.changed().await.map_err(|_| {
					Error::AnalyzerError("The dictionary load ended without a result".to_string())
				})?;
				let published = rx.borrow().clone();
				published.ok_or_else(|| {
					Error::AnalyzerError("The dictionary load reported no result".to_string())
				})?
			}
		};
		outcome.map_err(|e| anyhow::Error::new(Error::AnalyzerError(e)))
	}

	/// The wasm counterpart, which needs neither a task nor a shared outcome.
	///
	/// There are no threads to move the load onto, so it runs inline and
	/// therefore within a single poll of a single-threaded runtime: no second
	/// caller can observe a load in progress, and a caller can be cancelled
	/// before the load begins but never during it.
	#[cfg(target_family = "wasm")]
	async fn load(
		slot: Arc<Slot>,
		language: SegmentLanguage,
		dictionary_path: Option<PathBuf>,
	) -> Result<Segmenter> {
		let segmenter = Segmenter::new(language, dictionary_path.as_deref())?;
		let _ = slot.loaded.set(segmenter.clone());
		Ok(segmenter)
	}
}

#[cfg(test)]
mod tests {
	#[cfg(feature = "cjk-embed-korean")]
	use std::future::Future;
	#[cfg(feature = "cjk-embed-korean")]
	use std::task::{Context, Waker};
	#[cfg(feature = "cjk-embed-korean")]
	use std::time::Duration;

	use super::*;

	fn slot_of(segmenters: &Segmenters, language: SegmentLanguage) -> Arc<Slot> {
		let entry = segmenters.slots.get(&language).expect("the language's slot should exist");
		Arc::clone(entry.value())
	}

	/// A failed load is shared with the callers waiting on it and then retired,
	/// rather than cached or left in place.
	///
	/// Retiring it is what lets a later caller try again, for a dictionary that
	/// was only briefly unreadable. Sharing it first is what stops a burst
	/// paying for the same failing load once per caller: a dictionary that
	/// fails late in loading costs nearly as much as one that succeeds, and
	/// callers cancelled meanwhile would leave those attempts running with
	/// nobody left to receive them.
	#[tokio::test]
	async fn a_failed_load_is_retired_rather_than_cached() {
		// A directory that is not there fails before any dictionary is needed,
		// so this holds whether or not one is compiled in.
		let missing = PathBuf::from("/surrealdb-no-such-dictionary-directory");
		let segmenters = Segmenters::new(Some(missing));
		assert!(
			segmenters.get(SegmentLanguage::Korean).await.is_err(),
			"a configured directory that does not exist must fail the load"
		);
		let slot = slot_of(&segmenters, SegmentLanguage::Korean);
		assert!(slot.loaded.get().is_none(), "a failure must not be cached as a dictionary");
		assert!(
			slot.inflight.lock().unwrap().is_none(),
			"a finished load must be retired, or no later caller could start another"
		);
		assert!(
			segmenters.get(SegmentLanguage::Korean).await.is_err(),
			"a later caller must be able to try again"
		);
	}

	/// A caller cancelled mid-load does not take the load with it.
	///
	/// Statement execution runs under the transaction timeout, which drops the
	/// future it was given, so a caller here can vanish at any await point. The
	/// load must still finish and publish: work already handed to the blocking
	/// pool cannot be stopped and runs to completion either way, so abandoning
	/// the cell's initialization would not save the memory it costs. It would
	/// only leave the next caller to start a second copy of a dictionary large
	/// enough that a few concurrent ones exhaust memory.
	///
	/// Nothing calls `get` after the cancellation, so the cell can only be
	/// filled by the load the cancelled caller started.
	#[cfg(feature = "cjk-embed-korean")]
	#[tokio::test]
	async fn a_cancelled_caller_does_not_abandon_the_load() {
		let segmenters = Segmenters::new(None);
		let mut first = Box::pin(segmenters.get(SegmentLanguage::Korean));
		// One poll hands the load to a task of its own and parks on it.
		// Dropping the future is then exactly what a timeout does to a caller.
		assert!(
			first.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending(),
			"the first poll should start the load and park on it, not complete it"
		);
		drop(first);
		let slot = slot_of(&segmenters, SegmentLanguage::Korean);
		// Generous enough that only a load which never publishes runs it out.
		for _ in 0..600 {
			if slot.loaded.get().is_some() {
				return;
			}
			tokio::time::sleep(Duration::from_millis(50)).await;
		}
		panic!("the cancelled caller's load never published its dictionary");
	}
}
