use core::fmt;

use common::LeafError;
use http::header::{InvalidHeaderName, InvalidHeaderValue, ToStrError};
use thiserror::Error;

use crate::api::err::ApiError;
use crate::exec::Error as ExecError;
use crate::iam::PolicyError;
use crate::kvs::Error as KvsError;

#[cfg(test)]
mod behaviour_pins;
mod to_types;
#[cfg(test)]
mod wire_snapshot_test;
/// Re-exported so the engine names its universal failures under `err` like
/// every other error type, wherever the type itself has to live.
pub use common::EngineError;
pub(crate) use to_types::into_types_error;

/// The SurrealDB error types that can appear inside an [`anyhow::Error`],
/// and how each becomes a public error.
///
/// `?` on a typed error takes anyhow's blanket `From<E: StdError>` rather than
/// any wrapping variant, so the payload can be any one of these and not just
/// the outermost. That is why recovery is a chain: a type missing from this
/// list reaches the client as an untyped internal error with its structure
/// lost - a permission denial arrives as an opaque internal failure rather
/// than a refusal the client can branch on.
///
/// Each entry supplies a probe, so registering a type without also proving it
/// survives the boundary is not expressible. Order does not matter -
/// `downcast` is exact-type, so the arms are disjoint - except that an
/// already-public error passes through first, ahead of any classification.
///
/// # What belongs in the invocation below
///
/// Every error type any part of the engine can put into an `anyhow::Error`.
/// That is one entry per layer error - the types implementing
/// [`common::LeafError`] - plus core's own [`Error`] and the two types that are
/// only ever reached through a wrapper. It is not a list of *core's* errors:
/// most entries name a type core does not define. Adding a layer error type
/// without adding it here is the failure `every_leaf_error_is_registered`
/// exists to catch.
macro_rules! error_registry {
	($( $ty:ty {
		map: $map:expr,
		probe: $probe:expr,
		expect: $expect:ident,
	} )*) => {
		/// Convert an [`anyhow::Error`] into a structured [`surrealdb_types::Error`].
		///
		/// Errors from outside the engine (`reqwest`, `object_store`,
		/// `std::io`) are expected to fall through to the end.
		pub fn anyhow_to_types_error(error: anyhow::Error) -> surrealdb_types::Error {
			// Already public: pass it through rather than re-deriving a worse version.
			let error = match error.downcast::<surrealdb_types::Error>() {
				Ok(e) => return e,
				Err(e) => e,
			};
			$(
				let error = match error.downcast::<$ty>() {
					Ok(e) => return ($map)(e),
					Err(e) => e,
				};
			)*
			surrealdb_types::Error::from_anyhow_with_chain(error)
		}

		/// The types listed in [`error_registry!`], for the completeness test below.
		#[cfg(test)]
		const REGISTERED: &[&str] = &[$( stringify!($ty) ),*];

		#[cfg(test)]
		mod registry_tests {
			use std::collections::BTreeMap;

			use super::*;

			/// Every registered type must survive the trip through `anyhow`
			/// with its classification intact.
			///
			/// The failure this guards is silent: an unregistered type still
			/// compiles, still reaches the boundary, and still produces an
			/// error, just an untyped internal one with the structure gone.
			#[test]
			fn every_registered_type_survives_the_anyhow_boundary() {
				$({
					let mapped = anyhow_to_types_error(anyhow::Error::new($probe));
					assert!(
						mapped.$expect(),
						concat!(stringify!($ty), " lost its classification at the boundary: {}"),
						mapped.kind_str(),
					);

					// A probe has to be able to tell a registered type from an
					// unregistered one. The fall-through produces
					// `internal(to_string())`, so a probe whose mapped form
					// equals that proves nothing: the entry could be deleted
					// and this test would still pass. Choose a variant with a
					// kind, details or cause the fall-through cannot produce.
					let unregistered = surrealdb_types::Error::from_anyhow_with_chain(
						anyhow::Error::new($probe),
					);
					assert_ne!(
						mapped,
						unregistered,
						concat!(
							stringify!($ty),
							"'s probe cannot distinguish being registered from not being \
							 registered, so it does not guard the registration",
						),
					);
				})*
			}

			/// An already-public error is returned as it was, not re-derived.
			#[test]
			fn public_errors_pass_through_verbatim() {
				let original = surrealdb_types::Error::not_found("gone".to_string(), None);
				assert_eq!(anyhow_to_types_error(anyhow::Error::new(original.clone())), original);
			}

			/// Every `LeafError` in the tree must appear in the registry.
			///
			/// The registry generates its own assertions, so it is blind to a type
			/// that was never added to it: the missing entry takes its test along
			/// with it. Reading the source for the trait impls is what closes that,
			/// and it is the failure that matters most - an unregistered type does
			/// not break, it silently degrades to an untyped internal error.
			///
			/// Compared by name and count rather than by path, because several
			/// layers name their type plainly `Error`. The registry may hold more
			/// than the tree implements: `KvsError` and `PolicyError` are registered
			/// but map through a wrapper rather than implementing the trait.
			#[test]
			fn every_leaf_error_is_registered() {
				fn tally(names: impl Iterator<Item = String>) -> BTreeMap<String, usize> {
					let mut counts = BTreeMap::new();
					for name in names {
						*counts.entry(name).or_insert(0) += 1;
					}
					counts
				}

				// Scans core and every sibling crate, not a hand-written list of
				// the ones that happen to own a layer error today: an error owned
				// by a crate below core degrades at this boundary exactly as one
				// owned by core does, and the layers keep moving downwards.
				let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
				let mut implemented = Vec::new();
				let siblings: Vec<_> = std::fs::read_dir(root.join(".."))
					.expect("read the crate directory")
					.flatten()
					.filter(|sibling| sibling.file_name() != "core")
					.map(|sibling| sibling.path().join("src"))
					.filter(|src| src.is_dir())
					.collect();

				// Assert on the crates reached, not just on finding *some* impl.
				// Core's own impls satisfy a non-empty check on their own, so
				// without this the sibling walk could find nothing — a crate
				// moving out of `surrealdb/`, which is the direction of travel —
				// and the test would keep passing while covering only core.
				let scanned: std::collections::HashSet<_> = siblings
					.iter()
					.filter_map(|src| src.parent()?.file_name()?.to_str().map(str::to_owned))
					.collect();
				for owner in ["common", "syn"] {
					assert!(
						scanned.contains(owner),
						"`{owner}` owns a `LeafError` impl but was not scanned; the sibling walk \
						 covers {scanned:?}. If the crate moved, teach the walk where it went \
						 rather than dropping it, or its errors silently stop being checked",
					);
				}

				let mut pending = vec![root.join("src")];
				pending.extend(siblings);
				while let Some(dir) = pending.pop() {
					let Ok(entries) = std::fs::read_dir(&dir) else {
						continue;
					};
					for entry in entries.flatten() {
						let path = entry.path();
						if path.is_dir() {
							pending.push(path);
						} else if path.extension().is_some_and(|e| e == "rs") {
							let Ok(text) = std::fs::read_to_string(&path) else {
								continue;
							};
							// Matches the bare and fully-qualified spellings. A
							// rustfmt-wrapped or macro-generated impl is still
							// invisible; the crate-set assertion above is what
							// keeps that from silently covering nothing.
							implemented.extend(text.lines().filter_map(|l| {
								let l = l.trim();
								let rest = l
									.strip_prefix("impl LeafError for ")
									.or_else(|| l.strip_prefix("impl crate::LeafError for "))
									.or_else(|| l.strip_prefix("impl common::LeafError for "))?;
								Some(rest.trim_end_matches(" {").to_string())
							}));
						}
					}
				}
				assert!(!implemented.is_empty(), "found no `LeafError` impls; the scan is broken");

				let registered = tally(
					REGISTERED.iter().map(|e| e.rsplit("::").next().unwrap_or(e).to_string()),
				);
				let missing: Vec<_> = tally(implemented.into_iter())
					.into_iter()
					.filter(|(name, count)| registered.get(name).copied().unwrap_or(0) < *count)
					.collect();
				assert!(
					missing.is_empty(),
					"these types implement `LeafError` more often than the registry lists them, \
					 so at least one degrades to an untyped internal error at the boundary: \
					 {missing:?}",
				);
			}

			/// Errors from outside the engine fall through to the chain-preserving tail.
			#[test]
			fn foreign_errors_fall_through() {
				let mapped = anyhow_to_types_error(anyhow::Error::new(std::io::Error::other("disk")));
				assert!(mapped.is_internal());
				assert!(mapped.message().contains("disk"));
			}
		}
	};
}

error_registry! {
	Error {
		map: into_types_error,
		probe: Error::Http("sample".to_string()),
		expect: is_connection,
	}
	ApiError {
		map: LeafError::to_types_error,
		probe: ApiError::NotFound,
		expect: is_not_found,
	}
	// `KvsError` and `PolicyError` are wrapped rather than mapped directly: their
	// `Error` variants contribute a message prefix that clients already see.
	// A bare one arrives when the transactor bails at commit time, or when an
	// iam call is `?`-ed straight into `anyhow`; both shapes have to classify
	// the same way, which is why these are registered as well as wrapped.
	KvsError {
		map: |e| into_types_error(Error::Kvs(e)),
		probe: KvsError::TransactionConflict("busy".to_string()),
		expect: is_query,
	}
	PolicyError {
		map: |e| into_types_error(Error::IamError(e)),
		probe: PolicyError::NotAllowed {
			actor: "a".to_string(),
			action: "b".to_string(),
			resource: "c".to_string(),
		},
		expect: is_not_allowed,
	}
	EngineError {
		map: LeafError::to_types_error,
		probe: EngineError::QueryCancelled,
		expect: is_query,
	}
	crate::key::Error {
		map: LeafError::to_types_error,
		probe: crate::key::Error::Unencodable,
		expect: is_serialization,
	}
	crate::expr::Error {
		map: LeafError::to_types_error,
		probe: crate::expr::Error::TryAdd("a".to_string(), "b".to_string()),
		expect: is_validation,
	}
	crate::exec::Error {
		map: LeafError::to_types_error,
		probe: crate::exec::Error::NsEmpty,
		expect: is_validation,
	}
	crate::catalog::Error {
		map: LeafError::to_types_error,
		probe: crate::catalog::Error::NsNotFound {
			name: "n".to_string(),
		},
		expect: is_not_found,
	}
	// Probed through `ObsError` rather than a bare variant: every `buc::Error`
	// maps to `internal`, which is also what the unregistered fall-through
	// produces, so only a variant carrying a source distinguishes the two.
	crate::buc::Error {
		map: LeafError::to_types_error,
		probe: crate::buc::Error::ObsError(object_store::Error::NotSupported {
			source: "sample".into(),
		}),
		expect: is_internal,
	}
	crate::idx::Error {
		map: LeafError::to_types_error,
		probe: crate::idx::Error::DuplicatedMatchRef {
			mr: 1,
		},
		expect: is_validation,
	}
	crate::doc::Error {
		map: LeafError::to_types_error,
		probe: crate::doc::Error::IdNotFound {
			rid: "r".to_string(),
		},
		expect: is_not_found,
	}
	crate::kvs::DatastoreError {
		map: LeafError::to_types_error,
		probe: crate::kvs::DatastoreError::ExpiredSession,
		expect: is_not_allowed,
	}
	crate::iam::Error {
		map: LeafError::to_types_error,
		probe: crate::iam::Error::ExpiredToken,
		expect: is_not_allowed,
	}
	crate::dbs::capabilities::Error {
		map: LeafError::to_types_error,
		probe: crate::dbs::capabilities::Error::ScriptingNotAllowed,
		expect: is_not_allowed,
	}
	// Probed with `Revision` rather than a bare variant: the other two map to
	// `internal`, which is also what the unregistered fall-through produces.
	crate::dbs::SortError {
		map: LeafError::to_types_error,
		probe: crate::dbs::SortError::Revision(revision::Error::Serialize("sample".to_string())),
		expect: is_serialization,
	}
	// Owned by `surrealdb-syn` rather than by a core module: core raises a parse
	// failure exactly as the parser reported it.
	crate::syn::ParseError {
		map: LeafError::to_types_error,
		probe: crate::syn::ParseError::InvalidQuery(crate::syn::error::RenderedError {
			errors: vec!["sample".to_string()],
			snippets: Vec::new(),
		}),
		expect: is_validation,
	}
}

/// Recover an [`EngineError`] from an [`anyhow::Error`].
///
/// An engine failure reaches `anyhow` two ways: raised bare, or wrapped in
/// [`Error::Engine`] by a function typed on `Error`. Callers should not have to
/// know which, so every check goes through here rather than matching one shape
/// and quietly missing the other.
pub(crate) fn engine_error(error: &anyhow::Error) -> Option<&EngineError> {
	if let Some(engine) = error.downcast_ref::<EngineError>() {
		return Some(engine);
	}
	match error.downcast_ref::<Error>() {
		Some(Error::Engine(engine)) => Some(engine),
		_ => None,
	}
}

/// Recover an [`ExecError`] from an [`anyhow::Error`].
///
/// Statement execution raises its failures bare from the recursive `compute`
/// path and wrapped in [`Error::Exec`] from the functions typed on `Error`.
/// Both shapes reach the same `anyhow::Error`, so every behavioural check goes
/// through here rather than matching one and quietly missing the other.
pub(crate) fn exec_error(error: &anyhow::Error) -> Option<&ExecError> {
	if let Some(exec) = error.downcast_ref::<ExecError>() {
		return Some(exec);
	}
	match error.downcast_ref::<Error>() {
		Some(Error::Exec(exec)) => Some(exec),
		_ => None,
	}
}

/// Returns true if an [`anyhow::Error`] contains a core query-cancellation error.
pub fn is_query_cancelled(error: &anyhow::Error) -> bool {
	matches!(engine_error(error), Some(EngineError::QueryCancelled))
}

/// Returns true if an [`anyhow::Error`] contains a core query-timeout error.
pub fn is_query_timedout(error: &anyhow::Error) -> bool {
	matches!(engine_error(error), Some(EngineError::QueryTimedout(_)))
}

/// The failures core raises under its own name.
///
/// Core is the crate the other layers meet in, not a layer of its own, so this
/// enum is deliberately not "every error the engine can produce". It holds two
/// kinds of thing and nothing else:
///
/// 1. **Wrappers** over another layer's error, one per layer whose failures reach a core function
///    that is typed on `Error` instead of `anyhow::Result`. They exist for the `?` conversion and
///    nothing more: each classifies exactly as the error it wraps, three of them transparently and
///    two behind a message prefix clients already see. They earn their place by disappearing, and
///    they go away as those signatures do.
/// 2. **Outbound HTTP**, the one failure mode core owns end to end. There is no module below core
///    that performs outbound requests: the callers span `fnc`, `exec` and `iam`, and each reaches
///    the shared client in `fnc::util::http`. A layer error for two variants across three unrelated
///    callers would be a worse home than this one.
///
/// # Adding a variant
///
/// Almost certainly not here. A new failure belongs to the layer that raises
/// it, in that layer's own error type - `catalog`, `doc`, `exec`, `expr`,
/// `idx`, `key`, `buc`, `iam`, `kvs`, `dbs::capabilities`, `dbs::SortError`,
/// `api::err::ApiError`, or `surrealdb_syn::ParseError` - which then implements
/// [`common::LeafError`] and joins [`error_registry!`]. If no layer owns the
/// concept, which layer should is the question to answer first; this enum is
/// not the answer to it.
///
/// A failure that genuinely any layer can raise is [`EngineError`], in
/// `common`, not a variant here.
#[derive(Error, Debug)]
#[allow(
	clippy::enum_variant_names,
	reason = "each wrapper is named after the layer error it holds, two of which end in `Error`"
)]
#[cfg_attr(
	not(any(feature = "http", feature = "jwks")),
	allow(dead_code, reason = "the outbound HTTP variants need a client to be compiled in")
)]
pub(crate) enum Error {
	/// A failure any layer of the engine can raise.
	///
	/// Transparent: the message and source chain are the inner error's, so a
	/// wrapped engine failure is indistinguishable from a bare one on the wire.
	/// The wrapper exists only because core has functions typed on `Error`
	/// rather than `anyhow::Result`, which need the `?` conversion.
	#[error(transparent)]
	Engine(#[from] EngineError),

	/// A failure raised while executing a statement.
	///
	/// Transparent, like [`Error::Engine`]: the message and source chain are the
	/// inner error's. The wrapper exists because the planner and the execution
	/// contexts are typed on `Error` rather than `anyhow::Result`, so they need
	/// the `?` conversion.
	#[error(transparent)]
	Exec(#[from] ExecError),

	/// An error originating from the KVS (Key-Value Store) layer
	#[error("There was a problem with the key-value store: {0}")]
	Kvs(#[from] KvsError),

	/// Represents an underlying IAM error
	#[error("IAM error: {0}")]
	IamError(#[from] PolicyError),

	/// A failure from the `DEFINE API` layer.
	///
	/// Transparent, like [`Error::Engine`]: the message and source chain are the
	/// inner error's, so a wrapped API failure reads the same in a log as it
	/// does on the wire.
	#[error(transparent)]
	ApiError(ApiError),

	/// A `DEFINE API` path literal could not be parsed.
	///
	/// Kept in core rather than on `ApiError`: the server surfaces an
	/// `ApiError` with its own status and echoes the message, so moving it
	/// would turn a masked 500 into a described 400. That is the right
	/// outcome and a deliberate change to make on its own.
	#[error("The string could not be parsed into a path: {0}")]
	InvalidPath(String),

	/// The URL given to an outbound request is not a URL core can send to.
	#[error("The URL `{0}` is invalid")]
	InvalidUrl(String),

	/// An outbound request failed, or came back in a form the caller cannot use.
	#[error("There was an error processing a remote HTTP request: {0}")]
	Http(String),
}

impl Error {
	#[cold]
	#[track_caller]
	pub fn unreachable<T: fmt::Display>(message: T) -> Error {
		let location = std::panic::Location::caller();
		let message = format!("{}:{}: {}", location.file(), location.line(), message);
		EngineError::Unreachable(message).into()
	}
}

impl From<Error> for String {
	fn from(e: Error) -> String {
		e.to_string()
	}
}

impl From<ApiError> for Error {
	fn from(value: ApiError) -> Self {
		Error::ApiError(value)
	}
}

impl From<InvalidHeaderName> for Error {
	fn from(error: InvalidHeaderName) -> Self {
		EngineError::Unreachable(error.to_string()).into()
	}
}

impl From<InvalidHeaderValue> for Error {
	fn from(error: InvalidHeaderValue) -> Self {
		EngineError::Unreachable(error.to_string()).into()
	}
}

impl From<ToStrError> for Error {
	fn from(error: ToStrError) -> Self {
		EngineError::Unreachable(error.to_string()).into()
	}
}

#[cfg(any(feature = "http", feature = "jwks"))]
impl From<reqwest::Error> for Error {
	fn from(e: reqwest::Error) -> Error {
		Error::Http(e.to_string())
	}
}

impl From<Error> for crate::expr::ControlFlow {
	fn from(error: Error) -> Self {
		crate::expr::ControlFlow::Err(error.into())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::iam::Error as AuthError;

	/// A record access SIGNIN/SIGNUP clause is user SurrealQL, so a failure
	/// inside it reaches the client as a query error and not as a refusal to
	/// authenticate, even though it is raised from the authentication flow.
	#[test]
	fn test_anyhow_to_types_error_signup_query_failed() {
		let error = anyhow::Error::new(AuthError::AccessRecordSignupQueryFailed);
		let types_error = anyhow_to_types_error(error);
		assert!(
			types_error.is_query(),
			"expected Query error, got {} with message: {}",
			types_error.kind_str(),
			types_error.message()
		);
		assert!(!types_error.is_not_allowed(), "a failing SIGNUP clause is not an auth refusal");
	}

	#[test]
	fn test_anyhow_to_types_error_signin_query_failed() {
		let error = anyhow::Error::new(AuthError::AccessRecordSigninQueryFailed);
		let types_error = anyhow_to_types_error(error);
		assert!(
			types_error.is_query(),
			"expected Query error, got {} with message: {}",
			types_error.kind_str(),
			types_error.message()
		);
		assert!(!types_error.is_not_allowed(), "a failing SIGNIN clause is not an auth refusal");
	}
}
