//! What every kind of extension shares: the archive a definition pins, the
//! signer it must come from, what a definition lets an extension do, and the
//! runner that executes it.

pub(crate) mod archive;
#[cfg_attr(
	not(feature = "surrealism"),
	allow(dead_code, reason = "the extension runner is what reads effective capabilities")
)]
pub(crate) mod capabilities;
#[cfg(feature = "surrealism")]
pub(crate) mod trust;

/// Extension runners: the component that executes extensions, installed
/// through [`Builder::with_extension_runner`](crate::kvs::Builder::with_extension_runner)
/// or by the composer.
#[cfg(feature = "surrealism")]
pub use crate::surrealism::runner::{
	BoxFut, ExtensionRunner, Invocation, Limits, MAX_EXTENSION_DEPTH, NO_RUNNER, PreparedExtension,
	RunnerCapabilities, Stdio, SurrealismRunner, TransactionHandle, VerifiedSource,
};
