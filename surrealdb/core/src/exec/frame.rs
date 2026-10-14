//! The `Options` frame a stored expression or a Surrealism module call runs
//! in, derived from the execution context it is reached from.
//!
//! The options carry the evaluation frame the context is in;
//! [`options_for_frame`] is the one place that decides what the frame means.

#[cfg(any(test, feature = "surrealism"))]
use crate::dbs::Options;

#[cfg(any(test, feature = "surrealism"))]
/// The `Options` an evaluation runs with, given the frame it runs in.
///
/// An evaluation frame has two halves, and they move independently:
///
/// - `skip_fetch_perms` — permission enforcement is off, so the evaluation does not recurse into
///   the gates of the very table it is guarding. Every `PERMISSIONS` clause sets it, a read guard
///   and a write guard alike, and so do an event body and a bucket predicate.
/// - `no_write` — an effect reached from here that a schema clause must not have is refused, and
///   names why. Every schema clause sets it: a `PERMISSIONS` predicate of any kind
///   (GHSA-66r2-5gwj-gxm2), a `COMPUTED` body, and a field's `VALUE`, `DEFAULT` and `ASSERT`
///   clauses.
///
/// An event body has the first without the second: it runs as the definer
/// and may write. That is the line between
/// [`Options::new_for_permission_predicate`] and
/// `Options::new_with_unchecked_reads`, and reading one half off the other
/// would erase it.
///
/// A frame the caller's options already refuse in is never lifted here: the
/// strictest frame on the way in is the one the evaluation runs in.
///
/// The two halves are taken as arguments rather than read from a context
/// because they are tracked per `EvalContext` at some call sites and per root
/// at others. Naming them at each site keeps this the only place that decides
/// what they mean.
///
/// `version_stamp` is the `VERSION` the execution context reads at
/// (`ExecutionContext::version_stamp`, set by `VersionScope`). It is carried
/// into the options so a nested evaluation reads the same point in time as the
/// statement. Without a stamp the options keep the version they already carry.
pub(crate) fn options_for_frame(
	options: &Options,
	skip_fetch_perms: bool,
	no_write: Option<crate::dbs::NoWriteFrame>,
	version_stamp: Option<u64>,
) -> Options {
	let inherited = options.no_write;
	let mut options = if skip_fetch_perms {
		options.new_with_unchecked_reads()
	} else {
		options.clone()
	};
	options.no_write = no_write.or(inherited);
	match version_stamp {
		Some(stamp) => options.with_version(Some(stamp)),
		None => options,
	}
}

#[cfg(test)]
mod tests {
	use super::options_for_frame;
	use crate::dbs::{NoWriteFrame, Options};
	use crate::exec::config::ExecConfig;

	fn options() -> Options {
		Options::new(&ExecConfig::default())
	}

	/// A `SELECT` `PERMISSIONS` predicate runs in a frame that refuses every
	/// write (GHSA-66r2-5gwj-gxm2).
	#[test]
	fn the_permission_frame_forbids_writes() {
		let opt = options();
		let frame = options_for_frame(&opt, true, Some(NoWriteFrame::PermissionPredicate), None);
		assert_eq!(frame.no_write, Some(NoWriteFrame::PermissionPredicate));
		assert!(!frame.perms);
	}

	/// A `PERMISSIONS FOR create/update/delete` guard refuses effects as a
	/// read guard does, with enforcement off exactly as for a read guard.
	#[test]
	fn a_write_guards_frame_refuses_effects_with_enforcement_off() {
		let opt = options();
		let frame = options_for_frame(&opt, true, Some(NoWriteFrame::WritePermission), None);
		assert_eq!(frame.no_write, Some(NoWriteFrame::WritePermission));
		assert!(!frame.perms);
		assert!(!frame.caller_perms);
	}

	/// An event body reads with enforcement off and may write.
	#[test]
	fn an_event_frame_allows_a_write_with_enforcement_off() {
		let opt = options();
		let frame = options_for_frame(&opt, true, None, None);
		assert_eq!(frame.no_write, None);
		assert!(!frame.perms);
	}

	/// A computed body may not write either, but its reads stay
	/// permission-checked.
	#[test]
	fn the_computed_frame_forbids_writes_and_keeps_reads_checked() {
		let opt = options();
		let frame = options_for_frame(&opt, false, Some(NoWriteFrame::ComputedField), None);
		assert_eq!(frame.no_write, Some(NoWriteFrame::ComputedField));
		assert_eq!(frame.perms, opt.perms);
	}

	/// The two halves are independent: a computed body reached from inside a
	/// frame with enforcement already off keeps both.
	#[test]
	fn the_two_halves_of_a_frame_compose() {
		let opt = options();
		let frame = options_for_frame(&opt, true, Some(NoWriteFrame::ComputedField), None);
		assert_eq!(frame.no_write, Some(NoWriteFrame::ComputedField));
		assert!(!frame.perms);
	}

	/// A frame the caller's options already refuse in is never lifted: an
	/// event body reached from inside a read guard stays refused.
	#[test]
	fn an_inherited_refusal_is_not_lifted() {
		let opt = options().new_for_permission_predicate();
		let frame = options_for_frame(&opt, true, None, None);
		assert_eq!(frame.no_write, Some(NoWriteFrame::PermissionPredicate));
	}

	#[test]
	fn a_plain_frame_changes_nothing() {
		let opt = options();
		let frame = options_for_frame(&opt, false, None, None);
		assert_eq!(frame.no_write, None);
		assert_eq!(frame.perms, opt.perms);
		assert_eq!(frame.version, None);
	}

	/// An evaluation reached from a versioned statement reads at the
	/// statement's `VERSION`, in every frame.
	#[test]
	fn the_version_stamp_is_carried_into_every_frame() {
		let opt = options();
		assert_eq!(options_for_frame(&opt, false, None, Some(7)).version, Some(7));
		assert_eq!(
			options_for_frame(&opt, true, Some(NoWriteFrame::PermissionPredicate), Some(7)).version,
			Some(7)
		);
		assert_eq!(
			options_for_frame(&opt, false, Some(NoWriteFrame::ComputedField), Some(7)).version,
			Some(7)
		);
	}

	/// Without a stamp from the execution context, the options keep the
	/// version they already carry.
	#[test]
	fn no_stamp_keeps_the_options_version() {
		let opt = options().with_version(Some(3));
		assert_eq!(options_for_frame(&opt, false, None, None).version, Some(3));
	}
}
