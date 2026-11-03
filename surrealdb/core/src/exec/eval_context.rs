//! Borrowed execution frame for scalar VM units and engine host services.
//!
//! The current value and the document root are read only through the
//! accessors defined here, which answer a [`RowAccess`]: a row, no row, or no
//! row where a per-row evaluation would have held a KNN query vector's row
//! being scanned ([`ForbiddenRow`](crate::exec::context::ForbiddenRow)). The
//! caller reads it ([`RowAccess::read`], which refuses the forbidden row) or
//! hands the whole of it to a nested frame, so a forbidden row is never taken
//! for an absent one; field privacy makes a read that bypassed the accessors a
//! compile error anywhere outside this module.

use std::collections::HashMap;
use std::sync::Arc;

use surrealdb_strand::Strand;

use crate::dbs::Capabilities;
use crate::exec::ExecutionContext;
use crate::exec::context::{RowAccess, SessionInfo};
use crate::exec::operators::link_window::{LinkPool, RowsWindow};
use crate::kvs::Transaction;
use crate::val::{RecordId, Value};

/// Context for recursive tree-building via RepeatRecurse (@).
///
/// Set by `evaluate_recurse_iterative` and read by `evaluate_repeat_recurse`
/// during the two-phase iterative evaluation:
///
/// - **Discovery phase** (`discovery_sink` is `Some`): `@` writes its input values to the sink and
///   returns immediately, allowing BFS-style level discovery with no stack recursion.
/// - **Assembly phase** (`assembly_cache` is `Some`): `@` looks up pre-computed results from the
///   cache instead of recursing, enabling bottom-up tree construction with no stack recursion.
#[derive(Clone)]
pub struct RecursionCtx {
	/// Minimum recursion depth for path elimination.
	/// When a RepeatRecurse produces only dead-end values at a depth
	/// below this threshold, the entire sub-tree is eliminated (returns
	/// `Value::None` so that the parent's `clean_iteration` can filter it).
	pub min_depth: u32,
	/// Current recursion depth (set by the iterative evaluator for each level)
	pub depth: u32,
	/// Forward BFS discovery: The recursion step writes discovered values here.
	/// When `Some`, `@` writes its input values to this sink and returns
	/// immediately.
	///
	/// Uses `parking_lot::Mutex` (not `std::sync::Mutex`) because it does not
	/// poison on panic, matching the convention used by `DatabaseContext` caches.
	pub discovery_sink: Option<Arc<parking_lot::Mutex<Vec<Value>>>>,
	/// Backward assembly: The recursion step looks up pre-computed results here.
	/// Maps `value_hash` -> assembled result for the NEXT depth level.
	/// When `Some`, `@` does a cache lookup instead of recursing.
	pub assembly_cache: Option<Arc<HashMap<u64, Value>>>,
}

/// Evaluation context - what's available during expression evaluation.
///
/// This is a borrowed view into the execution context for expression evaluation.
/// It provides access to parameters, namespace/database names, and the current row
/// (for per-row expressions like filters and projections).
///
/// The `write_*` slots are the write path's bindings: `$before`, `$after`,
/// `$value`, `$input`, `$event` and `$reference`. A mutation evaluates field
/// clauses, permission predicates, event conditions and output expressions
/// that name this fixed, known set, and the set changes per field and per
/// record. They are typed slots rather than entries in `local_params` for two
/// reasons: a map allocated per field per record would sit in the write path's
/// innermost loop, and parameter resolution has to place them below
/// block-local `LET` and above the context's own parameter chain, which a
/// single map cannot express. [`EvalContext::write_binding`] is the name table
/// that resolution reads them through.
///
/// What each slot denotes depends on the stage that bound it: a field clause
/// binds that field's own values, while an event, output or permission stage
/// binds whole records. Every slot is `None` outside a write, and a body
/// evaluated in a context of its own (a user-defined function, a closure)
/// starts with every slot empty: a slot belongs to the stage that bound it
/// and does not cross a call boundary.
#[derive(Clone)]
pub struct EvalContext<'a> {
	pub exec_ctx: &'a ExecutionContext,

	/// Current row for per-row expressions (projections, filters).
	/// None when evaluating in "scalar context" (USE, LIMIT, TIMEOUT, etc.)
	///
	/// Private to this module: an absent value may be a KNN query vector's
	/// row being scanned, so it is read through [`Self::value_in_scope`] and
	/// set through the builders.
	current_value: Option<&'a Value>,

	/// Block-local parameters (LET bindings within current block scope).
	/// These shadow global parameters with the same name.
	pub local_params: Option<&'a HashMap<Strand, Value>>,

	/// Active recursion context for RepeatRecurse evaluation.
	/// Set by evaluate_recurse_iterative when the inner path contains .@ markers.
	pub recursion_ctx: Option<RecursionCtx>,

	/// Original document root for the current row.  Needed by IndexPart to
	/// evaluate dynamic key expressions (`[field]`, `[$param]`) against the
	/// document rather than the chain's current position.
	///
	/// Private to this module, as [`Self::current_value`] is: read through
	/// [`Self::root_document`] and [`Self::document`].
	document_root: Option<&'a Value>,

	/// Whether the value in scope is the element an element filter's body is
	/// applied to, set by [`Self::with_element`]. A closure invoked from here
	/// reads the element as its row, and the document root only where its
	/// body evaluates an index or a method argument.
	element_frame: bool,

	/// `$before`: the field, or the record, as it stood before the statement.
	pub write_before: Option<&'a Value>,

	/// `$after`: the field, or the record, as it stands once the statement's
	/// changes are applied.
	pub write_after: Option<&'a Value>,

	/// `$value`: the value the expression is about. A field clause binds that
	/// field's current value, which narrows as field evaluation descends; an
	/// event, output or reduction stage binds the record being evaluated.
	pub write_value: Option<&'a Value>,

	/// `$input`: the value the statement supplied, before a `DEFAULT` or
	/// `VALUE` clause rewrote it. `NONE` where it supplied nothing.
	pub write_input: Option<&'a Value>,

	/// `$event`: which action is running, as `CREATE`, `UPDATE` or `DELETE`.
	/// Present while an event's condition or body is evaluated, and for a
	/// live subscriber's expressions.
	pub write_event: Option<&'a Value>,

	/// `$reference`: the record whose deletion is running a reference's
	/// custom delete strategy. Present only while that strategy's expression
	/// is evaluated.
	pub write_reference: Option<&'a Value>,

	/// When true, RecordId dereferences use raw fetch (no permission checks).
	/// Set during permission predicate evaluation to prevent reentrant
	/// permission checks that would otherwise recurse infinitely on cyclic
	/// links with conditional table permissions.
	pub skip_fetch_perms: bool,

	/// The record currently having its computed fields evaluated, and the view
	/// a dereference of that same record resolves against. See
	/// [`ComputingRecord`].
	pub computing_record: Option<Arc<ComputingRecord>>,

	/// Expression-nesting depth recorded at plan time for a re-entry node (an
	/// `eval` / user-defined-function body). Carries the planner's depth across
	/// the analyse→execute→re-analyse boundary so that lazily-planned bodies and
	/// re-planned `eval` strings continue counting toward `max_computation_depth`
	/// instead of resetting to 0. Zero for ordinary per-row evaluation.
	pub plan_depth: u32,

	/// The windows a dereference of a collection of record links reads through, where the
	/// physical plan admitted them for the pipeline that evaluates it. `None` reads each link for
	/// itself, as every evaluation outside such a pipeline (a function body, a closure, a stored
	/// expression) does.
	pub(crate) link_windows: Option<&'a LinkPool>,

	/// The records the links of a batch of source rows name, read before the rows run, where the
	/// physical plan states that the pipeline's field dereferences take them. A dereference of a
	/// record id the window holds is served its bytes; any other reads for itself.
	pub(crate) link_ahead: Option<&'a RowsWindow>,
}

/// The record whose computed fields are being evaluated, and what a
/// dereference of that same record reads.
///
/// A field dereference that targets `rid` must not re-enter computed-field
/// evaluation for it: a body like `$this.id.prop` would recurse until the
/// stack gave out. The dereference is answered directly instead, from `stored`
/// when the caller supplied one and from storage otherwise.
pub struct ComputingRecord {
	/// The record being computed.
	pub rid: RecordId,
	/// The row's stored fields as this caller is permitted to read them.
	///
	/// SECURITY: a self-dereference answered from storage sees the record as
	/// written, not as reduced, so a computed field spelled `id.secret` returns
	/// a field the caller's `PERMISSIONS FOR select` removed. A caller that has
	/// reduced the row supplies it here so the dereference reads the same view
	/// the caller will receive.
	///
	/// Computed field names are deliberately absent from this view: they are
	/// not stored, so resolving one against it yields `NONE`, which is what a
	/// read of storage yields and what keeps the result independent of how far
	/// computed-field evaluation has progressed.
	///
	/// `None` restores the read from storage, for a caller that has applied no
	/// reduction and holds no view to offer.
	pub stored: Option<Value>,
}

impl<'a> EvalContext<'a> {
	/// Convert from ExecutionContext enum for expression evaluation.
	///
	/// For session-level scalar evaluation (USE, LIMIT, etc.)
	pub(crate) fn from_exec_ctx(exec_ctx: &'a ExecutionContext) -> Self {
		Self {
			exec_ctx,
			current_value: None,
			local_params: None,
			recursion_ctx: None,
			document_root: None,
			element_frame: false,
			// Nothing to carry: this builds a context out of the execution
			// context alone, with no row and no write in scope. A caller that
			// rebuilds a row's context on top of it restores the bindings
			// itself, the way it restores `local_params`.
			write_before: None,
			write_after: None,
			write_value: None,
			write_input: None,
			write_event: None,
			write_reference: None,
			skip_fetch_perms: exec_ctx.root().skip_fetch_perms,
			computing_record: exec_ctx.root().computing_record.clone(),
			plan_depth: 0,
			link_windows: None,
			link_ahead: None,
		}
	}

	/// For per-row evaluation (projections, filters)
	pub fn with_value(&self, value: &'a Value) -> Self {
		Self {
			exec_ctx: self.exec_ctx,
			current_value: Some(value),
			local_params: self.local_params,
			recursion_ctx: self.recursion_ctx.clone(),
			document_root: self.document_root,
			element_frame: false,
			// The bindings belong to the record being written, not to the
			// position within it that evaluation has reached, so narrowing
			// the current value leaves them alone.
			write_before: self.write_before,
			write_after: self.write_after,
			write_value: self.write_value,
			write_input: self.write_input,
			write_event: self.write_event,
			write_reference: self.write_reference,
			skip_fetch_perms: self.skip_fetch_perms,
			computing_record: self.computing_record.clone(),
			plan_depth: self.plan_depth,
			link_windows: self.link_windows,
			link_ahead: self.link_ahead,
		}
	}

	/// For per-row evaluation that also sets the document root
	/// (used at the top level of idiom evaluation to provide the
	/// original document for dynamic index expressions).
	pub fn with_value_and_doc(&self, value: &'a Value) -> Self {
		Self {
			exec_ctx: self.exec_ctx,
			current_value: Some(value),
			local_params: self.local_params,
			recursion_ctx: self.recursion_ctx.clone(),
			document_root: Some(value),
			element_frame: false,
			// Still the same record being written: a new document root
			// changes what an idiom resolves against, not what the statement
			// bound.
			write_before: self.write_before,
			write_after: self.write_after,
			write_value: self.write_value,
			write_input: self.write_input,
			write_event: self.write_event,
			write_reference: self.write_reference,
			skip_fetch_perms: self.skip_fetch_perms,
			computing_record: self.computing_record.clone(),
			plan_depth: self.plan_depth,
			link_windows: self.link_windows,
			link_ahead: self.link_ahead,
		}
	}

	/// For the evaluation of an element filter's body: `element` is the value in
	/// scope, and the document root stays the row the filter is applied over.
	pub fn with_element(&self, element: &'a Value) -> Self {
		Self {
			current_value: Some(element),
			element_frame: true,
			..self.clone()
		}
	}

	/// Whether the value in scope is an element filter's element
	/// ([`Self::with_element`]).
	#[inline]
	pub(crate) fn in_element(&self) -> bool {
		self.element_frame
	}

	/// Set the recursion context for RepeatRecurse evaluation.
	pub fn with_recursion_ctx(&self, ctx: RecursionCtx) -> Self {
		Self {
			exec_ctx: self.exec_ctx,
			current_value: self.current_value,
			local_params: self.local_params,
			recursion_ctx: Some(ctx),
			document_root: self.document_root,
			element_frame: self.element_frame,
			// A recursion context changes how `@` is evaluated, nothing
			// about which record is being written, so the bindings stay.
			write_before: self.write_before,
			write_after: self.write_after,
			write_value: self.write_value,
			write_input: self.write_input,
			write_event: self.write_event,
			write_reference: self.write_reference,
			skip_fetch_perms: self.skip_fetch_perms,
			computing_record: self.computing_record.clone(),
			plan_depth: self.plan_depth,
			link_windows: self.link_windows,
			link_ahead: self.link_ahead,
		}
	}

	/// The same evaluation in another execution context: one deriving from
	/// this one's (a parameter binding, an auth limit, a database level). The
	/// row, local parameters, write bindings and frame flags are kept.
	pub(crate) fn with_exec_ctx(&self, exec_ctx: &'a ExecutionContext) -> Self {
		Self {
			exec_ctx,
			..self.clone()
		}
	}

	/// The value in scope: the row a per-row expression is evaluated against,
	/// or the element a body is evaluated against. What a field, `$this`, a
	/// projection builtin or an index function reads, and the row a call hands
	/// its callee.
	#[inline]
	pub(crate) fn value_in_scope(&self) -> RowAccess<'a, &'a Value> {
		self.exec_ctx.row_access(self.current_value)
	}

	/// The document root: the row a path started from, which an unbound
	/// `$parent` falls back to.
	#[inline]
	pub(crate) fn root_document(&self) -> RowAccess<'a, &'a Value> {
		self.exec_ctx.row_access(self.document_root)
	}

	/// The document: the document root, or the value in scope where there is
	/// none. The row a nested frame is entered from — a subquery's `$parent`
	/// and `$this`, a closure's value in scope.
	#[inline]
	pub(crate) fn document(&self) -> RowAccess<'a, &'a Value> {
		self.exec_ctx.row_access(self.document_root.or(self.current_value))
	}

	/// Resolve a write-path binding by the name it is spelled with, or `None`
	/// where the name is not one of them or the slot is empty.
	///
	/// This is the whole name table for the bindings; parameter resolution
	/// consults it after block-local `LET` bindings and before the execution
	/// context's parameter chain.
	pub fn write_binding(&self, name: &str) -> Option<&'a Value> {
		match name {
			"before" => self.write_before,
			"after" => self.write_after,
			"value" => self.write_value,
			"input" => self.write_input,
			"event" => self.write_event,
			"reference" => self.write_reference,
			_ => None,
		}
	}

	// =========================================================================
	// Session accessors
	// =========================================================================

	/// Get the session information (if available).
	pub fn session(&self) -> Option<&SessionInfo> {
		self.exec_ctx.session()
	}

	// =========================================================================
	// Context accessors (shortcuts)
	// =========================================================================

	/// Get the transaction (delegates to FrozenContext).
	pub fn txn(&self) -> Arc<Transaction> {
		self.exec_ctx.txn()
	}

	/// Get the capabilities as an Arc.
	pub fn capabilities(&self) -> Arc<Capabilities> {
		self.exec_ctx.capabilities()
	}

	// =========================================================================
	// Capability checks
	// =========================================================================

	/// Check if a network target is allowed.
	///
	/// Returns an error if the URL is not allowed by the capabilities.
	#[cfg(feature = "http")]
	pub async fn check_allowed_net(&self, url: &url::Url) -> anyhow::Result<()> {
		use crate::dbs::capabilities::{Error as CapabilitiesError, NetTarget};
		use crate::err::Error;

		let capabilities = self.capabilities();

		// Check if the URL host is allowed
		let host = url.host().ok_or_else(|| Error::InvalidUrl(url.to_string()))?;

		let target = NetTarget::Host(NetTarget::canonical_host(host), url.port_or_known_default());

		// Check the domain name (if any) matches the allow list
		if !capabilities.matches_any_allow_net(&target) {
			return Err(CapabilitiesError::NetTargetNotAllowed(target.to_string()).into());
		}

		// Check against the deny list by hostname
		if capabilities.matches_any_deny_net(&target) {
			return Err(CapabilitiesError::NetTargetNotAllowed(target.to_string()).into());
		}

		// Resolve the domain name to IP addresses and check each against the deny list
		#[cfg(not(target_family = "wasm"))]
		let resolved = crate::net::resolve_net_target(&target).await?;
		#[cfg(target_family = "wasm")]
		let resolved = crate::net::resolve_net_target(&target)?;

		for t in &resolved {
			if capabilities.matches_any_deny_net(t) {
				return Err(CapabilitiesError::NetTargetNotAllowed(t.to_string()).into());
			}
		}

		Ok(())
	}

	/// Check if a function is allowed by capabilities.
	///
	/// Returns an error if the function is not allowed.
	pub fn check_allowed_function(&self, name: &str) -> anyhow::Result<()> {
		use crate::dbs::capabilities::Error as CapabilitiesError;

		// The query trace's own functions are written by the trace, never by the query.
		if crate::expr::logical::trace::is_internal(name) {
			return Ok(());
		}
		if !self.capabilities().allows_function_name(name) {
			return Err(CapabilitiesError::FunctionNotAllowed(name.to_string()).into());
		}
		Ok(())
	}

	/// Get the capabilities as an Arc (cloned from FrozenContext).
	#[allow(dead_code)]
	pub fn get_capabilities(&self) -> Arc<Capabilities> {
		self.exec_ctx.ctx().get_capabilities()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::root_ctx_with_options;

	#[test]
	fn a_row_access_tells_a_forbidden_row_from_an_absent_one() {
		let plain = root_ctx_with_options();
		let vector = plain.with_forbidden_row(Arc::from("$v"));
		let row = Value::from("row");
		let root = Value::from("root");

		// Outside a KNN query vector, no row in scope is absent, and reads as
		// none.
		let ctx = EvalContext::from_exec_ctx(&plain);
		assert!(matches!(ctx.value_in_scope(), RowAccess::Absent));
		assert!(matches!(ctx.document(), RowAccess::Absent));
		assert!(ctx.value_in_scope().read().unwrap().is_none());

		// Inside one, it is the row being scanned: every accessor answers it
		// as forbidden, a read of it fails, and only a callee run in the same
		// context takes it as none, meeting the forbidden row there itself.
		let ctx = EvalContext::from_exec_ctx(&vector);
		let refusal =
			"Invalid query: The KNN query vector cannot depend on the record being scanned: $v";
		assert!(matches!(ctx.value_in_scope(), RowAccess::Forbidden(_)));
		assert!(matches!(ctx.root_document(), RowAccess::Forbidden(_)));
		assert!(matches!(ctx.document(), RowAccess::Forbidden(_)));
		assert_eq!(ctx.value_in_scope().read().unwrap_err().to_string(), refusal);
		assert!(ctx.document().within_context().is_none());

		// A row in scope is read as it is, forbidden row or not, and the
		// document is the root where there is one.
		let ctx = EvalContext::from_exec_ctx(&vector).with_value(&row);
		assert_eq!(ctx.value_in_scope().read().unwrap(), Some(&row));
		assert_eq!(ctx.document().read().unwrap(), Some(&row));
		assert!(matches!(ctx.root_document(), RowAccess::Forbidden(_)));
		let ctx = EvalContext::from_exec_ctx(&vector).with_value_and_doc(&row);
		assert_eq!(ctx.root_document().read().unwrap(), Some(&row));
		let ctx = EvalContext {
			document_root: Some(&root),
			..EvalContext::from_exec_ctx(&vector).with_value(&row)
		};
		assert_eq!(ctx.document().read().unwrap(), Some(&root));
		assert_eq!(ctx.value_in_scope().read().unwrap(), Some(&row));
	}
}
