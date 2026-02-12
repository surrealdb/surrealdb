//! Index function system for the streaming executor.
//!
//! This module provides traits for functions that are bound to WHERE clause
//! predicates via index infrastructure. Unlike scalar functions which operate
//! purely on their arguments, index functions reference a specific predicate
//! (e.g., a MATCHES clause) in the WHERE condition and need access to the
//! associated index at evaluation time.
//!
//! The match_ref argument in the user's query (e.g., the `1` in
//! `search::highlight('<b>', '</b>', 1)`) is extracted at plan time and
//! resolved to a `MatchContext` containing the index metadata. This context
//! is then passed to the function at evaluation time along with the
//! remaining runtime arguments.
//!
//! Examples: search::highlight, search::score, search::offsets

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use anyhow::Result;

use super::Signature;
use crate::exec::physical_expr::EvalContext;
use crate::exec::{BoxFut, ContextLevel, SendSyncRequirement};
use crate::expr::Kind;
use crate::expr::idiom::Idiom;
use crate::idx::ft::MatchRef;
use crate::idx::ft::fulltext::{FullTextIndex, QueryTerms, Scorer};
use crate::val::{TableName, Value};

/// A function that is bound to a WHERE clause predicate via index infrastructure.
///
/// Index functions differ from scalar functions in that they:
/// - Reference a specific WHERE clause predicate via a match_ref argument
/// - Need access to index infrastructure (e.g., FullTextIndex) at evaluation time
/// - Have the match_ref argument extracted at plan time, not passed at runtime
///
/// The match_ref argument position is declared by `match_ref_arg_index()`. The
/// planner extracts this argument from the AST, resolves it against the WHERE
/// clause's MATCHES operators, and creates a `MatchContext` that is passed to
/// the function at evaluation time.
pub trait IndexFunction: SendSyncRequirement + Debug {
	/// The fully qualified function name (e.g., "search::highlight", "search::score")
	fn name(&self) -> &'static str;

	/// The function signature describing arguments and return type.
	#[allow(unused)]
	fn signature(&self) -> Signature;

	/// Infer the return type given the argument types.
	///
	/// The default implementation returns the signature's return type.
	#[allow(unused)]
	fn return_type(&self, _arg_types: &[Kind]) -> Result<Kind> {
		Ok(self.signature().returns)
	}

	/// Which argument position contains the match_ref number.
	///
	/// This argument is extracted at plan time by the planner and is NOT
	/// passed to `invoke_async` as a runtime argument. The planner uses it
	/// to look up the corresponding MATCHES clause in the WHERE condition
	/// and build a `MatchContext`.
	fn match_ref_arg_index(&self) -> usize;

	/// The minimum context level required to execute this function.
	///
	/// Index functions typically need root context for transaction and
	/// index store access.
	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	/// Async invocation with index context.
	///
	/// # Arguments
	/// * `ctx` - The evaluation context with access to current row and parameters
	/// * `match_ctx` - The resolved MATCHES clause context with lazy index access
	/// * `args` - The evaluated function arguments, WITHOUT the match_ref argument
	///
	/// # Returns
	/// The computed value
	fn invoke_async<'a>(
		&'a self,
		ctx: &'a EvalContext<'_>,
		match_ctx: &'a MatchContext,
		args: Vec<Value>,
	) -> BoxFut<'a, Result<Value>>;
}

// =========================================================================
// MatchContext - resolved context for a single MATCHES clause
// =========================================================================

/// Resolved context for a single MATCHES clause, created at plan time.
///
/// This captures the field path and query string from a `WHERE field @N@ 'query'`
/// expression and provides lazy access to the associated full-text index
/// infrastructure. The expensive FullTextIndex/QueryTerms/Scorer are initialized
/// only on first use and then cached for all subsequent rows.
pub struct MatchContext {
	/// The field path from the left side of the MATCHES operator.
	pub idiom: Idiom,
	/// The search query string from the right side of the MATCHES operator.
	pub query: String,
	/// The table name for index lookup.
	pub table: TableName,
	/// Lazily initialized full-text index resources.
	ft_cache: tokio::sync::OnceCell<(FullTextIndex, QueryTerms, Option<Scorer>)>,
}

impl MatchContext {
	/// Create a new MatchContext from resolved MATCHES clause info.
	pub fn new(idiom: Idiom, query: String, table: TableName) -> Self {
		Self {
			idiom,
			query,
			table,
			ft_cache: tokio::sync::OnceCell::new(),
		}
	}

	/// Get or lazily initialize the full-text index resources.
	///
	/// On first call, this looks up the full-text index definition for the
	/// table/idiom, opens the FullTextIndex, extracts QueryTerms, and
	/// optionally creates a Scorer. Subsequent calls return the cached result.
	pub async fn ft_resources(
		&self,
		ctx: &EvalContext<'_>,
	) -> Result<&(FullTextIndex, QueryTerms, Option<Scorer>)> {
		self.ft_cache
			.get_or_try_init(|| async {
				use crate::catalog::providers::TableProvider;

				let frozen = ctx.exec_ctx.ctx();
				let root = ctx.exec_ctx.root();
				let opt = root
					.options
					.as_ref()
					.ok_or_else(|| anyhow::anyhow!("IndexFunction requires Options context"))?;
				let tx = ctx.txn();

				// Get namespace and database IDs from the execution context
				let db_ctx = ctx.exec_ctx.database().map_err(|e| {
					anyhow::anyhow!("IndexFunction requires database context: {}", e)
				})?;
				let ns_id = db_ctx.ns_ctx.ns.namespace_id;
				let db_id = db_ctx.db.database_id;

				// Find the full-text index for this table and idiom
				let indexes = tx.all_tb_indexes(ns_id, db_id, &self.table).await?;
				let index_def = indexes
					.iter()
					.find(|idx| {
						matches!(&idx.index, crate::catalog::Index::FullText(_))
							&& idx.cols.iter().any(|col| col.0 == self.idiom.0)
					})
					.ok_or_else(|| {
						anyhow::anyhow!(
							"No full-text index found for field {:?} on table {}",
							self.idiom,
							self.table
						)
					})?;

				let ft_params = match &index_def.index {
					crate::catalog::Index::FullText(params) => params,
					_ => unreachable!("Already checked for FullText above"),
				};

				let ikb = crate::idx::IndexKeyBase::new(
					ns_id,
					db_id,
					self.table.clone(),
					index_def.index_id,
				);

				// Open the full-text index
				let fti =
					FullTextIndex::new(frozen.get_index_stores(), tx.as_ref(), ikb, ft_params)
						.await?;

				// Extract query terms
				let query_terms = {
					let mut stack = reblessive::TreeStack::new();
					stack
						.enter(|stk| {
							fti.extract_querying_terms(stk, frozen, opt, self.query.clone())
						})
						.finish()
						.await?
				};

				// Create scorer if BM25 is configured
				let scorer = fti.new_scorer(frozen).await?;

				Ok((fti, query_terms, scorer))
			})
			.await
	}
}

impl Debug for MatchContext {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("MatchContext")
			.field("idiom", &self.idiom)
			.field("query", &self.query)
			.field("table", &self.table)
			.field("initialized", &self.ft_cache.initialized())
			.finish()
	}
}

// =========================================================================
// MatchesContext - planning-time map of all MATCHES clauses
// =========================================================================

/// Information about a single MATCHES clause extracted from the WHERE condition.
#[derive(Debug, Clone)]
pub struct MatchInfo {
	/// The field path from the left side of the MATCHES operator.
	pub idiom: Idiom,
	/// The search query string from the right side of the MATCHES operator.
	pub query: String,
}

/// Planning-time context mapping match_ref numbers to MATCHES clause info.
///
/// Built by analyzing the WHERE clause AST during query planning. Each entry
/// maps a match_ref number (e.g., `1` from `@1@`) to the idiom and query
/// string of the corresponding MATCHES operator.
#[derive(Debug, Clone)]
pub struct MatchesContext {
	matches: HashMap<MatchRef, MatchInfo>,
	/// The table name from the FROM clause, set during planning.
	table: Option<TableName>,
}

impl MatchesContext {
	/// Create a new empty MatchesContext.
	pub fn new() -> Self {
		Self {
			matches: HashMap::new(),
			table: None,
		}
	}

	/// Set the table name for index lookup.
	pub fn set_table(&mut self, table: TableName) {
		self.table = Some(table);
	}

	/// Get the table name.
	pub fn table(&self) -> Option<&TableName> {
		self.table.as_ref()
	}

	/// Insert a MATCHES clause entry.
	pub fn insert(&mut self, match_ref: MatchRef, info: MatchInfo) {
		self.matches.insert(match_ref, info);
	}

	/// Look up a MATCHES clause by its match_ref number.
	pub fn get(&self, match_ref: MatchRef) -> Option<&MatchInfo> {
		self.matches.get(&match_ref)
	}

	/// Get the first available MatchInfo (for when there's only one MATCHES clause).
	pub fn first(&self) -> Option<(MatchRef, &MatchInfo)> {
		self.matches.iter().next().map(|(&k, v)| (k, v))
	}

	/// Check if the context has no MATCHES entries.
	pub fn is_empty(&self) -> bool {
		self.matches.is_empty()
	}

	/// Create a MatchContext for a given match_ref, resolving against this context.
	///
	/// If the match_ref is found, creates a MatchContext with the resolved
	/// idiom, query, and table name. If not found and there's exactly one
	/// entry, falls back to that entry (common case: single MATCHES clause).
	pub fn resolve(&self, match_ref: MatchRef, table: TableName) -> Result<Arc<MatchContext>> {
		let info = self.get(match_ref).or_else(|| {
			// Fall back to the single entry if there's only one
			if self.matches.len() == 1 {
				self.first().map(|(_, info)| info)
			} else {
				None
			}
		});

		match info {
			Some(info) => {
				Ok(Arc::new(MatchContext::new(info.idiom.clone(), info.query.clone(), table)))
			}
			None => {
				// If there are no MATCHES clauses at all, provide a clear error
				if self.matches.is_empty() {
					Err(anyhow::anyhow!("no MATCHES clause found in WHERE condition"))
				} else {
					Err(anyhow::anyhow!(
						"no MATCHES clause found for match_ref {} (available: {:?})",
						match_ref,
						self.matches.keys().collect::<Vec<_>>()
					))
				}
			}
		}
	}
}

impl Default for MatchesContext {
	fn default() -> Self {
		Self::new()
	}
}
