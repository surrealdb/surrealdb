//! Lowering of the GQL data-modifying statements onto the [`MutationStage`] IR.
//!
//! `SET`/`REMOVE`/`DELETE` reference variables bound by the read body and lower
//! to [`MutationStage::Update`] / [`MutationStage::Delete`] over the resolved
//! binding. `INSERT` describes new nodes and edges; each node is classified as a
//! REFERENCE to a read-bound variable (no label, no props) or a NEW element
//! (declares a fresh binding, carries a label = table). The binding registry is
//! shared with the read body so references resolve and `RETURN` can project
//! `INSERT`-created variables.
//!
//! Rejections (the ledger): label mutations (`SET a:Label` / `REMOVE a:Label` —
//! a SurrealDB record belongs to exactly one table), mutating a group/path
//! variable, mutating an unbound variable, deleting a non-variable expression,
//! `SET a = {…}` that sets the reserved `id`/`in`/`out` keys, and an `INSERT`
//! node that is neither a labelled new node nor a bound-variable reference.

use reblessive::Stk;
use surrealdb_strand::TableName;

use crate::expr::match_plan::{
	BindingId, BindingKind, DetachMode, InsertEdgePlan, InsertNodePlan, InsertStage, MutationStage,
	UpdateData,
};
use crate::expr::{Expr, Idiom};
use crate::gql::ast::{
	DeleteStatement, DetachMode as AstDetachMode, GqlExpr, Ident, InsertEdgeDir, InsertNode,
	InsertStatement, MutationStatement, RemoveItem, RemoveStatement, SetItem, SetStatement,
};
use crate::gql::lower::binding::Registry;
use crate::gql::lower::expr::{Scope, lower_value};
use crate::syn::error::{SyntaxError, bail, syntax_error};

/// A new `INSERT` node awaiting property lowering: `(binding, table, raw props)`.
type PendingNode<'ast> = (BindingId, TableName, &'ast [(Ident, GqlExpr)]);
/// A new `INSERT` edge awaiting property lowering:
/// `(binding, table, from binding, to binding, raw props)`.
type PendingEdge<'ast> = (BindingId, TableName, BindingId, BindingId, &'ast [(Ident, GqlExpr)]);

/// Lowers one data-modifying statement into its write stage(s), extending the
/// registry in place: `INSERT` declares new node/edge bindings (so a trailing
/// `RETURN` and a later `MATCH` / property map can reference them). A `SET` or
/// `REMOVE` with several items may yield several stages.
pub(super) async fn lower_statement(
	stk: &mut Stk,
	registry: &mut Registry,
	stmt: &MutationStatement,
) -> Result<Vec<MutationStage>, SyntaxError> {
	let mut stages = Vec::new();
	match stmt {
		MutationStatement::Set(set) => lower_set(stk, registry, set, &mut stages).await?,
		MutationStatement::Remove(remove) => lower_remove(registry, remove, &mut stages)?,
		MutationStatement::Delete(delete) => lower_delete(registry, delete, &mut stages)?,
		MutationStatement::Insert(insert) => {
			let stage = lower_insert(stk, registry, insert).await?;
			stages.push(MutationStage::Insert(stage));
		}
	}
	Ok(stages)
}

/// Resolves a `SET`/`REMOVE`/`DELETE` target variable to its binding, rejecting
/// an unbound variable (via [`Registry::resolve`]) and a group/path variable
/// (which hold composite values, not a record to mutate).
fn resolve_target(registry: &Registry, ident: &Ident) -> Result<BindingId, SyntaxError> {
	let id = registry.resolve(ident)?;
	match registry.kind(id) {
		BindingKind::Node | BindingKind::Edge => Ok(id),
		BindingKind::EdgeGroup | BindingKind::Path => Err(syntax_error!(
			"Cannot mutate `{}`: a group or path variable holds a composite value, not a record",
			ident.name,
			@ident.span => "mutate a node or edge variable instead"
		)),
	}
}

/// Lowers a `SET` statement. Consecutive property assignments to the SAME
/// target merge into one [`UpdateData::Set`] (applied as a single UPDATE); a
/// `SET a = {…}` is its own [`UpdateData::Content`] stage; a label item is
/// rejected.
async fn lower_set(
	stk: &mut Stk,
	registry: &mut Registry,
	set: &SetStatement,
	stages: &mut Vec<MutationStage>,
) -> Result<(), SyntaxError> {
	let scope = Scope {
		registry,
		allow_aggregates: false,
	};
	let mut i = 0;
	while i < set.items.len() {
		match &set.items[i] {
			SetItem::Property {
				var,
				..
			} => {
				let target = resolve_target(registry, var)?;
				let target_name = var.name.clone();
				let mut assignments = Vec::new();
				// Merge the run of consecutive property items sharing this target.
				while let Some(SetItem::Property {
					var: next_var,
					prop,
					value,
					..
				}) = set.items.get(i)
				{
					if next_var.name != target_name {
						break;
					}
					// Reject the reserved keys here too — not only on the
					// `SET a = {…}` surface — so e.g. `SET k.out = …` does not
					// silently no-op (the native write path re-stamps an edge's
					// `in`/`out` and the record `id` after the SET applies).
					reject_reserved_key(prop)?;
					let value: Expr = lower_value(stk, value, &scope).await?.into();
					assignments.push((Idiom::field(prop.name.clone()), value));
					i += 1;
				}
				stages.push(MutationStage::Update {
					target,
					data: UpdateData::Set(assignments),
				});
			}
			SetItem::AllProperties {
				var,
				props,
				span,
			} => {
				let target = resolve_target(registry, var)?;
				reject_reserved_keys(props)?;
				let map = GqlExpr::Map(props.clone(), *span);
				let content: Expr = lower_value(stk, &map, &scope).await?.into();
				stages.push(MutationStage::Update {
					target,
					data: UpdateData::Content(content),
				});
				i += 1;
			}
			SetItem::Label {
				span,
				..
			} => return Err(label_mutation_rejected(*span)),
		}
	}
	Ok(())
}

/// Lowers a `REMOVE` statement. Consecutive property removals to the SAME
/// target merge into one [`UpdateData::Unset`]; a label item is rejected.
fn lower_remove(
	registry: &Registry,
	remove: &RemoveStatement,
	stages: &mut Vec<MutationStage>,
) -> Result<(), SyntaxError> {
	let mut i = 0;
	while i < remove.items.len() {
		match &remove.items[i] {
			RemoveItem::Property {
				var,
				..
			} => {
				let target = resolve_target(registry, var)?;
				let target_name = var.name.clone();
				let mut fields = Vec::new();
				while let Some(RemoveItem::Property {
					var: next_var,
					prop,
					..
				}) = remove.items.get(i)
				{
					if next_var.name != target_name {
						break;
					}
					fields.push(Idiom::field(prop.name.clone()));
					i += 1;
				}
				stages.push(MutationStage::Update {
					target,
					data: UpdateData::Unset(fields),
				});
			}
			RemoveItem::Label {
				span,
				..
			} => return Err(label_mutation_rejected(*span)),
		}
	}
	Ok(())
}

/// Lowers a `DELETE` statement: one [`MutationStage::Delete`] per item. Each
/// `deleteItem` must be a bound variable reference.
fn lower_delete(
	registry: &Registry,
	delete: &DeleteStatement,
	stages: &mut Vec<MutationStage>,
) -> Result<(), SyntaxError> {
	let detach = match delete.detach {
		AstDetachMode::Detach => DetachMode::Detach,
		AstDetachMode::NoDetach => DetachMode::NoDetach,
	};
	for item in &delete.items {
		let GqlExpr::Variable(ident) = item else {
			bail!(
				"DELETE expects a bound variable, e.g. `DELETE a`",
				@item.span() => "name a variable bound by the MATCH pattern"
			);
		};
		let target = resolve_target(registry, ident)?;
		stages.push(MutationStage::Delete {
			target,
			detach,
		});
	}
	Ok(())
}

/// Lowers an `INSERT` statement into an [`InsertStage`]. A two-pass walk:
/// pass 1 classifies and declares every node/edge binding (so cross-references
/// resolve); pass 2 lowers each element's property map against the full
/// registry. New nodes are emitted in path order (creation order); edges relate
/// once their endpoints exist.
async fn lower_insert(
	stk: &mut Stk,
	registry: &mut Registry,
	insert: &InsertStatement,
) -> Result<InsertStage, SyntaxError> {
	// Pass 1: classify nodes (NEW vs REFERENCE) and declare bindings.
	// A NEW node records `(binding, label, props)`; a reference records nothing
	// (it resolves to an existing binding at run time).
	let mut new_nodes: Vec<PendingNode> = Vec::new();
	let mut new_edges: Vec<PendingEdge> = Vec::new();

	for path in &insert.paths {
		let mut prev = classify_node(registry, &path.start, &mut new_nodes)?;
		for (edge, node) in &path.steps {
			let current = classify_node(registry, node, &mut new_nodes)?;
			let Some(label) = edge.label.as_ref() else {
				bail!(
					"An INSERT edge requires a label, e.g. `-[:knows]->`",
					@edge.span => "label the edge with its table"
				);
			};
			let binding = registry.declare_new_edge(edge.var.as_ref())?;
			// RELATE is always from -> through -> to; the arrow fixes the roles.
			let (from, to) = match edge.direction {
				InsertEdgeDir::Right => (prev, current),
				InsertEdgeDir::Left => (current, prev),
			};
			new_edges.push((binding, TableName::new(label.name.clone()), from, to, &edge.props));
			prev = current;
		}
	}

	// Pass 2: lower property maps against the registry (now carrying every
	// INSERT binding, so a node's props may reference an earlier node).
	let scope = Scope {
		registry,
		allow_aggregates: false,
	};
	let mut nodes = Vec::with_capacity(new_nodes.len());
	for (binding, label, props) in &new_nodes {
		let props = lower_props(stk, props, &scope).await?;
		nodes.push(InsertNodePlan {
			binding: *binding,
			label: label.clone(),
			props,
		});
	}
	let mut edges = Vec::with_capacity(new_edges.len());
	for (binding, label, from, to, props) in &new_edges {
		let props = lower_props(stk, props, &scope).await?;
		edges.push(InsertEdgePlan {
			binding: *binding,
			label: label.clone(),
			from: *from,
			to: *to,
			props,
		});
	}

	Ok(InsertStage {
		nodes,
		edges,
	})
}

/// Classifies an `INSERT` node and returns its binding id. A node with no label
/// and no properties is a REFERENCE to a read-bound variable; otherwise it is a
/// NEW node, which requires a label and is recorded in `new_nodes`.
fn classify_node<'ast>(
	registry: &mut Registry,
	node: &'ast InsertNode,
	new_nodes: &mut Vec<PendingNode<'ast>>,
) -> Result<BindingId, SyntaxError> {
	let has_label = node.label.is_some();
	let has_props = !node.props.is_empty();

	if !has_label && !has_props {
		// Reference candidate: a bound variable used as an edge endpoint.
		if let Some(ident) = node.var.as_ref()
			&& registry.try_lookup(&ident.name).is_some()
		{
			return registry.resolve_node_ref(ident);
		}
		bail!(
			"An INSERT node must declare a label `(a:Label)` or reference a variable bound by a \
			 preceding MATCH",
			@node.span => "give the node a label, or match the variable first"
		);
	}

	let Some(label) = node.label.as_ref() else {
		bail!(
			"A new INSERT node requires a label, e.g. `(a:Label {{…}})`",
			@node.span => "add the target table as the node's label"
		);
	};
	let binding = registry.declare_new_node(node.var.as_ref())?;
	new_nodes.push((binding, TableName::new(label.name.clone()), &node.props));
	Ok(binding)
}

/// Lowers a property map into a binding-row-scoped object expression (each value
/// is evaluated against the binding row at write time).
async fn lower_props(
	stk: &mut Stk,
	props: &[(Ident, GqlExpr)],
	scope: &Scope<'_>,
) -> Result<Expr, SyntaxError> {
	// Reuse the value lowering's object handling by lowering a synthetic map.
	let map = GqlExpr::Map(props.to_vec(), crate::syn::token::Span::empty());
	Ok(lower_value(stk, &map, scope).await?.into())
}

/// Rejects `SET a = {…}` that sets the reserved record keys (`id`, and an edge's
/// `in`/`out`), which the record identity / graph wiring owns.
fn reject_reserved_keys(props: &[(Ident, GqlExpr)]) -> Result<(), SyntaxError> {
	for (key, _) in props {
		reject_reserved_key(key)?;
	}
	Ok(())
}

/// Rejects a single reserved record key (`id`, or an edge's `in`/`out`) — used by
/// both the `SET a = {…}` surface and per-property `SET a.p = …`, so the two
/// agree (the native write path silently re-stamps these after the write).
fn reject_reserved_key(key: &Ident) -> Result<(), SyntaxError> {
	if matches!(key.name.as_str(), "id" | "in" | "out") {
		bail!(
			"`SET` cannot set the reserved `{}` key",
			key.name,
			@key.span => "the record id and edge endpoints are managed by the database"
		);
	}
	Ok(())
}

/// The label-mutation rejection (`SET a:Label` / `REMOVE a:Label`).
fn label_mutation_rejected(span: crate::syn::token::Span) -> SyntaxError {
	syntax_error!(
		"Label mutation is not supported: a record belongs to exactly one table",
		@span => "SurrealDB has no multi-label model; create the record in the target table instead"
	)
}
