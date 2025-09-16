use std::collections::HashMap;
use std::collections::hash_map::Entry;

use anyhow::{Result, bail};
use futures::future::try_join_all;
use reblessive::tree::Stk;
use rust_decimal::Decimal;

use crate::catalog::providers::TableProvider;
use crate::catalog::{TableDefinition, ViewDefinition};
use crate::ctx::Context;
use crate::dbs::{Force, Options, Statement};
use crate::doc::{CursorDoc, Document};
use crate::err::Error;
use crate::expr::data::Assignment;
use crate::expr::paths::ID;
use crate::expr::statements::SelectStatement;
use crate::expr::statements::delete::DeleteStatement;
use crate::expr::statements::ifelse::IfelseStatement;
use crate::expr::statements::upsert::UpsertStatement;
use crate::expr::{
	AssignOperator, BinaryOperator, Cond, Data, Expr, Field, Fields, FlowResultExt as _, Function,
	FunctionCall, Groups, Ident, Idiom, Literal, Part,
};
use crate::val::record::FieldStats;
use crate::val::{Array, RecordId, RecordIdKey, TryAdd, TrySub, Value};

/// Represents a change to field statistics
#[derive(Clone, Debug)]
enum FieldStatsDelta {
	CountAdd(u64),
	CountSub(u64),
	SumAdd,
	SumSub,
	MeanAdd {
		value: Decimal,
	},
	MeanSub {
		value: Decimal,
	},
	MeanUpdate {
		old_value: Decimal,
		new_value: Decimal,
	},
	MinMaxAdd,
	MinMaxSub,
}

/// Combine two deltas for the same field
fn combine_field_deltas(first: FieldStatsDelta, second: FieldStatsDelta) -> FieldStatsDelta {
	match (first, second) {
		// Count operations
		(FieldStatsDelta::CountAdd(a), FieldStatsDelta::CountAdd(b)) => {
			FieldStatsDelta::CountAdd(a + b)
		}
		(FieldStatsDelta::CountSub(a), FieldStatsDelta::CountSub(b)) => {
			FieldStatsDelta::CountSub(a + b)
		}
		(FieldStatsDelta::CountAdd(a), FieldStatsDelta::CountSub(b)) => {
			if a >= b {
				FieldStatsDelta::CountAdd(a - b)
			} else {
				FieldStatsDelta::CountSub(b - a)
			}
		}
		(FieldStatsDelta::CountSub(a), FieldStatsDelta::CountAdd(b)) => {
			if b >= a {
				FieldStatsDelta::CountAdd(b - a)
			} else {
				FieldStatsDelta::CountSub(a - b)
			}
		}

		// Sum operations
		(FieldStatsDelta::SumAdd, FieldStatsDelta::SumSub) => FieldStatsDelta::SumAdd, /* No net
		                                                                                 * change
		                                                                                 * in count
		                                                                                 * for sum */
		(FieldStatsDelta::SumSub, FieldStatsDelta::SumAdd) => FieldStatsDelta::SumAdd, /* No net
		                                                                                 * change
		                                                                                 * in count
		                                                                                 * for sum */

		// Mean operations
		(
			FieldStatsDelta::MeanSub {
				value: v1,
			},
			FieldStatsDelta::MeanAdd {
				value: v2,
			},
		) => {
			// This represents an UPDATE: remove old value, add new value
			// Net effect: sum changes by (v2 - v1), count unchanged
			FieldStatsDelta::MeanUpdate {
				old_value: v1,
				new_value: v2,
			}
		}
		(
			FieldStatsDelta::MeanAdd {
				value: v1,
			},
			FieldStatsDelta::MeanSub {
				value: v2,
			},
		) => {
			// This shouldn't happen in normal operation (sub before add), but handle it
			FieldStatsDelta::MeanUpdate {
				old_value: v2,
				new_value: v1,
			}
		}
		(
			FieldStatsDelta::MeanAdd {
				value: v1,
			},
			FieldStatsDelta::MeanAdd {
				value: v2,
			},
		) => FieldStatsDelta::MeanAdd {
			value: v1 + v2,
		},
		(
			FieldStatsDelta::MeanSub {
				value: v1,
			},
			FieldStatsDelta::MeanSub {
				value: v2,
			},
		) => FieldStatsDelta::MeanSub {
			value: v1 + v2,
		},

		// MinMax operations
		(FieldStatsDelta::MinMaxAdd, FieldStatsDelta::MinMaxSub) => FieldStatsDelta::MinMaxAdd, /* No net change */
		(FieldStatsDelta::MinMaxSub, FieldStatsDelta::MinMaxAdd) => FieldStatsDelta::MinMaxAdd, /* No net change */

		// Default case - shouldn't happen in normal operation but handle gracefully
		(first, _) => first,
	}
}

/// Apply a delta to existing field stats
fn apply_field_stats_delta(
	existing: Option<FieldStats>,
	delta: FieldStatsDelta,
) -> Option<FieldStats> {
	match (existing, delta) {
		// Count operations
		(Some(FieldStats::Count(count)), FieldStatsDelta::CountAdd(delta)) => {
			Some(FieldStats::Count(count + delta))
		}
		(Some(FieldStats::Count(count)), FieldStatsDelta::CountSub(delta)) => {
			let new_count = count.saturating_sub(delta);
			if new_count == 0 {
				None
			} else {
				Some(FieldStats::Count(new_count))
			}
		}
		(None, FieldStatsDelta::CountAdd(delta)) => Some(FieldStats::Count(delta)),
		(None, FieldStatsDelta::CountSub(_)) => None, // Can't subtract from nothing

		// Sum operations
		(
			Some(FieldStats::Sum {
				count,
			}),
			FieldStatsDelta::SumAdd,
		) => Some(FieldStats::Sum {
			count: count + 1,
		}),
		(
			Some(FieldStats::Sum {
				count,
			}),
			FieldStatsDelta::SumSub,
		) => {
			let new_count = count.saturating_sub(1);
			if new_count == 0 {
				None
			} else {
				Some(FieldStats::Sum {
					count: new_count,
				})
			}
		}
		(None, FieldStatsDelta::SumAdd) => Some(FieldStats::Sum {
			count: 1,
		}),
		(None, FieldStatsDelta::SumSub) => None,

		// Mean operations
		(
			Some(FieldStats::Mean {
				sum,
				count,
			}),
			FieldStatsDelta::MeanAdd {
				value,
			},
		) => Some(FieldStats::Mean {
			sum: sum + value,
			count: count + 1,
		}),
		(
			Some(FieldStats::Mean {
				sum,
				count,
			}),
			FieldStatsDelta::MeanSub {
				value,
			},
		) => {
			let new_count = count.saturating_sub(1);
			if new_count == 0 {
				None
			} else {
				Some(FieldStats::Mean {
					sum: sum - value,
					count: new_count,
				})
			}
		}
		(
			Some(FieldStats::Mean {
				sum,
				count,
			}),
			FieldStatsDelta::MeanUpdate {
				old_value,
				new_value,
			},
		) => {
			// For UPDATE: change sum by (new_value - old_value), keep count the same
			Some(FieldStats::Mean {
				sum: sum - old_value + new_value,
				count,
			})
		}
		(
			None,
			FieldStatsDelta::MeanAdd {
				value,
			},
		) => Some(FieldStats::Mean {
			sum: value,
			count: 1,
		}),
		(
			None,
			FieldStatsDelta::MeanSub {
				..
			},
		) => None,
		(
			None,
			FieldStatsDelta::MeanUpdate {
				new_value,
				..
			},
		) => Some(FieldStats::Mean {
			sum: new_value,
			count: 1,
		}),

		// MinMax operations
		(
			Some(FieldStats::MinMax {
				count,
			}),
			FieldStatsDelta::MinMaxAdd,
		) => Some(FieldStats::MinMax {
			count: count + 1,
		}),
		(
			Some(FieldStats::MinMax {
				count,
			}),
			FieldStatsDelta::MinMaxSub,
		) => {
			let new_count = count.saturating_sub(1);
			if new_count == 0 {
				None
			} else {
				Some(FieldStats::MinMax {
					count: new_count,
				})
			}
		}
		(None, FieldStatsDelta::MinMaxAdd) => Some(FieldStats::MinMax {
			count: 1,
		}),
		(None, FieldStatsDelta::MinMaxSub) => None,

		// Mismatched operations - ignore (shouldn't happen)
		(existing, _) => existing,
	}
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Action {
	Create,
	Update,
	Delete,
}

#[derive(Debug, Eq, PartialEq)]
enum FieldAction {
	Add,
	Sub,
	UpdateAdd, // Add part of an UPDATE (record stays in same group)
	UpdateSub, // Sub part of an UPDATE (record stays in same group)
}

struct FieldDataContext<'a> {
	ft: &'a TableDefinition,
	act: FieldAction,
	view: &'a ViewDefinition,
	groups: &'a Groups,
	group_ids: Vec<Value>,
	doc: &'a CursorDoc,
}

/// utlity function for `OR`ing expressions together, modifies accum to be the
/// expression of all `new`'s OR'ed together.
fn accumulate_delete_expr(accum: &mut Option<Expr>, new: Expr) {
	match accum.take() {
		Some(old) => {
			*accum = Some(Expr::Binary {
				left: Box::new(old),
				op: BinaryOperator::Or,
				right: Box::new(new),
			});
		}
		None => *accum = Some(new),
	}
}

/// Accumulate delete expressions from one Option into another
fn accumulate_all_delete_expr(accum: &mut Option<Expr>, new: Option<Expr>) {
	if let Some(expr) = new {
		accumulate_delete_expr(accum, expr);
	}
}

/// Merge metadata deltas from one HashMap into another, combining deltas for the same field
fn merge_metadata_deltas(
	target: &mut HashMap<String, FieldStatsDelta>,
	source: HashMap<String, FieldStatsDelta>,
) {
	for (field_name, delta) in source {
		match target.entry(field_name) {
			Entry::Occupied(mut occupied_entry) => {
				// Temporarly replace the value to take ownership
				let existing = occupied_entry.insert(FieldStatsDelta::SumAdd);
				occupied_entry.insert(combine_field_deltas(existing, delta));
			}
			Entry::Vacant(vacant_entry) => {
				vacant_entry.insert(delta);
			}
		}
	}
}

impl Document {
	/// Processes any DEFINE TABLE AS clauses which
	/// have been defined for the table which this
	/// record belongs to. This functions loops
	/// through the tables and processes them all
	/// within the currently running transaction.
	pub(super) async fn process_table_views(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<()> {
		// Check import
		if opt.import {
			return Ok(());
		}
		// Was this force targeted at a specific foreign table?
		let targeted_force = matches!(opt.force, Force::Table(_));
		// Collect foreign tables or skip
		let fts = match &opt.force {
			Force::Table(tb)
				if tb.first().is_some_and(|tb| {
					tb.view.as_ref().is_some_and(|v| {
						self.id.as_ref().is_some_and(|id| {
							v.what.iter().any(|p| p.as_str() == id.table.as_str())
						})
					})
				}) =>
			{
				tb.clone()
			}
			Force::All => self.ft(ctx, opt).await?,
			_ if self.changed() => self.ft(ctx, opt).await?,
			_ => return Ok(()),
		};
		// Don't run permissions
		let opt = &opt.new_with_perms(false);
		// Get the record id
		let rid = self.id()?;
		// Get the query action
		let act = if stm.is_delete() {
			Action::Delete
		} else if self.is_new() {
			Action::Create
		} else {
			Action::Update
		};
		// Loop through all foreign table statements
		for ft in fts.iter() {
			// Get the table definition
			let Some(tb) = ft.view.as_ref() else {
				fail!("Table stored as view table did not have a view");
			};
			// Check if there is a GROUP BY clause
			match &tb.groups {
				// There is a GROUP BY clause specified
				Some(group) => {
					// Check if a WHERE clause is specified
					match &tb.cond {
						// There is a WHERE clause specified
						Some(cond) => {
							// Get the group IDs for the initial and current values
							let initial_group_ids = if !targeted_force && act != Action::Create {
								if stk
									.run(|stk| cond.compute(stk, ctx, opt, Some(&self.initial)))
									.await
									.catch_return()?
									.is_truthy()
								{
									Some(
										Self::get_group_ids(stk, ctx, opt, group, &self.initial)
											.await?,
									)
								} else {
									None
								}
							} else {
								None
							};

							let current_group_ids = if act != Action::Delete {
								if stk
									.run(|stk| cond.compute(stk, ctx, opt, Some(&self.current)))
									.await
									.catch_return()?
									.is_truthy()
								{
									Some(
										Self::get_group_ids(stk, ctx, opt, group, &self.current)
											.await?,
									)
								} else {
									None
								}
							} else {
								None
							};

							// Check if the groups are different (record moved between groups)
							let groups_changed = match (&initial_group_ids, &current_group_ids) {
								(Some(initial), Some(current)) => initial != current,
								_ => false,
							};

							if groups_changed {
								// Handle removal from old group
								if let Some(initial_ids) = initial_group_ids {
									let mut old_set_ops = Vec::new();
									let mut old_del_ops = None;
									let mut old_metadata_deltas = HashMap::new();

									let fdc = FieldDataContext {
										ft,
										act: FieldAction::Sub,
										view: tb,
										groups: group,
										group_ids: initial_ids.clone(),
										doc: &self.initial,
									};
									let (set_ops, del_ops, metadata_deltas) =
										self.fields(stk, ctx, opt, &fdc).await?;
									old_set_ops.extend(set_ops);
									accumulate_all_delete_expr(&mut old_del_ops, del_ops);
									merge_metadata_deltas(
										&mut old_metadata_deltas,
										metadata_deltas,
									);

									if !old_metadata_deltas.is_empty() || !old_set_ops.is_empty() {
										let rid = RecordId {
											table: ft.name.clone(),
											key: RecordIdKey::Array(Array(initial_ids)),
										};
										self.handle_record_with_metadata(
											stk,
											ctx,
											opt,
											&rid,
											old_set_ops,
											old_del_ops,
											old_metadata_deltas,
										)
										.await?;
									}
								}

								// Handle addition to new group
								if let Some(current_ids) = current_group_ids {
									let mut new_set_ops = Vec::new();
									let mut new_del_ops = None;
									let mut new_metadata_deltas = HashMap::new();

									let fdc = FieldDataContext {
										ft,
										act: FieldAction::Add,
										view: tb,
										groups: group,
										group_ids: current_ids.clone(),
										doc: &self.current,
									};
									let (set_ops, del_ops, metadata_deltas) =
										self.fields(stk, ctx, opt, &fdc).await?;
									new_set_ops.extend(set_ops);
									accumulate_all_delete_expr(&mut new_del_ops, del_ops);
									merge_metadata_deltas(
										&mut new_metadata_deltas,
										metadata_deltas,
									);

									if !new_metadata_deltas.is_empty() || !new_set_ops.is_empty() {
										let rid = RecordId {
											table: ft.name.clone(),
											key: RecordIdKey::Array(Array(current_ids)),
										};
										self.handle_record_with_metadata(
											stk,
											ctx,
											opt,
											&rid,
											new_set_ops,
											new_del_ops,
											new_metadata_deltas,
										)
										.await?;
									}
								}
							} else {
								// Groups didn't change, handle normally
								let mut all_set_ops = Vec::new();
								let mut all_del_ops = None;
								let mut all_metadata_deltas = HashMap::new();

								// Determine the action based on document state
								// - If document hasn't changed (initial == current), it's initial
								//   population → only Add
								// - If document has changed and both groups exist, it's an UPDATE →
								//   UpdateSub + UpdateAdd
								// - If only initial group exists, it's a DELETE → Sub
								// - If only current group exists, it's a CREATE → Add

								let doc_changed = self.changed();
								let has_initial = initial_group_ids.is_some();
								let has_current = current_group_ids.is_some();

								// Process the old value if needed
								if has_initial && (doc_changed || !has_current) {
									if let Some(initial_ids) = &initial_group_ids {
										let act = if has_current && doc_changed {
											FieldAction::UpdateSub
										} else {
											FieldAction::Sub
										};

										let fdc = FieldDataContext {
											ft,
											act,
											view: tb,
											groups: group,
											group_ids: initial_ids.clone(),
											doc: &self.initial,
										};
										let (set_ops, del_ops, metadata_deltas) =
											self.fields(stk, ctx, opt, &fdc).await?;
										all_set_ops.extend(set_ops);
										accumulate_all_delete_expr(&mut all_del_ops, del_ops);
										merge_metadata_deltas(
											&mut all_metadata_deltas,
											metadata_deltas,
										);
									}
								}

								// Process the new value if it exists
								if let Some(current_ids) = &current_group_ids {
									let act = if has_initial && doc_changed {
										FieldAction::UpdateAdd
									} else {
										FieldAction::Add
									};

									let fdc = FieldDataContext {
										ft,
										act,
										view: tb,
										groups: group,
										group_ids: current_ids.clone(),
										doc: &self.current,
									};
									let (set_ops, del_ops, metadata_deltas) =
										self.fields(stk, ctx, opt, &fdc).await?;
									all_set_ops.extend(set_ops);
									accumulate_all_delete_expr(&mut all_del_ops, del_ops);
									merge_metadata_deltas(
										&mut all_metadata_deltas,
										metadata_deltas,
									);
								}

								// Apply all collected changes to the appropriate group
								if !all_metadata_deltas.is_empty() || !all_set_ops.is_empty() {
									// Use current_group_ids if available, otherwise use
									// initial_group_ids
									let group_ids =
										current_group_ids.or(initial_group_ids).unwrap();
									let rid = RecordId {
										table: ft.name.clone(),
										key: RecordIdKey::Array(Array(group_ids)),
									};
									self.handle_record_with_metadata(
										stk,
										ctx,
										opt,
										&rid,
										all_set_ops,
										all_del_ops,
										all_metadata_deltas,
									)
									.await?;
								}
							}
						}
						// No WHERE clause is specified
						None => {
							// Get the group IDs for initial and current values
							let initial_group_ids = if !targeted_force
								&& (act == Action::Delete || act == Action::Update)
							{
								Some(
									Self::get_group_ids(stk, ctx, opt, group, &self.initial)
										.await?,
								)
							} else {
								None
							};

							let current_group_ids =
								if act == Action::Create || act == Action::Update {
									Some(
										Self::get_group_ids(stk, ctx, opt, group, &self.current)
											.await?,
									)
								} else {
									None
								};

							// Check if the groups are different (record moved between groups)
							let groups_changed = match (&initial_group_ids, &current_group_ids) {
								(Some(initial), Some(current)) => initial != current,
								_ => false,
							};

							if groups_changed {
								// Handle removal from old group
								if let Some(initial_ids) = initial_group_ids {
									let mut old_set_ops = Vec::new();
									let mut old_del_ops = None;
									let mut old_metadata_deltas = HashMap::new();

									let fdc = FieldDataContext {
										ft,
										act: FieldAction::Sub,
										view: tb,
										groups: group,
										group_ids: initial_ids.clone(),
										doc: &self.initial,
									};
									let (set_ops, del_ops, metadata_deltas) =
										self.fields(stk, ctx, opt, &fdc).await?;
									old_set_ops.extend(set_ops);
									accumulate_all_delete_expr(&mut old_del_ops, del_ops);
									merge_metadata_deltas(
										&mut old_metadata_deltas,
										metadata_deltas,
									);

									if !old_metadata_deltas.is_empty() || !old_set_ops.is_empty() {
										let rid = RecordId {
											table: ft.name.clone(),
											key: RecordIdKey::Array(Array(initial_ids)),
										};
										self.handle_record_with_metadata(
											stk,
											ctx,
											opt,
											&rid,
											old_set_ops,
											old_del_ops,
											old_metadata_deltas,
										)
										.await?;
									}
								}

								// Handle addition to new group
								if let Some(current_ids) = current_group_ids {
									let mut new_set_ops = Vec::new();
									let mut new_del_ops = None;
									let mut new_metadata_deltas = HashMap::new();

									let fdc = FieldDataContext {
										ft,
										act: FieldAction::Add,
										view: tb,
										groups: group,
										group_ids: current_ids.clone(),
										doc: &self.current,
									};
									let (set_ops, del_ops, metadata_deltas) =
										self.fields(stk, ctx, opt, &fdc).await?;
									new_set_ops.extend(set_ops);
									accumulate_all_delete_expr(&mut new_del_ops, del_ops);
									merge_metadata_deltas(
										&mut new_metadata_deltas,
										metadata_deltas,
									);

									if !new_metadata_deltas.is_empty() || !new_set_ops.is_empty() {
										let rid = RecordId {
											table: ft.name.clone(),
											key: RecordIdKey::Array(Array(current_ids)),
										};
										self.handle_record_with_metadata(
											stk,
											ctx,
											opt,
											&rid,
											new_set_ops,
											new_del_ops,
											new_metadata_deltas,
										)
										.await?;
									}
								}
							} else {
								// Groups didn't change, handle normally
								let mut all_set_ops = Vec::new();
								let mut all_del_ops = None;
								let mut all_metadata_deltas = HashMap::new();

								// Check if this is an UPDATE (both old and new values exist)
								let is_update =
									initial_group_ids.is_some() && current_group_ids.is_some();

								// Process the old value
								if let Some(initial_ids) = &initial_group_ids {
									let fdc = FieldDataContext {
										ft,
										act: if is_update {
											FieldAction::UpdateSub
										} else {
											FieldAction::Sub
										},
										view: tb,
										groups: group,
										group_ids: initial_ids.clone(),
										doc: &self.initial,
									};
									let (set_ops, del_ops, metadata_deltas) =
										self.fields(stk, ctx, opt, &fdc).await?;
									all_set_ops.extend(set_ops);
									accumulate_all_delete_expr(&mut all_del_ops, del_ops);
									merge_metadata_deltas(
										&mut all_metadata_deltas,
										metadata_deltas,
									);
								}

								// Process the new value
								if let Some(current_ids) = &current_group_ids {
									let fdc = FieldDataContext {
										ft,
										act: if is_update {
											FieldAction::UpdateAdd
										} else {
											FieldAction::Add
										},
										view: tb,
										groups: group,
										group_ids: current_ids.clone(),
										doc: &self.current,
									};
									let (set_ops, del_ops, metadata_deltas) =
										self.fields(stk, ctx, opt, &fdc).await?;
									all_set_ops.extend(set_ops);
									accumulate_all_delete_expr(&mut all_del_ops, del_ops);
									merge_metadata_deltas(
										&mut all_metadata_deltas,
										metadata_deltas,
									);
								}

								// Apply all collected changes to the appropriate group
								if !all_metadata_deltas.is_empty() || !all_set_ops.is_empty() {
									// Use current_group_ids if available, otherwise use
									// initial_group_ids
									let group_ids =
										current_group_ids.or(initial_group_ids).unwrap();
									let rid = RecordId {
										table: ft.name.clone(),
										key: RecordIdKey::Array(Array(group_ids)),
									};
									self.handle_record_with_metadata(
										stk,
										ctx,
										opt,
										&rid,
										all_set_ops,
										all_del_ops,
										all_metadata_deltas,
									)
									.await?;
								}
							}
						}
					}
				}
				// No GROUP BY clause is specified
				None => {
					// Set the current record id
					let rid = RecordId {
						table: ft.name.clone(),
						key: rid.key.clone(),
					};
					// Check if a WHERE clause is specified
					match &tb.cond {
						// There is a WHERE clause specified
						Some(cond) => {
							match stk
								.run(|stk| cond.compute(stk, ctx, opt, Some(&self.current)))
								.await
								.catch_return()?
							{
								v if v.is_truthy() => {
									// Define the statement
									match act {
										// Delete the value in the table
										Action::Delete => {
											let stm = DeleteStatement {
												what: vec![Expr::Literal(Literal::RecordId(
													rid.into_literal(),
												))],
												..DeleteStatement::default()
											};
											// Execute the statement
											stm.compute(stk, ctx, opt, None).await?;
										}
										// Update the value in the table
										_ => {
											let stm = UpsertStatement {
												what: vec![Expr::Literal(Literal::RecordId(
													rid.into_literal(),
												))],
												data: Some(
													self.full(stk, ctx, opt, &tb.fields).await?,
												),
												..UpsertStatement::default()
											};
											// Execute the statement
											stm.compute(stk, ctx, opt, None).await?;
										}
									};
								}
								_ => {
									// Delete the value in the table
									let stm = DeleteStatement {
										what: vec![Expr::Literal(Literal::RecordId(
											rid.into_literal(),
										))],
										..DeleteStatement::default()
									};
									// Execute the statement
									stm.compute(stk, ctx, opt, None).await?;
								}
							}
						}
						// No WHERE clause is specified
						None => {
							// Define the statement
							match act {
								// Delete the value in the table
								Action::Delete => {
									let stm = DeleteStatement {
										what: vec![Expr::Literal(Literal::RecordId(
											rid.into_literal(),
										))],
										..DeleteStatement::default()
									};
									// Execute the statement
									stm.compute(stk, ctx, opt, None).await?;
								}
								// Update the value in the table
								_ => {
									let stm = UpsertStatement {
										what: vec![Expr::Literal(Literal::RecordId(
											rid.into_literal(),
										))],
										data: Some(self.full(stk, ctx, opt, &tb.fields).await?),
										..UpsertStatement::default()
									};
									// Execute the statement
									stm.compute(stk, ctx, opt, None).await?;
								}
							};
						}
					}
				}
			}
		}
		// Carry on
		Ok(())
	}

	async fn get_group_ids(
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		group: &Groups,
		doc: &CursorDoc,
	) -> Result<Vec<Value>> {
		Ok(stk
			.scope(|scope| {
				try_join_all(group.iter().map(|v| {
					scope.run(|stk| async {
						v.compute(stk, ctx, opt, Some(doc)).await.catch_return()
					})
				}))
			})
			.await?
			.into_iter()
			.collect::<Vec<_>>())
	}

	//
	async fn full(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		exp: &Fields,
	) -> Result<Data> {
		let mut data = exp.compute(stk, ctx, opt, Some(&self.current), false).await?;
		data.cut(ID.as_ref());
		Ok(Data::ReplaceExpression(data.into_literal()))
	}

	#[allow(clippy::too_many_arguments)]
	async fn handle_record_with_metadata(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		rid: &RecordId,
		set_ops: Vec<Assignment>,
		del_ops: Option<Expr>,
		metadata_deltas: HashMap<String, FieldStatsDelta>,
	) -> Result<()> {
		use crate::expr::FlowResultExt as _;

		// Get NS & DB identifiers
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;

		// Get the transaction
		let txn = ctx.tx();

		// Retrieve the existing record (if any)
		let record = txn.get_record(ns, db, &rid.table, &rid.key, None).await?;
		let mut record = (*record).clone(); // Convert from Arc to owned

		// Apply field assignments to record data
		if !set_ops.is_empty() {
			// Create a CursorDoc from the current record for expression evaluation
			let doc = CursorDoc::new(Some(rid.clone().into()), None, record.clone());

			for assignment in set_ops {
				// Compute the value with the record as context (needed for IF ELSE expressions in
				// min/max)
				let val =
					assignment.value.compute(stk, ctx, opt, Some(&doc)).await.catch_return()?;
				// Apply the assignment to the record data based on the operator
				match assignment.operator {
					AssignOperator::Assign => {
						record.data.to_mut().put(&assignment.place, val);
					}
					AssignOperator::Add => {
						let existing = record.data.as_ref().pick(&assignment.place);
						let new_val = if existing.is_none() {
							val
						} else {
							existing.try_add(val)?
						};
						record.data.to_mut().put(&assignment.place, new_val);
					}
					AssignOperator::Subtract => {
						let existing = record.data.as_ref().pick(&assignment.place);
						let new_val = if existing.is_none() {
							// For subtraction on None, treat as 0 - val
							Value::from(0).try_sub(val)?
						} else {
							existing.try_sub(val)?
						};
						record.data.to_mut().put(&assignment.place, new_val);
					}
					_ => {
						// For other operators, just apply the value for now
						record.data.to_mut().put(&assignment.place, val);
					}
				}
			}
		}

		// Apply metadata deltas and update field values where needed
		let mut any_field_stats_removed = false;
		for (field_name, delta) in metadata_deltas {
			// Get the existing stats for this field
			let existing_stats = record.get_field_stats(&field_name).cloned();

			// Apply the delta to get new stats
			if let Some(new_stats) = apply_field_stats_delta(existing_stats, delta.clone()) {
				record.set_field_stats(field_name.clone(), new_stats.clone());

				// For mean calculations, we need to update the actual field value too
				if let FieldStats::Mean {
					sum,
					count,
				} = &new_stats
				{
					if *count > 0 {
						let mean_value = Value::from(*sum / rust_decimal::Decimal::from(*count));
						// Convert field name to Parts array for put method
						let parts = vec![Part::field(field_name.clone()).unwrap()];
						record.data.to_mut().put(&parts, mean_value);
					}
				}
			} else {
				// If delta results in None, remove the field stats (count reached 0)
				record.remove_field_stats(&field_name);
				any_field_stats_removed = true;

				// Also remove the field value if it was a computed aggregation
				if matches!(
					delta,
					FieldStatsDelta::MeanAdd { .. }
						| FieldStatsDelta::MeanSub { .. }
						| FieldStatsDelta::MeanUpdate { .. }
				) {
					let parts = vec![Part::field(field_name.clone()).unwrap()];
					record.data.to_mut().put(&parts, Value::None);
				}
			}
		}

		// Check delete condition
		let mut should_delete = if let Some(del_condition) = del_ops {
			let doc = CursorDoc::new(Some(rid.clone().into()), None, record.clone());
			del_condition.compute(stk, ctx, opt, Some(&doc)).await.catch_return()?.is_truthy()
		} else {
			false
		};

		// Check if any field stats were removed (count became 0) or if any remaining count field is
		// 0
		if !should_delete {
			should_delete = any_field_stats_removed || record.has_zero_count();
		}

		if should_delete {
			// Delete the record
			let key = crate::key::record::new(ns, db, &rid.table, &rid.key);
			txn.del(&key).await?;
		} else {
			// Store the updated record
			let key = crate::key::record::new(ns, db, &rid.table, &rid.key);
			txn.set(&key, &record, None).await?;
		}

		// Clear cache to ensure subsequent operations see the updated record
		txn.clear_cache();

		Ok(())
	}

	async fn fields(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		fdc: &FieldDataContext<'_>,
	) -> Result<(Vec<Assignment>, Option<Expr>, HashMap<String, FieldStatsDelta>)> {
		let mut set_ops = Vec::new();
		let mut del_ops = None;
		let mut metadata_deltas = HashMap::new();
		//
		for field in fdc.view.fields.iter_non_all_fields() {
			// Process the field
			if let Field::Single {
				expr,
				alias,
			} = field
			{
				// Get the name of the field
				let idiom = alias.clone().unwrap_or_else(|| expr.to_idiom());
				// Ignore any id field
				if idiom.is_id() {
					continue;
				}

				if let Expr::FunctionCall(f) = expr {
					if let Function::Normal(name) = &f.receiver {
						match name.as_str() {
							"count" => {
								let val = expr
									.compute(stk, ctx, opt, Some(fdc.doc))
									.await
									.catch_return()?;
								self.chg(&mut set_ops, &mut metadata_deltas, &fdc.act, idiom, val)?;
								continue;
							}
							"time::min" => {
								let val = stk
									.run(|stk| f.arguments[0].compute(stk, ctx, opt, Some(fdc.doc)))
									.await
									.catch_return()?;
								let val = match val {
									val @ Value::Datetime(_) => val,
									val => {
										bail!(Error::InvalidAggregation {
											name: name.to_string(),
											table: fdc.ft.name.clone(),
											message: format!(
												"This function expects a datetime but found {val}"
											),
										})
									}
								};
								self.min(
									&mut set_ops,
									&mut del_ops,
									&mut metadata_deltas,
									fdc,
									field,
									idiom,
									val,
								)?;
								continue;
							}
							"time::max" => {
								let val = stk
									.run(|stk| f.arguments[0].compute(stk, ctx, opt, Some(fdc.doc)))
									.await
									.catch_return()?;
								let val = match val {
									val @ Value::Datetime(_) => val,
									val => {
										bail!(Error::InvalidAggregation {
											name: name.to_string(),
											table: fdc.ft.name.clone(),
											message: format!(
												"This function expects a datetime but found {val}"
											),
										})
									}
								};
								self.max(
									&mut set_ops,
									&mut del_ops,
									&mut metadata_deltas,
									fdc,
									field,
									idiom,
									val,
								)?;
								continue;
							}
							"math::sum" => {
								let val = stk
									.run(|stk| f.arguments[0].compute(stk, ctx, opt, Some(fdc.doc)))
									.await
									.catch_return()?;
								let val = match val {
									val @ Value::Number(_) => val,
									val => {
										bail!(Error::InvalidAggregation {
											name: name.to_string(),
											table: fdc.ft.name.clone(),
											message: format!(
												"This function expects a number but found {val}"
											),
										})
									}
								};
								self.chg(&mut set_ops, &mut metadata_deltas, &fdc.act, idiom, val)?;
								continue;
							}

							"math::min" => {
								let val = stk
									.run(|stk| f.arguments[0].compute(stk, ctx, opt, Some(fdc.doc)))
									.await
									.catch_return()?;
								let val = match val {
									val @ Value::Number(_) => val,
									val => {
										bail!(Error::InvalidAggregation {
											name: name.to_string(),
											table: fdc.ft.name.clone(),
											message: format!(
												"This function expects a number but found {val}"
											),
										})
									}
								};
								self.min(
									&mut set_ops,
									&mut del_ops,
									&mut metadata_deltas,
									fdc,
									field,
									idiom,
									val,
								)?;
								continue;
							}
							"math::max" => {
								let val = stk
									.run(|stk| f.arguments[0].compute(stk, ctx, opt, Some(fdc.doc)))
									.await
									.catch_return()?;
								let val = match val {
									val @ Value::Number(_) => val,
									val => {
										bail!(Error::InvalidAggregation {
											name: name.to_string(),
											table: fdc.ft.name.clone(),
											message: format!(
												"This function expects a number but found {val}"
											),
										})
									}
								};
								self.max(
									&mut set_ops,
									&mut del_ops,
									&mut metadata_deltas,
									fdc,
									field,
									idiom,
									val,
								)?;
								continue;
							}
							"math::mean" => {
								let val = stk
									.run(|stk| f.arguments[0].compute(stk, ctx, opt, Some(fdc.doc)))
									.await
									.catch_return()?;
								let val = match val {
									val @ Value::Number(_) => val.coerce_to::<Decimal>()?.into(),
									val => {
										bail!(Error::InvalidAggregation {
											name: name.to_string(),
											table: fdc.ft.name.clone(),
											message: format!(
												"This function expects a number but found {val}"
											),
										})
									}
								};
								self.mean(
									&mut del_ops,
									&mut metadata_deltas,
									&fdc.act,
									idiom,
									val,
								)?;
								continue;
							}
							_ => {}
						}
					}
				}

				let val = stk
					.run(|stk| expr.compute(stk, ctx, opt, Some(fdc.doc)))
					.await
					.catch_return()?;
				self.set(&mut set_ops, idiom, val)?;
			}
		}
		Ok((set_ops, del_ops, metadata_deltas))
	}

	/// Set the field in the foreign table
	fn set(&self, ops: &mut Vec<Assignment>, key: Idiom, val: Value) -> Result<()> {
		ops.push(Assignment {
			place: key,
			operator: AssignOperator::Assign,
			value: val.into_literal(),
		});
		// Everything ok
		Ok(())
	}
	/// Increment or decrement the field in the foreign table
	fn chg(
		&self,
		set_ops: &mut Vec<Assignment>,
		metadata_deltas: &mut HashMap<String, FieldStatsDelta>,
		act: &FieldAction,
		key: Idiom,
		val: Value,
	) -> Result<()> {
		let field_name = key.to_string();
		let count_val = if let Value::Number(n) = &val {
			n.as_int() as u64
		} else {
			1 // For non-numeric count operations, default to 1
		};

		match act {
			FieldAction::Add | FieldAction::UpdateAdd => {
				set_ops.push(Assignment {
					place: key.clone(),
					operator: AssignOperator::Add,
					value: val.into_literal(),
				});

				// Add positive delta to metadata, combining with any existing delta
				let new_delta = if field_name.contains("count") || field_name == "count" {
					FieldStatsDelta::CountAdd(count_val)
				} else {
					FieldStatsDelta::SumAdd
				};

				match metadata_deltas.entry(field_name.clone()) {
					Entry::Occupied(mut occupied_entry) => {
						let existing = occupied_entry.insert(FieldStatsDelta::CountAdd(count_val));
						occupied_entry.insert(combine_field_deltas(existing, new_delta));
					}
					Entry::Vacant(vacant_entry) => {
						vacant_entry.insert(new_delta);
					}
				}
			}
			FieldAction::Sub | FieldAction::UpdateSub => {
				set_ops.push(Assignment {
					place: key.clone(),
					operator: AssignOperator::Subtract,
					value: val.into_literal(),
				});

				// Add negative delta to metadata, combining with any existing delta
				let new_delta = if field_name.contains("count") || field_name == "count" {
					FieldStatsDelta::CountSub(count_val)
				} else {
					FieldStatsDelta::SumSub
				};

				match metadata_deltas.entry(field_name.clone()) {
					Entry::Occupied(mut occupied_entry) => {
						let existing = occupied_entry.insert(FieldStatsDelta::CountSub(count_val));
						occupied_entry.insert(combine_field_deltas(existing, new_delta));
					}
					Entry::Vacant(vacant_entry) => {
						vacant_entry.insert(new_delta);
					}
				}

				// Add a purge condition based on metadata count becoming 0
				// This will be handled in handle_record_with_metadata based on final count state
			}
		}
		// Everything ok
		Ok(())
	}

	/// Set the new minimum value for the field in the foreign table
	#[allow(clippy::too_many_arguments)]
	fn min(
		&self,
		set_ops: &mut Vec<Assignment>,
		del_cond: &mut Option<Expr>,
		metadata_deltas: &mut HashMap<String, FieldStatsDelta>,
		fdc: &FieldDataContext,
		field: &Field,
		key: Idiom,
		val: Value,
	) -> Result<()> {
		let field_name = key.to_string();

		match fdc.act {
			FieldAction::Add => {
				let val_lit = val.into_literal();
				set_ops.push(Assignment {
					place: key.clone(),
					operator: AssignOperator::Assign,
					value: Expr::IfElse(Box::new(IfelseStatement {
						exprs: vec![(
							Expr::Binary {
								left: Box::new(Expr::Binary {
									left: Box::new(Expr::Idiom(key.clone())),
									op: BinaryOperator::ExactEqual,
									right: Box::new(Expr::Literal(Literal::None)),
								}),
								op: BinaryOperator::Or,
								right: Box::new(Expr::Binary {
									left: Box::new(Expr::Idiom(key.clone())),
									op: BinaryOperator::MoreThan,
									right: Box::new(val_lit.clone()),
								}),
							},
							val_lit,
						)],
						close: Some(Expr::Idiom(key)),
					})),
				});

				// Update metadata for min/max tracking, combining with any existing delta
				let new_delta = FieldStatsDelta::MinMaxAdd;
				match metadata_deltas.entry(field_name.clone()) {
					Entry::Occupied(mut occupied_entry) => {
						let existing = occupied_entry.insert(FieldStatsDelta::MinMaxAdd);
						occupied_entry.insert(combine_field_deltas(existing, new_delta));
					}
					Entry::Vacant(vacant_entry) => {
						vacant_entry.insert(new_delta);
					}
				}
			}
			FieldAction::Sub => {
				// If it is equal to the previous MIN value,
				// as we can't know what was the previous MIN value,
				// we have to recompute it
				let subquery = Self::one_group_query(fdc, field, &key, val)?;
				set_ops.push(Assignment {
					place: key.clone(),
					operator: AssignOperator::Assign,
					value: subquery,
				});

				// Update metadata for min/max tracking, combining with any existing delta
				let new_delta = FieldStatsDelta::MinMaxSub;
				match metadata_deltas.entry(field_name.clone()) {
					Entry::Occupied(mut occupied_entry) => {
						let existing = occupied_entry.insert(FieldStatsDelta::MinMaxSub);
						occupied_entry.insert(combine_field_deltas(existing, new_delta));
					}
					Entry::Vacant(vacant_entry) => {
						vacant_entry.insert(new_delta);
					}
				}

				// Add a purge condition (delete record if the number of values is 0)
				// Note: The actual purge decision will be made based on the final count after
				// applying delta
				accumulate_delete_expr(
					del_cond,
					Expr::Binary {
						left: Box::new(Expr::Idiom(key.clone())),
						op: BinaryOperator::ExactEqual,
						right: Box::new(Expr::Literal(Literal::None)),
					},
				);
			}
			FieldAction::UpdateSub => {
				// For UPDATE operations, we skip the Sub processing
				// The recompute will be done in UpdateAdd
			}
			FieldAction::UpdateAdd => {
				// For UPDATE operations within the same group, always recompute min
				// We need to unconditionally recompute because the value has changed
				let recompute_expr = Self::group_recompute_query(fdc, field)?;
				set_ops.push(Assignment {
					place: key.clone(),
					operator: AssignOperator::Assign,
					value: recompute_expr,
				});

				// Don't update metadata for UPDATE operations - count stays the same
			}
		}
		// Everything ok
		Ok(())
	}
	/// Set the new maximum value for the field in the foreign table
	#[allow(clippy::too_many_arguments)]
	fn max(
		&self,
		set_ops: &mut Vec<Assignment>,
		del_cond: &mut Option<Expr>,
		metadata_deltas: &mut HashMap<String, FieldStatsDelta>,
		fdc: &FieldDataContext,
		field: &Field,
		key: Idiom,
		val: Value,
	) -> Result<()> {
		let field_name = key.to_string();

		match fdc.act {
			FieldAction::Add => {
				let val_lit = val.into_literal();
				set_ops.push(Assignment {
					place: key.clone(),
					operator: AssignOperator::Assign,
					value: Expr::IfElse(Box::new(IfelseStatement {
						exprs: vec![(
							Expr::Binary {
								left: Box::new(Expr::Binary {
									left: Box::new(Expr::Idiom(key.clone())),
									op: BinaryOperator::ExactEqual,
									right: Box::new(Expr::Literal(Literal::None)),
								}),
								op: BinaryOperator::Or,
								right: Box::new(Expr::Binary {
									left: Box::new(Expr::Idiom(key.clone())),
									op: BinaryOperator::LessThan,
									right: Box::new(val_lit.clone()),
								}),
							},
							val_lit,
						)],
						close: Some(Expr::Idiom(key)),
					})),
				});

				// Update metadata for min/max tracking, combining with any existing delta
				let new_delta = FieldStatsDelta::MinMaxAdd;
				match metadata_deltas.entry(field_name.clone()) {
					Entry::Occupied(mut occupied_entry) => {
						let existing = occupied_entry.insert(FieldStatsDelta::MinMaxAdd);
						occupied_entry.insert(combine_field_deltas(existing, new_delta));
					}
					Entry::Vacant(vacant_entry) => {
						vacant_entry.insert(new_delta);
					}
				}
			}
			FieldAction::Sub => {
				// If it is equal to the previous MAX value,
				// as we can't know what was the previous MAX value,
				// we have to recompute the MAX
				let subquery = Self::one_group_query(fdc, field, &key, val)?;
				set_ops.push(Assignment {
					place: key.clone(),
					operator: AssignOperator::Assign,
					value: subquery,
				});

				// Update metadata for min/max tracking, combining with any existing delta
				let new_delta = FieldStatsDelta::MinMaxSub;
				match metadata_deltas.entry(field_name.clone()) {
					Entry::Occupied(mut occupied_entry) => {
						let existing = occupied_entry.insert(FieldStatsDelta::MinMaxSub);
						occupied_entry.insert(combine_field_deltas(existing, new_delta));
					}
					Entry::Vacant(vacant_entry) => {
						vacant_entry.insert(new_delta);
					}
				}

				// Add a purge condition (delete record if the number of values is 0)
				// Note: The actual purge decision will be made based on the final count after
				// applying delta
				accumulate_delete_expr(
					del_cond,
					Expr::Binary {
						left: Box::new(Expr::Idiom(key.clone())),
						op: BinaryOperator::ExactEqual,
						right: Box::new(Expr::Literal(Literal::None)),
					},
				);
			}
			FieldAction::UpdateSub => {
				// For UPDATE operations, we skip the Sub processing
				// The recompute will be done in UpdateAdd
			}
			FieldAction::UpdateAdd => {
				// For UPDATE operations within the same group, always recompute max
				// We need to unconditionally recompute because the value has changed
				let recompute_expr = Self::group_recompute_query(fdc, field)?;
				set_ops.push(Assignment {
					place: key.clone(),
					operator: AssignOperator::Assign,
					value: recompute_expr,
				});

				// Don't update metadata for UPDATE operations - count stays the same
			}
		}
		// Everything ok
		Ok(())
	}

	/// Set the new average value for the field in the foreign table
	fn mean(
		&self,
		del_cond: &mut Option<Expr>,
		metadata_deltas: &mut HashMap<String, FieldStatsDelta>,
		act: &FieldAction,
		key: Idiom,
		val: Value,
	) -> Result<()> {
		let field_name = key.to_string();
		let decimal_val = match &val {
			Value::Number(n) => n.to_decimal(),
			_ => bail!(Error::InvalidAggregation {
				name: "mean".to_string(),
				table: "unknown".to_string(), // We don't have table context here
				message: format!("Mean expects a number but found {val}"),
			}),
		};

		// Store the delta operation for mean calculation, combining with any existing delta
		match act {
			FieldAction::Add | FieldAction::UpdateAdd => {
				let new_delta = FieldStatsDelta::MeanAdd {
					value: decimal_val,
				};
				match metadata_deltas.entry(field_name.clone()) {
					Entry::Occupied(mut occupied_entry) => {
						let existing = occupied_entry.insert(FieldStatsDelta::MeanAdd {
							value: decimal_val,
						});
						occupied_entry.insert(combine_field_deltas(existing, new_delta));
					}
					Entry::Vacant(vacant_entry) => {
						vacant_entry.insert(new_delta);
					}
				}
				// Field value will be calculated from metadata during record processing
			}
			FieldAction::Sub | FieldAction::UpdateSub => {
				let new_delta = FieldStatsDelta::MeanSub {
					value: decimal_val,
				};
				match metadata_deltas.entry(field_name.clone()) {
					Entry::Occupied(mut occupied_entry) => {
						// Temporarly replace the value to take ownership
						let existing = occupied_entry.insert(FieldStatsDelta::MeanSub {
							value: decimal_val,
						});
						occupied_entry.insert(combine_field_deltas(existing, new_delta));
					}
					Entry::Vacant(vacant_entry) => {
						vacant_entry.insert(new_delta);
					}
				}

				// For mean, we need to potentially delete the record if count becomes 0
				accumulate_delete_expr(
					del_cond,
					Expr::Binary {
						left: Box::new(Expr::Idiom(key.clone())),
						op: BinaryOperator::ExactEqual,
						right: Box::new(Expr::Literal(Literal::Decimal(Decimal::ZERO))),
					},
				);
			}
		}

		// Everything ok
		Ok(())
	}

	/// Unconditionally recomputes the value for one group (used for UPDATE operations)
	fn group_recompute_query(fdc: &FieldDataContext, field: &Field) -> Result<Expr> {
		// Build the condition merging the optional user provided condition and the group
		let mut iter = fdc.groups.0.iter().enumerate();
		let cond = if let Some((i, g)) = iter.next() {
			let mut root = Expr::Binary {
				left: Box::new(Expr::Idiom(g.0.clone())),
				op: BinaryOperator::Equal,
				right: Box::new(fdc.group_ids[i].clone().into_literal()),
			};
			for (i, g) in iter {
				let exp = Expr::Binary {
					left: Box::new(Expr::Idiom(g.0.clone())),
					op: BinaryOperator::Equal,
					right: Box::new(fdc.group_ids[i].clone().into_literal()),
				};
				root = Expr::Binary {
					left: Box::new(root),
					op: BinaryOperator::And,
					right: Box::new(exp),
				};
			}
			if let Some(c) = &fdc.view.cond {
				root = Expr::Binary {
					left: Box::new(root),
					op: BinaryOperator::And,
					right: Box::new(c.clone()),
				};
			}
			Some(Cond(root))
		} else {
			fdc.view.cond.clone().map(Cond)
		};

		let group_select = Expr::Select(Box::new(SelectStatement {
			expr: Fields::Select(vec![field.clone()]),
			cond,
			what: fdc
				.view
				.what
				.iter()
				.map(|x| Expr::Table(unsafe { Ident::new_unchecked(x.clone()) }))
				.collect(),
			group: Some(fdc.groups.clone()),
			..SelectStatement::default()
		}));
		let array_first = Expr::FunctionCall(Box::new(FunctionCall {
			receiver: Function::Normal("array::first".to_string()),
			arguments: vec![group_select],
		}));
		let ident = match field {
			Field::Single {
				alias: Some(alias),
				..
			} => match alias.0.first() {
				Some(Part::Field(ident)) => ident.clone(),
				p => fail!("Unexpected ident type encountered: {p:?}"),
			},
			f => fail!("Unexpected field type encountered: {f:?}"),
		};
		Ok(Expr::Idiom(Idiom(vec![Part::Start(array_first), Part::Field(ident)])))
	}

	/// Recomputes the value for one group (with conditional check)
	fn one_group_query(
		fdc: &FieldDataContext,
		field: &Field,
		key: &Idiom,
		val: Value,
	) -> Result<Expr> {
		// Build the condition merging the optional user provided condition and the
		// group
		let mut iter = fdc.groups.0.iter().enumerate();
		let cond = if let Some((i, g)) = iter.next() {
			let mut root = Expr::Binary {
				left: Box::new(Expr::Idiom(g.0.clone())),
				op: BinaryOperator::Equal,
				right: Box::new(fdc.group_ids[i].clone().into_literal()),
			};
			for (i, g) in iter {
				let exp = Expr::Binary {
					left: Box::new(Expr::Idiom(g.0.clone())),
					op: BinaryOperator::Equal,
					right: Box::new(fdc.group_ids[i].clone().into_literal()),
				};
				root = Expr::Binary {
					left: Box::new(root),
					op: BinaryOperator::And,
					right: Box::new(exp),
				};
			}
			if let Some(c) = &fdc.view.cond {
				root = Expr::Binary {
					left: Box::new(root),
					op: BinaryOperator::And,
					right: Box::new(c.clone()),
				};
			}
			Some(Cond(root))
		} else {
			fdc.view.cond.clone().map(Cond)
		};

		let group_select = Expr::Select(Box::new(SelectStatement {
			expr: Fields::Select(vec![field.clone()]),
			cond,
			what: fdc
				.view
				.what
				.iter()
				.map(|x| Expr::Table(unsafe { Ident::new_unchecked(x.clone()) }))
				.collect(),
			group: Some(fdc.groups.clone()),
			..SelectStatement::default()
		}));
		let array_first = Expr::FunctionCall(Box::new(FunctionCall {
			receiver: Function::Normal("array::first".to_string()),
			arguments: vec![group_select],
		}));
		let ident = match field {
			Field::Single {
				alias: Some(alias),
				..
			} => match alias.0.first() {
				Some(Part::Field(ident)) => ident.clone(),
				p => fail!("Unexpected ident type encountered: {p:?}"),
			},
			f => fail!("Unexpected field type encountered: {f:?}"),
		};
		let compute_query = Expr::Idiom(Idiom(vec![Part::Start(array_first), Part::Field(ident)]));
		Ok(Expr::IfElse(Box::new(IfelseStatement {
			exprs: vec![(
				Expr::Binary {
					left: Box::new(Expr::Idiom(key.clone())),
					op: BinaryOperator::Equal,
					right: Box::new(val.clone().into_literal()),
				},
				compute_query,
			)],
			close: Some(Expr::Idiom(key.clone())),
		})))
	}
}
