use anyhow::{Result, bail};
use futures::future::try_join_all;
use reblessive::tree::Stk;
use rust_decimal::Decimal;

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
use crate::val::{Array, RecordId, RecordIdKey, Value};

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
							// What do we do with the initial value on UPDATE and DELETE?
							if !targeted_force
								&& act != Action::Create && stk
								.run(|stk| cond.compute(stk, ctx, opt, Some(&self.initial)))
								.await
								.catch_return()?
								.is_truthy()
							{
								// Delete the old value in the table
								let fdc = FieldDataContext {
									ft,
									act: FieldAction::Sub,
									view: tb,
									groups: group,
									group_ids: Self::get_group_ids(
										stk,
										ctx,
										opt,
										group,
										&self.initial,
									)
									.await?,
									doc: &self.initial,
								};
								self.data(stk, ctx, opt, fdc).await?;
							}
							// What do we do with the current value on CREATE and UPDATE?
							if act != Action::Delete
								&& stk
									.run(|stk| cond.compute(stk, ctx, opt, Some(&self.current)))
									.await
									.catch_return()?
									.is_truthy()
							{
								// Update the new value in the table
								let fdc = FieldDataContext {
									ft,
									act: FieldAction::Add,
									view: tb,
									groups: group,
									group_ids: Self::get_group_ids(
										stk,
										ctx,
										opt,
										group,
										&self.current,
									)
									.await?,
									doc: &self.current,
								};
								self.data(stk, ctx, opt, fdc).await?;
							}
						}
						// No WHERE clause is specified
						None => {
							if !targeted_force && (act == Action::Delete || act == Action::Update) {
								// Delete the old value in the table
								let fdc = FieldDataContext {
									ft,
									act: FieldAction::Sub,
									view: tb,
									groups: group,
									group_ids: Self::get_group_ids(
										stk,
										ctx,
										opt,
										group,
										&self.initial,
									)
									.await?,
									doc: &self.initial,
								};
								self.data(stk, ctx, opt, fdc).await?;
							}
							if act == Action::Create || act == Action::Update {
								// Update the new value in the table
								let fdc = FieldDataContext {
									ft,
									act: FieldAction::Add,
									view: tb,
									groups: group,
									group_ids: Self::get_group_ids(
										stk,
										ctx,
										opt,
										group,
										&self.current,
									)
									.await?,
									doc: &self.current,
								};
								self.data(stk, ctx, opt, fdc).await?;
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
	//
	async fn data(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		fdc: FieldDataContext<'_>,
	) -> Result<()> {
		//
		let (set_ops, del_ops) = self.fields(stk, ctx, opt, &fdc).await?;
		//
		let thg = RecordId {
			table: fdc.ft.name.clone(),
			key: RecordIdKey::Array(Array(fdc.group_ids)),
		};
		let what = vec![Expr::Literal(Literal::RecordId(thg.clone().into_literal()))];
		let stm = UpsertStatement {
			what,
			data: Some(Data::SetExpression(set_ops)),
			..UpsertStatement::default()
		};
		stm.compute(stk, ctx, opt, None).await?;

		if let Some(del_cond) = del_ops {
			let what = vec![Expr::Literal(Literal::RecordId(thg.into_literal()))];
			let stm = DeleteStatement {
				what,
				cond: Some(Cond(del_cond)),
				..DeleteStatement::default()
			};
			stm.compute(stk, ctx, opt, None).await?;
		}
		Ok(())
	}

	async fn fields(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		fdc: &FieldDataContext<'_>,
	) -> Result<(Vec<Assignment>, Option<Expr>)> {
		let mut set_ops = Vec::new();
		let mut del_ops = None;
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
								self.chg(&mut set_ops, &mut del_ops, &fdc.act, idiom, val)?;
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
								self.min(&mut set_ops, &mut del_ops, fdc, field, idiom, val)?;
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
								self.max(&mut set_ops, &mut del_ops, fdc, field, idiom, val)?;
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
								self.chg(&mut set_ops, &mut del_ops, &fdc.act, idiom, val)?;
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
								self.min(&mut set_ops, &mut del_ops, fdc, field, idiom, val)?;
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
								self.max(&mut set_ops, &mut del_ops, fdc, field, idiom, val)?;
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
								self.mean(&mut set_ops, &mut del_ops, &fdc.act, idiom, val)?;
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
		Ok((set_ops, del_ops))
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
		del_cond: &mut Option<Expr>,
		act: &FieldAction,
		key: Idiom,
		val: Value,
	) -> Result<()> {
		match act {
			FieldAction::Add => {
				set_ops.push(Assignment {
					place: key.clone(),
					operator: AssignOperator::Add,
					value: val.into_literal(),
				});
			}
			FieldAction::Sub => {
				set_ops.push(Assignment {
					place: key.clone(),
					operator: AssignOperator::Subtract,
					value: val.into_literal(),
				});

				// Add a purge condition (delete record if the number of values is 0)
				accumulate_delete_expr(
					del_cond,
					Expr::Binary {
						left: Box::new(Expr::Idiom(key)),
						op: BinaryOperator::Equal,
						right: Box::new(Expr::Literal(Literal::Integer(0))),
					},
				);
			}
		}
		// Everything ok
		Ok(())
	}

	/// Set the new minimum value for the field in the foreign table
	fn min(
		&self,
		set_ops: &mut Vec<Assignment>,
		del_cond: &mut Option<Expr>,
		fdc: &FieldDataContext,
		field: &Field,
		key: Idiom,
		val: Value,
	) -> Result<()> {
		// Key for the value count
		let mut key_c = Idiom(vec![Part::field("__".to_owned()).unwrap()]);
		key_c.0.push(Part::field(key.to_hash()).unwrap());
		key_c.0.push(Part::field("c".to_owned()).unwrap());

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
				set_ops.push(Assignment {
					place: key_c,
					operator: AssignOperator::Add,
					value: Expr::Literal(Literal::Integer(1)),
				});
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
				//  Decrement the number of values
				set_ops.push(Assignment {
					place: key_c.clone(),
					operator: AssignOperator::Subtract,
					value: Expr::Literal(Literal::Integer(1)),
				});
				// Add a purge condition (delete record if the number of values is 0)
				accumulate_delete_expr(
					del_cond,
					Expr::Binary {
						left: Box::new(Expr::Idiom(key_c)),
						op: BinaryOperator::Equal,
						right: Box::new(Expr::Literal(Literal::Integer(0))),
					},
				)
			}
		}
		// Everything ok
		Ok(())
	}
	/// Set the new maximum value for the field in the foreign table
	fn max(
		&self,
		set_ops: &mut Vec<Assignment>,
		del_cond: &mut Option<Expr>,
		fdc: &FieldDataContext,
		field: &Field,
		key: Idiom,
		val: Value,
	) -> Result<()> {
		// Key for the value count
		let mut key_c = Idiom(vec![Part::field("__".to_owned()).unwrap()]);
		key_c.0.push(Part::field(key.to_hash()).unwrap());
		key_c.0.push(Part::field("c".to_owned()).unwrap());
		//
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
				set_ops.push(Assignment {
					place: key_c,
					operator: AssignOperator::Add,
					value: Expr::Literal(Literal::Integer(1)),
				})
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
				//  Decrement the number of values
				set_ops.push(Assignment {
					place: key_c.clone(),
					operator: AssignOperator::Subtract,
					value: Expr::Literal(Literal::Integer(1)),
				});
				// Add a purge condition (delete record if the number of values is 0)
				accumulate_delete_expr(
					del_cond,
					Expr::Binary {
						left: Box::new(Expr::Idiom(key_c)),
						op: BinaryOperator::Equal,
						right: Box::new(Expr::Literal(Literal::Integer(0))),
					},
				)
			}
		}
		// Everything ok
		Ok(())
	}

	/// Set the new average value for the field in the foreign table
	fn mean(
		&self,
		set_ops: &mut Vec<Assignment>,
		del_cond: &mut Option<Expr>,
		act: &FieldAction,
		key: Idiom,
		val: Value,
	) -> Result<()> {
		// Key for the value count

		let key_c = Idiom(vec![
			Part::field("__".to_owned()).unwrap(),
			Part::field(key.to_hash()).unwrap(),
			Part::field("c".to_owned()).unwrap(),
		]);
		//
		set_ops.push(Assignment {
			place: key.clone(),
			operator: AssignOperator::Assign,
			value: Expr::Binary {
				left: Box::new(Expr::Binary {
					left: Box::new(Expr::Binary {
						left: Box::new(Expr::Binary {
							left: Box::new(Expr::Idiom(key)),
							op: BinaryOperator::NullCoalescing,
							right: Box::new(Expr::Literal(Literal::Integer(0))),
						}),
						op: BinaryOperator::Multiply,
						right: Box::new(Expr::Binary {
							left: Box::new(Expr::Idiom(key_c.clone())),
							op: BinaryOperator::NullCoalescing,
							right: Box::new(Expr::Literal(Literal::Integer(0))),
						}),
					}),
					op: match act {
						FieldAction::Sub => BinaryOperator::Subtract,
						FieldAction::Add => BinaryOperator::Add,
					},
					right: Box::new(val.into_literal()),
				}),
				op: BinaryOperator::Divide,
				right: Box::new(Expr::Binary {
					left: Box::new(Expr::Binary {
						left: Box::new(Expr::Idiom(key_c.clone())),
						op: BinaryOperator::NullCoalescing,
						right: Box::new(Expr::Literal(Literal::Integer(0))),
					}),
					op: match act {
						FieldAction::Sub => BinaryOperator::Subtract,
						FieldAction::Add => BinaryOperator::Add,
					},
					right: Box::new(Expr::Literal(Literal::Integer(1))),
				}),
			},
		});
		match act {
			//  Increment the number of values
			FieldAction::Add => set_ops.push(Assignment {
				place: key_c,
				operator: AssignOperator::Add,
				value: Expr::Literal(Literal::Integer(1)),
			}),
			FieldAction::Sub => {
				//  Decrement the number of values
				set_ops.push(Assignment {
					place: key_c.clone(),
					operator: AssignOperator::Subtract,
					value: Expr::Literal(Literal::Integer(1)),
				});
				// Add a purge condition (delete record if the number of values is 0)
				accumulate_delete_expr(
					del_cond,
					Expr::Binary {
						left: Box::new(Expr::Idiom(key_c)),
						op: BinaryOperator::Equal,
						right: Box::new(Expr::Literal(Literal::Integer(0))),
					},
				)
			}
		}
		// Everything ok
		Ok(())
	}

	/// Recomputes the value for one group
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
