use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::{Force, Statement};
use crate::doc::Document;
use crate::err::Error;
use crate::sql::data::Data;
use crate::sql::expression::Expression;
use crate::sql::field::{Field, Fields};
use crate::sql::idiom::Idiom;
use crate::sql::number::Number;
use crate::sql::operator::Operator;
use crate::sql::part::Part;
use crate::sql::paths::ID;
use crate::sql::statement::Statement as Query;
use crate::sql::statements::delete::DeleteStatement;
use crate::sql::statements::ifelse::IfelseStatement;
use crate::sql::statements::update::UpdateStatement;
use crate::sql::subquery::Subquery;
use crate::sql::thing::Thing;
use crate::sql::value::{Value, Values};
use futures::future::try_join_all;
use reblessive::tree::Stk;

type Ops = Vec<(Idiom, Operator, Value)>;

#[derive(Clone, Debug, Eq, PartialEq)]
enum Action {
	Create,
	Update,
	Delete,
}

impl<'a> Document<'a> {
	pub async fn table(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check import
		if opt.import {
			return Ok(());
		}
		let txn = ctx.transaction()?;
		// Was this force targeted at a specific foreign table?
		let targeted_force = matches!(opt.force, Force::Table(_));
		// Collect foreign tables or skip
		let fts = match &opt.force {
			Force::Table(tb)
				if tb.first().is_some_and(|tb| {
					tb.view.as_ref().is_some_and(|v| {
						self.id.is_some_and(|id| v.what.iter().any(|p| p.0 == id.tb))
					})
				}) =>
			{
				tb.clone()
			}
			Force::All => self.ft(opt, txn).await?,
			_ if self.changed() => self.ft(opt, txn).await?,
			_ => return Ok(()),
		};
		// Don't run permissions
		let opt = &opt.new_with_perms(false);
		// Get the record id
		let rid = self.id.as_ref().unwrap();
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
			let tb = ft.view.as_ref().unwrap();
			// Check if there is a GROUP BY clause
			match &tb.group {
				// There is a GROUP BY clause specified
				Some(group) => {
					let id = stk
						.scope(|scope| {
							try_join_all(group.iter().map(|v| {
								scope.run(|stk| v.compute(stk, ctx, opt, Some(&self.initial)))
							}))
						})
						.await?
						.into_iter()
						.collect::<Vec<_>>()
						.into();
					// Set the previous record id
					let old = Thing {
						tb: ft.name.to_raw(),
						id,
					};

					let id = stk
						.scope(|scope| {
							try_join_all(group.iter().map(|v| {
								scope.run(|stk| v.compute(stk, ctx, opt, Some(&self.current)))
							}))
						})
						.await?
						.into_iter()
						.collect::<Vec<_>>()
						.into();
					// Set the current record id
					let rid = Thing {
						tb: ft.name.to_raw(),
						id,
					};
					// Check if a WHERE clause is specified
					match &tb.cond {
						// There is a WHERE clause specified
						Some(cond) => {
							match cond.compute(stk, ctx, opt, Some(&self.current)).await? {
								v if v.is_truthy() => {
									if !targeted_force && act != Action::Create {
										// Delete the old value
										let act = Action::Delete;
										// Modify the value in the table
										let stm = UpdateStatement {
											what: Values(vec![Value::from(old)]),
											data: Some(
												self.data(stk, ctx, opt, act, &tb.expr).await?,
											),
											..UpdateStatement::default()
										};
										// Execute the statement
										stm.compute(stk, ctx, opt, None).await?;
									}
									if act != Action::Delete {
										// Update the new value
										let act = Action::Update;
										// Modify the value in the table
										let stm = UpdateStatement {
											what: Values(vec![Value::from(rid)]),
											data: Some(
												self.data(stk, ctx, opt, act, &tb.expr).await?,
											),
											..UpdateStatement::default()
										};
										// Execute the statement
										stm.compute(stk, ctx, opt, None).await?;
									}
								}
								_ => {
									if !targeted_force && act != Action::Create {
										// Update the new value
										let act = Action::Update;
										// Modify the value in the table
										let stm = UpdateStatement {
											what: Values(vec![Value::from(old)]),
											data: Some(
												self.data(stk, ctx, opt, act, &tb.expr).await?,
											),
											..UpdateStatement::default()
										};
										// Execute the statement
										stm.compute(stk, ctx, opt, None).await?;
									}
								}
							}
						}
						// No WHERE clause is specified
						None => {
							if !targeted_force && act != Action::Create {
								// Delete the old value
								let act = Action::Delete;
								// Modify the value in the table
								let stm = UpdateStatement {
									what: Values(vec![Value::from(old)]),
									data: Some(self.data(stk, ctx, opt, act, &tb.expr).await?),
									..UpdateStatement::default()
								};
								// Execute the statement
								stm.compute(stk, ctx, opt, None).await?;
							}
							if act != Action::Delete {
								// Update the new value
								let act = Action::Update;
								// Modify the value in the table
								let stm = UpdateStatement {
									what: Values(vec![Value::from(rid)]),
									data: Some(self.data(stk, ctx, opt, act, &tb.expr).await?),
									..UpdateStatement::default()
								};
								// Execute the statement
								stm.compute(stk, ctx, opt, None).await?;
							}
						}
					}
				}
				// No GROUP BY clause is specified
				None => {
					// Set the current record id
					let rid = Thing {
						tb: ft.name.to_raw(),
						id: rid.id.clone(),
					};
					// Check if a WHERE clause is specified
					match &tb.cond {
						// There is a WHERE clause specified
						Some(cond) => {
							match cond.compute(stk, ctx, opt, Some(&self.current)).await? {
								v if v.is_truthy() => {
									// Define the statement
									let stm = match act {
										// Delete the value in the table
										Action::Delete => Query::Delete(DeleteStatement {
											what: Values(vec![Value::from(rid)]),
											..DeleteStatement::default()
										}),
										// Update the value in the table
										_ => Query::Update(UpdateStatement {
											what: Values(vec![Value::from(rid)]),
											data: Some(self.full(stk, ctx, opt, &tb.expr).await?),
											..UpdateStatement::default()
										}),
									};
									// Execute the statement
									stm.compute(stk, ctx, opt, None).await?;
								}
								_ => {
									// Delete the value in the table
									let stm = DeleteStatement {
										what: Values(vec![Value::from(rid)]),
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
							let stm = match act {
								// Delete the value in the table
								Action::Delete => Query::Delete(DeleteStatement {
									what: Values(vec![Value::from(rid)]),
									..DeleteStatement::default()
								}),
								// Update the value in the table
								_ => Query::Update(UpdateStatement {
									what: Values(vec![Value::from(rid)]),
									data: Some(self.full(stk, ctx, opt, &tb.expr).await?),
									..UpdateStatement::default()
								}),
							};
							// Execute the statement
							stm.compute(stk, ctx, opt, None).await?;
						}
					}
				}
			}
		}
		// Carry on
		Ok(())
	}
	//
	async fn full(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		exp: &Fields,
	) -> Result<Data, Error> {
		let mut data = exp.compute(stk, ctx, opt, Some(&self.current), false).await?;
		data.cut(ID.as_ref());
		Ok(Data::ReplaceExpression(data))
	}
	//
	async fn data(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		act: Action,
		exp: &Fields,
	) -> Result<Data, Error> {
		//
		let mut ops: Ops = vec![];
		// Create a new context with the initial or the current doc
		let doc = match act {
			Action::Delete => Some(&self.initial),
			Action::Update => Some(&self.current),
			_ => unreachable!(),
		};
		//
		for field in exp.other() {
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
				// Process the field projection
				match expr {
					Value::Function(f) if f.is_rolling() => match f.name() {
						Some("count") => {
							let val = f.compute(stk, ctx, opt, doc).await?;
							self.chg(&mut ops, &act, idiom, val);
						}
						Some("math::sum") => {
							let val = f.args()[0].compute(stk, ctx, opt, doc).await?;
							self.chg(&mut ops, &act, idiom, val);
						}
						Some("math::min") | Some("time::min") => {
							let val = f.args()[0].compute(stk, ctx, opt, doc).await?;
							self.min(&mut ops, &act, idiom, val);
						}
						Some("math::max") | Some("time::max") => {
							let val = f.args()[0].compute(stk, ctx, opt, doc).await?;
							self.max(&mut ops, &act, idiom, val);
						}
						Some("math::mean") => {
							let val = f.args()[0].compute(stk, ctx, opt, doc).await?;
							self.mean(&mut ops, &act, idiom, val);
						}
						_ => unreachable!(),
					},
					_ => {
						let val = expr.compute(stk, ctx, opt, doc).await?;
						self.set(&mut ops, idiom, val);
					}
				}
			}
		}
		//
		Ok(Data::SetExpression(ops))
	}
	/// Set the field in the foreign table
	fn set(&self, ops: &mut Ops, key: Idiom, val: Value) {
		ops.push((key, Operator::Equal, val));
	}
	/// Increment or decrement the field in the foreign table
	fn chg(&self, ops: &mut Ops, act: &Action, key: Idiom, val: Value) {
		ops.push((
			key,
			match act {
				Action::Delete => Operator::Dec,
				Action::Update => Operator::Inc,
				_ => unreachable!(),
			},
			val,
		));
	}
	/// Set the new minimum value for the field in the foreign table
	fn min(&self, ops: &mut Ops, act: &Action, key: Idiom, val: Value) {
		if act == &Action::Update {
			ops.push((
				key.clone(),
				Operator::Equal,
				Value::Subquery(Box::new(Subquery::Ifelse(IfelseStatement {
					exprs: vec![(
						Value::Expression(Box::new(Expression::Binary {
							l: Value::Idiom(key.clone()),
							o: Operator::MoreThan,
							r: val.clone(),
						})),
						val,
					)],
					close: Some(Value::Idiom(key)),
				}))),
			));
		}
	}
	/// Set the new maximum value for the field in the foreign table
	fn max(&self, ops: &mut Ops, act: &Action, key: Idiom, val: Value) {
		if act == &Action::Update {
			ops.push((
				key.clone(),
				Operator::Equal,
				Value::Subquery(Box::new(Subquery::Ifelse(IfelseStatement {
					exprs: vec![(
						Value::Expression(Box::new(Expression::Binary {
							l: Value::Idiom(key.clone()),
							o: Operator::LessThan,
							r: val.clone(),
						})),
						val,
					)],
					close: Some(Value::Idiom(key)),
				}))),
			));
		}
	}
	/// Set the new average value for the field in the foreign table
	fn mean(&self, ops: &mut Ops, act: &Action, key: Idiom, val: Value) {
		//
		let mut key_c = Idiom::from(vec![Part::from("__")]);
		key_c.0.push(Part::from(key.to_hash()));
		key_c.0.push(Part::from("c"));
		//
		ops.push((
			key.clone(),
			Operator::Equal,
			Value::Expression(Box::new(Expression::Binary {
				l: Value::Subquery(Box::new(Subquery::Value(Value::Expression(Box::new(
					Expression::Binary {
						l: Value::Subquery(Box::new(Subquery::Value(Value::Expression(Box::new(
							Expression::Binary {
								l: Value::Subquery(Box::new(Subquery::Value(Value::Expression(
									Box::new(Expression::Binary {
										l: Value::Idiom(key),
										o: Operator::Nco,
										r: Value::Number(Number::Int(0)),
									}),
								)))),
								o: Operator::Mul,
								r: Value::Subquery(Box::new(Subquery::Value(Value::Expression(
									Box::new(Expression::Binary {
										l: Value::Idiom(key_c.clone()),
										o: Operator::Nco,
										r: Value::Number(Number::Int(0)),
									}),
								)))),
							},
						))))),
						o: match act {
							Action::Delete => Operator::Sub,
							Action::Update => Operator::Add,
							_ => unreachable!(),
						},
						r: val,
					},
				))))),
				o: Operator::Div,
				r: Value::Subquery(Box::new(Subquery::Value(Value::Expression(Box::new(
					Expression::Binary {
						l: Value::Subquery(Box::new(Subquery::Value(Value::Expression(Box::new(
							Expression::Binary {
								l: Value::Idiom(key_c.clone()),
								o: Operator::Nco,
								r: Value::Number(Number::Int(0)),
							},
						))))),
						o: match act {
							Action::Delete => Operator::Sub,
							Action::Update => Operator::Add,
							_ => unreachable!(),
						},
						r: Value::from(1),
					},
				))))),
			})),
		));
		//
		ops.push((
			key_c.clone(),
			match act {
				Action::Delete => Operator::Dec,
				Action::Update => Operator::Inc,
				_ => unreachable!(),
			},
			Value::from(1),
		));
	}
}
