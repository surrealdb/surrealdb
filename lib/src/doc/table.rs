use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::data::Data;
use crate::sql::expression::Expression;
use crate::sql::field::{Field, Fields};
use crate::sql::idiom::Idiom;
use crate::sql::number::Number;
use crate::sql::operator::Operator;
use crate::sql::part::Part;
use crate::sql::statement::Statement as Query;
use crate::sql::statements::delete::DeleteStatement;
use crate::sql::statements::ifelse::IfelseStatement;
use crate::sql::statements::update::UpdateStatement;
use crate::sql::subquery::Subquery;
use crate::sql::thing::Thing;
use crate::sql::value::{Value, Values};
use futures::future::try_join_all;

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
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check events
		if !opt.tables {
			return Ok(());
		}
		// Check if forced
		if !opt.force && !self.changed() {
			return Ok(());
		}
		// Don't run permissions
		let opt = &opt.perms(false);
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
		// Clone transaction
		let txn = ctx.clone_transaction()?;
		// Loop through all foreign table statements
		for ft in self.ft(opt, &txn).await?.iter() {
			// Get the table definition
			let tb = ft.view.as_ref().unwrap();
			// Check if there is a GROUP BY clause
			match &tb.group {
				// There is a GROUP BY clause specified
				Some(group) => {
					let mut initial_ctx = Context::new(ctx);
					initial_ctx.add_cursor_doc(&self.initial);
					// Set the previous record id
					let old = Thing {
						tb: ft.name.to_raw(),
						id: try_join_all(group.iter().map(|v| v.compute(&initial_ctx, opt)))
							.await?
							.into_iter()
							.collect::<Vec<_>>()
							.into(),
					};
					let mut current_ctx = Context::new(ctx);
					current_ctx.add_cursor_doc(&self.current);
					// Set the current record id
					let rid = Thing {
						tb: ft.name.to_raw(),
						id: try_join_all(group.iter().map(|v| v.compute(&current_ctx, opt)))
							.await?
							.into_iter()
							.collect::<Vec<_>>()
							.into(),
					};
					// Check if a WHERE clause is specified
					match &tb.cond {
						// There is a WHERE clause specified
						Some(cond) => {
							match cond.compute(&current_ctx, opt).await? {
								v if v.is_truthy() => {
									if !opt.force && act != Action::Create {
										// Delete the old value
										let act = Action::Delete;
										// Modify the value in the table
										let stm = UpdateStatement {
											what: Values(vec![Value::from(old)]),
											data: Some(self.data(ctx, opt, act, &tb.expr).await?),
											..UpdateStatement::default()
										};
										// Execute the statement
										stm.compute(ctx, opt).await?;
									}
									if act != Action::Delete {
										// Update the new value
										let act = Action::Update;
										// Modify the value in the table
										let stm = UpdateStatement {
											what: Values(vec![Value::from(rid)]),
											data: Some(self.data(ctx, opt, act, &tb.expr).await?),
											..UpdateStatement::default()
										};
										// Execute the statement
										stm.compute(ctx, opt).await?;
									}
								}
								_ => {
									if !opt.force && act != Action::Create {
										// Update the new value
										let act = Action::Update;
										// Modify the value in the table
										let stm = UpdateStatement {
											what: Values(vec![Value::from(old)]),
											data: Some(self.data(ctx, opt, act, &tb.expr).await?),
											..UpdateStatement::default()
										};
										// Execute the statement
										stm.compute(ctx, opt).await?;
									}
								}
							}
						}
						// No WHERE clause is specified
						None => {
							if !opt.force && act != Action::Create {
								// Delete the old value
								let act = Action::Delete;
								// Modify the value in the table
								let stm = UpdateStatement {
									what: Values(vec![Value::from(old)]),
									data: Some(self.data(ctx, opt, act, &tb.expr).await?),
									..UpdateStatement::default()
								};
								// Execute the statement
								stm.compute(ctx, opt).await?;
							}
							if act != Action::Delete {
								// Update the new value
								let act = Action::Update;
								// Modify the value in the table
								let stm = UpdateStatement {
									what: Values(vec![Value::from(rid)]),
									data: Some(self.data(ctx, opt, act, &tb.expr).await?),
									..UpdateStatement::default()
								};
								// Execute the statement
								stm.compute(ctx, opt).await?;
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
					// Use the current record data
					let mut ctx = Context::new(ctx);
					ctx.add_cursor_doc(&self.current);
					// Check if a WHERE clause is specified
					match &tb.cond {
						// There is a WHERE clause specified
						Some(cond) => {
							match cond.compute(&ctx, opt).await? {
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
											data: Some(Data::ReplaceExpression(
												tb.expr.compute(&ctx, opt, false).await?,
											)),
											..UpdateStatement::default()
										}),
									};
									// Execute the statement
									stm.compute(&ctx, opt).await?;
								}
								_ => {
									// Delete the value in the table
									let stm = DeleteStatement {
										what: Values(vec![Value::from(rid)]),
										..DeleteStatement::default()
									};
									// Execute the statement
									stm.compute(&ctx, opt).await?;
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
									data: Some(Data::ReplaceExpression(
										tb.expr.compute(&ctx, opt, false).await?,
									)),
									..UpdateStatement::default()
								}),
							};
							// Execute the statement
							stm.compute(&ctx, opt).await?;
						}
					}
				}
			}
		}
		// Carry on
		Ok(())
	}
	//
	async fn data(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		act: Action,
		exp: &Fields,
	) -> Result<Data, Error> {
		//
		let mut ops: Ops = vec![];
		// Create a new context with the initial or the current doc
		let mut ctx = Context::new(ctx);
		match act {
			Action::Delete => ctx.add_cursor_doc(self.initial.as_ref()),
			Action::Update => ctx.add_cursor_doc(self.current.as_ref()),
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
				let idiom = alias.clone().unwrap_or_else(|| expr.to_idiom());
				match expr {
					Value::Function(f) if f.is_rolling() => match f.name() {
						"count" => {
							let val = f.compute(&ctx, opt).await?;
							self.chg(&mut ops, &act, idiom, val);
						}
						"math::sum" => {
							let val = f.args()[0].compute(&ctx, opt).await?;
							self.chg(&mut ops, &act, idiom, val);
						}
						"math::min" => {
							let val = f.args()[0].compute(&ctx, opt).await?;
							self.min(&mut ops, &act, idiom, val);
						}
						"math::max" => {
							let val = f.args()[0].compute(&ctx, opt).await?;
							self.max(&mut ops, &act, idiom, val);
						}
						"math::mean" => {
							let val = f.args()[0].compute(&ctx, opt).await?;
							self.mean(&mut ops, &act, idiom, val);
						}
						_ => unreachable!(),
					},
					_ => {
						let val = expr.compute(&ctx, opt).await?;
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
						Value::Expression(Box::new(Expression {
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
						Value::Expression(Box::new(Expression {
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
			Value::Expression(Box::new(Expression {
				l: Value::Subquery(Box::new(Subquery::Value(Value::Expression(Box::new(
					Expression {
						l: Value::Subquery(Box::new(Subquery::Value(Value::Expression(Box::new(
							Expression {
								l: Value::Subquery(Box::new(Subquery::Value(Value::Expression(
									Box::new(Expression {
										l: Value::Idiom(key),
										o: Operator::Nco,
										r: Value::Number(Number::Int(0)),
									}),
								)))),
								o: Operator::Mul,
								r: Value::Subquery(Box::new(Subquery::Value(Value::Expression(
									Box::new(Expression {
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
					Expression {
						l: Value::Subquery(Box::new(Subquery::Value(Value::Expression(Box::new(
							Expression {
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
