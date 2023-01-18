use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::data::Data;
use crate::sql::expression::Expression;
use crate::sql::field::{Field, Fields};
use crate::sql::idiom::Idiom;
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
		txn: &Transaction,
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
		// Loop through all foreign table statements
		for ft in self.ft(opt, txn).await?.iter() {
			// Get the table definition
			let tb = ft.view.as_ref().unwrap();
			// Check if there is a GROUP BY clause
			match &tb.group {
				// There is a GROUP BY clause specified
				Some(group) => {
					// Set the previous record id
					let old = Thing {
						tb: ft.name.to_raw(),
						id: try_join_all(
							group.iter().map(|v| v.compute(ctx, opt, txn, Some(&self.initial))),
						)
						.await?
						.into_iter()
						.collect::<Vec<_>>()
						.into(),
					};
					// Set the current record id
					let rid = Thing {
						tb: ft.name.to_raw(),
						id: try_join_all(
							group.iter().map(|v| v.compute(ctx, opt, txn, Some(&self.current))),
						)
						.await?
						.into_iter()
						.collect::<Vec<_>>()
						.into(),
					};
					// Check if a WHERE clause is specified
					match &tb.cond {
						// There is a WHERE clause specified
						Some(cond) => {
							match cond.compute(ctx, opt, txn, Some(&self.current)).await? {
								v if v.is_truthy() => {
									if !opt.force && act != Action::Create {
										// Delete the old value
										let act = Action::Delete;
										// Modify the value in the table
										let stm = UpdateStatement {
											what: Values(vec![Value::from(old)]),
											data: Some(
												self.data(ctx, opt, txn, act, &tb.expr).await?,
											),
											..UpdateStatement::default()
										};
										// Execute the statement
										stm.compute(ctx, opt, txn, None).await?;
									}
									if act != Action::Delete {
										// Update the new value
										let act = Action::Update;
										// Modify the value in the table
										let stm = UpdateStatement {
											what: Values(vec![Value::from(rid)]),
											data: Some(
												self.data(ctx, opt, txn, act, &tb.expr).await?,
											),
											..UpdateStatement::default()
										};
										// Execute the statement
										stm.compute(ctx, opt, txn, None).await?;
									}
								}
								_ => {
									if !opt.force && act != Action::Create {
										// Update the new value
										let act = Action::Update;
										// Modify the value in the table
										let stm = UpdateStatement {
											what: Values(vec![Value::from(old)]),
											data: Some(
												self.data(ctx, opt, txn, act, &tb.expr).await?,
											),
											..UpdateStatement::default()
										};
										// Execute the statement
										stm.compute(ctx, opt, txn, None).await?;
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
									data: Some(self.data(ctx, opt, txn, act, &tb.expr).await?),
									..UpdateStatement::default()
								};
								// Execute the statement
								stm.compute(ctx, opt, txn, None).await?;
							}
							if act != Action::Delete {
								// Update the new value
								let act = Action::Update;
								// Modify the value in the table
								let stm = UpdateStatement {
									what: Values(vec![Value::from(rid)]),
									data: Some(self.data(ctx, opt, txn, act, &tb.expr).await?),
									..UpdateStatement::default()
								};
								// Execute the statement
								stm.compute(ctx, opt, txn, None).await?;
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
					let doc = Some(self.current.as_ref());
					// Check if a WHERE clause is specified
					match &tb.cond {
						// There is a WHERE clause specified
						Some(cond) => {
							match cond.compute(ctx, opt, txn, doc).await? {
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
												tb.expr.compute(ctx, opt, txn, doc, false).await?,
											)),
											..UpdateStatement::default()
										}),
									};
									// Execute the statement
									stm.compute(ctx, opt, txn, None).await?;
								}
								_ => {
									// Delete the value in the table
									let stm = DeleteStatement {
										what: Values(vec![Value::from(rid)]),
										..DeleteStatement::default()
									};
									// Execute the statement
									stm.compute(ctx, opt, txn, None).await?;
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
										tb.expr.compute(ctx, opt, txn, doc, false).await?,
									)),
									..UpdateStatement::default()
								}),
							};
							// Execute the statement
							stm.compute(ctx, opt, txn, None).await?;
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
		txn: &Transaction,
		act: Action,
		exp: &Fields,
	) -> Result<Data, Error> {
		//
		let mut ops: Ops = vec![];
		//
		let doc = match act {
			Action::Delete => Some(self.initial.as_ref()),
			Action::Update => Some(self.current.as_ref()),
			_ => unreachable!(),
		};
		//
		for field in exp.other() {
			// Process it if it is a normal field
			if let Field::Alone(v) = field {
				match v {
					Value::Function(f) if f.is_rolling() => match f.name() {
						"count" => {
							let val = f.compute(ctx, opt, txn, doc).await?;
							self.chg(&mut ops, &act, v.to_idiom(), val);
						}
						"math::sum" => {
							let val = f.args()[0].compute(ctx, opt, txn, doc).await?;
							self.chg(&mut ops, &act, v.to_idiom(), val);
						}
						"math::min" => {
							let val = f.args()[0].compute(ctx, opt, txn, doc).await?;
							self.min(&mut ops, &act, v.to_idiom(), val);
						}
						"math::max" => {
							let val = f.args()[0].compute(ctx, opt, txn, doc).await?;
							self.max(&mut ops, &act, v.to_idiom(), val);
						}
						"math::mean" => {
							let val = f.args()[0].compute(ctx, opt, txn, doc).await?;
							self.mean(&mut ops, &act, v.to_idiom(), val);
						}
						_ => unreachable!(),
					},
					_ => {
						let val = v.compute(ctx, opt, txn, doc).await?;
						self.set(&mut ops, v.to_idiom(), val);
					}
				}
			}
			// Process it if it is a aliased field
			if let Field::Alias(v, i) = field {
				match v {
					Value::Function(f) if f.is_rolling() => match f.name() {
						"count" => {
							let val = f.compute(ctx, opt, txn, doc).await?;
							self.chg(&mut ops, &act, i.to_owned(), val);
						}
						"math::sum" => {
							let val = f.args()[0].compute(ctx, opt, txn, doc).await?;
							self.chg(&mut ops, &act, i.to_owned(), val);
						}
						"math::min" => {
							let val = f.args()[0].compute(ctx, opt, txn, doc).await?;
							self.min(&mut ops, &act, i.to_owned(), val);
						}
						"math::max" => {
							let val = f.args()[0].compute(ctx, opt, txn, doc).await?;
							self.max(&mut ops, &act, i.to_owned(), val);
						}
						"math::mean" => {
							let val = f.args()[0].compute(ctx, opt, txn, doc).await?;
							self.mean(&mut ops, &act, i.to_owned(), val);
						}
						_ => unreachable!(),
					},
					_ => {
						let val = v.compute(ctx, opt, txn, doc).await?;
						self.set(&mut ops, i.to_owned(), val);
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
								l: Value::Idiom(key),
								o: Operator::Mul,
								r: Value::Idiom(key_c.clone()),
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
						l: Value::Idiom(key_c.clone()),
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
