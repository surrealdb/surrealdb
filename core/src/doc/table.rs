use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::{Force, Statement};
use crate::doc::{CursorDoc, Document};
use crate::err::Error;
use crate::sql::data::Data;
use crate::sql::expression::Expression;
use crate::sql::field::{Field, Fields};
use crate::sql::idiom::Idiom;
use crate::sql::number::Number;
use crate::sql::operator::Operator;
use crate::sql::part::Part;
use crate::sql::paths::ID;
use crate::sql::statements::delete::DeleteStatement;
use crate::sql::statements::ifelse::IfelseStatement;
use crate::sql::statements::update::UpdateStatement;
use crate::sql::subquery::Subquery;
use crate::sql::thing::Thing;
use crate::sql::value::{Value, Values};
use crate::sql::Cond;
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
			Force::All => self.ft(ctx, opt).await?,
			_ if self.changed() => self.ft(ctx, opt).await?,
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
							// What do we do with the initial value?
							if !targeted_force
								&& act != Action::Create && cond
								.compute(stk, ctx, opt, Some(&self.initial))
								.await?
								.is_truthy()
							{
								// Delete the old value in the table
								self.data(stk, ctx, opt, Action::Delete, old, &tb.expr).await?;
							}
							// What do we do with the current value?
							if act != Action::Delete
								&& cond
									.compute(stk, ctx, opt, Some(&self.current))
									.await?
									.is_truthy()
							{
								// Update the new value in the table
								self.data(stk, ctx, opt, Action::Update, rid, &tb.expr).await?;
							}
						}
						// No WHERE clause is specified
						None => {
							if !targeted_force && act != Action::Create {
								// Delete the old value in the table
								self.data(stk, ctx, opt, Action::Delete, old, &tb.expr).await?;
							}
							if act != Action::Delete {
								// Update the new value in the table
								self.data(stk, ctx, opt, Action::Update, rid, &tb.expr).await?;
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
									match act {
										// Delete the value in the table
										Action::Delete => {
											let stm = DeleteStatement {
												what: Values(vec![Value::from(rid)]),
												..DeleteStatement::default()
											};
											// Execute the statement
											stm.compute(stk, ctx, opt, None).await?;
										}
										// Update the value in the table
										_ => {
											let stm = UpdateStatement {
												what: Values(vec![Value::from(rid)]),
												data: Some(
													self.full(stk, ctx, opt, &tb.expr).await?,
												),
												..UpdateStatement::default()
											};
											// Execute the statement
											stm.compute(stk, ctx, opt, None).await?;
										}
									};
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
							match act {
								// Delete the value in the table
								Action::Delete => {
									let stm = DeleteStatement {
										what: Values(vec![Value::from(rid)]),
										..DeleteStatement::default()
									};
									// Execute the statement
									stm.compute(stk, ctx, opt, None).await?;
								}
								// Update the value in the table
								_ => {
									let stm = UpdateStatement {
										what: Values(vec![Value::from(rid)]),
										data: Some(self.full(stk, ctx, opt, &tb.expr).await?),
										..UpdateStatement::default()
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
		thg: Thing,
		exp: &Fields,
	) -> Result<(), Error> {
		// Create a new context with the initial or the current doc
		let doc = match act {
			Action::Delete => Some(&self.initial),
			Action::Update => Some(&self.current),
			_ => unreachable!(),
		};
		//
		let (set_ops, del_ops) = self.fields(stk, ctx, opt, act, doc, exp).await?;
		//
		let what = Values(vec![Value::from(thg.clone())]);
		let stm = UpdateStatement {
			what,
			data: Some(Data::SetExpression(set_ops)),
			..UpdateStatement::default()
		};
		stm.compute(stk, ctx, opt, None).await?;

		if !del_ops.is_empty() {
			let mut iter = del_ops.into_iter();
			if let Some((i, o, v)) = iter.next() {
				let mut root = Value::Expression(Box::new(Expression::Binary {
					l: Value::Idiom(i),
					o,
					r: v,
				}));
				for (i, o, v) in iter {
					let exp = Value::Expression(Box::new(Expression::Binary {
						l: Value::Idiom(i),
						o,
						r: v,
					}));
					root = Value::Expression(Box::new(Expression::Binary {
						l: root,
						o: Operator::Or,
						r: exp,
					}));
				}
				let what = Values(vec![Value::from(thg)]);
				let stm = DeleteStatement {
					what,
					cond: Some(Cond(root)),
					..DeleteStatement::default()
				};
				stm.compute(stk, ctx, opt, None).await?;
			}
		}
		Ok(())
	}

	async fn fields(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		act: Action,
		doc: Option<&CursorDoc<'_>>,
		exp: &Fields,
	) -> Result<(Ops, Ops), Error> {
		let mut set_ops: Ops = vec![];
		let mut del_ops: Ops = vec![];
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
							self.chg(&mut set_ops, &mut del_ops, &act, idiom, val);
						}
						Some("math::sum") => {
							let val = f.args()[0].compute(stk, ctx, opt, doc).await?;
							self.chg(&mut set_ops, &mut del_ops, &act, idiom, val);
						}
						Some("math::min") | Some("time::min") => {
							let val = f.args()[0].compute(stk, ctx, opt, doc).await?;
							self.min(&mut set_ops, &act, idiom, val);
						}
						Some("math::max") | Some("time::max") => {
							let val = f.args()[0].compute(stk, ctx, opt, doc).await?;
							self.max(&mut set_ops, &act, idiom, val);
						}
						Some("math::mean") => {
							let val = f.args()[0].compute(stk, ctx, opt, doc).await?;
							self.mean(&mut set_ops, &mut del_ops, &act, idiom, val);
						}
						_ => unreachable!(),
					},
					_ => {
						let val = expr.compute(stk, ctx, opt, doc).await?;
						self.set(&mut set_ops, idiom, val);
					}
				}
			}
		}
		Ok((set_ops, del_ops))
	}

	/// Set the field in the foreign table
	fn set(&self, ops: &mut Ops, key: Idiom, val: Value) {
		ops.push((key, Operator::Equal, val));
	}
	/// Increment or decrement the field in the foreign table
	fn chg(&self, set_ops: &mut Ops, del_ops: &mut Ops, act: &Action, key: Idiom, val: Value) {
		match act {
			Action::Update => {
				set_ops.push((key.clone(), Operator::Inc, val));
			}
			Action::Delete => {
				set_ops.push((key.clone(), Operator::Dec, val));
				// Add a purge condition (delete record if the number of values is 0)
				del_ops.push((key, Operator::Equal, Value::from(0)));
			}
			_ => unreachable!(),
		}
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
	fn mean(&self, set_ops: &mut Ops, del_ops: &mut Ops, act: &Action, key: Idiom, val: Value) {
		//
		let mut key_c = Idiom::from(vec![Part::from("__")]);
		key_c.0.push(Part::from(key.to_hash()));
		key_c.0.push(Part::from("c"));
		//
		set_ops.push((
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
		// Count the number of values
		let one = Value::from(1);
		match act {
			Action::Update => set_ops.push((key_c, Operator::Inc, one)),
			Action::Delete => {
				set_ops.push((key_c.clone(), Operator::Dec, one));
				// Add a purge condition (delete record if the number of values is 0)
				del_ops.push((key_c, Operator::Equal, Value::from(0)));
			}
			_ => unreachable!(),
		}
	}
}
