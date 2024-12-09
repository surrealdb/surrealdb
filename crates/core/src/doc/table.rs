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
use crate::sql::statements::upsert::UpsertStatement;
use crate::sql::statements::{DefineTableStatement, SelectStatement};
use crate::sql::subquery::Subquery;
use crate::sql::thing::Thing;
use crate::sql::value::{Value, Values};
use crate::sql::{Cond, Function, Groups, View};
use futures::future::try_join_all;
use reblessive::tree::Stk;

type Ops = Vec<(Idiom, Operator, Value)>;

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
	ft: &'a DefineTableStatement,
	act: FieldAction,
	view: &'a View,
	groups: &'a Groups,
	group_ids: Vec<Value>,
	doc: &'a CursorDoc,
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
						self.id.as_ref().is_some_and(|id| v.what.iter().any(|p| p.0 == id.tb))
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
			let tb = ft.view.as_ref().unwrap();
			// Check if there is a GROUP BY clause
			match &tb.group {
				// There is a GROUP BY clause specified
				Some(group) => {
					// Check if a WHERE clause is specified
					match &tb.cond {
						// There is a WHERE clause specified
						Some(cond) => {
							// What do we do with the initial value on UPDATE and DELETE?
							if !targeted_force
								&& act != Action::Create && cond
								.compute(stk, ctx, opt, Some(&self.initial))
								.await?
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
								&& cond
									.compute(stk, ctx, opt, Some(&self.current))
									.await?
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
							if !targeted_force && act != Action::Create {
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
							if act != Action::Delete {
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
											let stm = UpsertStatement {
												what: Values(vec![Value::from(rid)]),
												data: Some(
													self.full(stk, ctx, opt, &tb.expr).await?,
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
									let stm = UpsertStatement {
										what: Values(vec![Value::from(rid)]),
										data: Some(self.full(stk, ctx, opt, &tb.expr).await?),
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
	) -> Result<Vec<Value>, Error> {
		Ok(stk
			.scope(|scope| {
				try_join_all(
					group.iter().map(|v| scope.run(|stk| v.compute(stk, ctx, opt, Some(doc)))),
				)
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
	) -> Result<Data, Error> {
		let mut data = exp.compute(stk, ctx, opt, Some(&self.current), false).await?;
		data.cut(ID.as_ref());
		Ok(Data::ReplaceExpression(data))
	}
	//
	async fn data(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		fdc: FieldDataContext<'_>,
	) -> Result<(), Error> {
		//
		let (set_ops, del_ops) = self.fields(stk, ctx, opt, &fdc).await?;
		//
		let thg = Thing {
			tb: fdc.ft.name.to_raw(),
			id: fdc.group_ids.into(),
		};
		let what = Values(vec![Value::from(thg.clone())]);
		let stm = UpsertStatement {
			what,
			data: Some(Data::SetExpression(set_ops)),
			..UpsertStatement::default()
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
		ctx: &Context,
		opt: &Options,
		fdc: &FieldDataContext<'_>,
	) -> Result<(Ops, Ops), Error> {
		let mut set_ops: Ops = vec![];
		let mut del_ops: Ops = vec![];
		//
		for field in fdc.view.expr.other() {
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
							let val = f.compute(stk, ctx, opt, Some(fdc.doc)).await?;
							self.chg(&mut set_ops, &mut del_ops, &fdc.act, idiom, val)?;
						}
						Some(name) if name == "time::min" => {
							let val = f.args()[0].compute(stk, ctx, opt, Some(fdc.doc)).await?;
							let val = match val {
								val @ Value::Datetime(_) => val,
								val => {
									return Err(Error::InvalidAggregation {
										name: name.to_string(),
										table: fdc.ft.name.to_raw(),
										message: format!(
											"This function expects a datetime but found {val}"
										),
									})
								}
							};
							self.min(&mut set_ops, &mut del_ops, fdc, field, idiom, val)?;
						}
						Some(name) if name == "time::max" => {
							let val = f.args()[0].compute(stk, ctx, opt, Some(fdc.doc)).await?;
							let val = match val {
								val @ Value::Datetime(_) => val,
								val => {
									return Err(Error::InvalidAggregation {
										name: name.to_string(),
										table: fdc.ft.name.to_raw(),
										message: format!(
											"This function expects a datetime but found {val}"
										),
									})
								}
							};
							self.max(&mut set_ops, &mut del_ops, fdc, field, idiom, val)?;
						}
						Some(name) if name == "math::sum" => {
							let val = f.args()[0].compute(stk, ctx, opt, Some(fdc.doc)).await?;
							let val = match val {
								val @ Value::Number(_) => val,
								val => {
									return Err(Error::InvalidAggregation {
										name: name.to_string(),
										table: fdc.ft.name.to_raw(),
										message: format!(
											"This function expects a number but found {val}"
										),
									})
								}
							};
							self.chg(&mut set_ops, &mut del_ops, &fdc.act, idiom, val)?;
						}
						Some(name) if name == "math::min" => {
							let val = f.args()[0].compute(stk, ctx, opt, Some(fdc.doc)).await?;
							let val = match val {
								val @ Value::Number(_) => val,
								val => {
									return Err(Error::InvalidAggregation {
										name: name.to_string(),
										table: fdc.ft.name.to_raw(),
										message: format!(
											"This function expects a number but found {val}"
										),
									})
								}
							};
							self.min(&mut set_ops, &mut del_ops, fdc, field, idiom, val)?;
						}
						Some(name) if name == "math::max" => {
							let val = f.args()[0].compute(stk, ctx, opt, Some(fdc.doc)).await?;
							let val = match val {
								val @ Value::Number(_) => val,
								val => {
									return Err(Error::InvalidAggregation {
										name: name.to_string(),
										table: fdc.ft.name.to_raw(),
										message: format!(
											"This function expects a number but found {val}"
										),
									})
								}
							};
							self.max(&mut set_ops, &mut del_ops, fdc, field, idiom, val)?;
						}
						Some(name) if name == "math::mean" => {
							let val = f.args()[0].compute(stk, ctx, opt, Some(fdc.doc)).await?;
							let val = match val {
								val @ Value::Number(_) => val.coerce_to_decimal()?.into(),
								val => {
									return Err(Error::InvalidAggregation {
										name: name.to_string(),
										table: fdc.ft.name.to_raw(),
										message: format!(
											"This function expects a number but found {val}"
										),
									})
								}
							};
							self.mean(&mut set_ops, &mut del_ops, &fdc.act, idiom, val)?;
						}
						f => return Err(fail!("Unexpected function {f:?} encountered")),
					},
					_ => {
						let val = expr.compute(stk, ctx, opt, Some(fdc.doc)).await?;
						self.set(&mut set_ops, idiom, val)?;
					}
				}
			}
		}
		Ok((set_ops, del_ops))
	}

	/// Set the field in the foreign table
	fn set(&self, ops: &mut Ops, key: Idiom, val: Value) -> Result<(), Error> {
		ops.push((key, Operator::Equal, val));
		// Everything ok
		Ok(())
	}
	/// Increment or decrement the field in the foreign table
	fn chg(
		&self,
		set_ops: &mut Ops,
		del_ops: &mut Ops,
		act: &FieldAction,
		key: Idiom,
		val: Value,
	) -> Result<(), Error> {
		match act {
			FieldAction::Add => {
				set_ops.push((key.clone(), Operator::Inc, val));
			}
			FieldAction::Sub => {
				set_ops.push((key.clone(), Operator::Dec, val));
				// Add a purge condition (delete record if the number of values is 0)
				del_ops.push((key, Operator::Equal, Value::from(0)));
			}
		}
		// Everything ok
		Ok(())
	}

	/// Set the new minimum value for the field in the foreign table
	fn min(
		&self,
		set_ops: &mut Ops,
		del_ops: &mut Ops,
		fdc: &FieldDataContext,
		field: &Field,
		key: Idiom,
		val: Value,
	) -> Result<(), Error> {
		// Key for the value count
		let mut key_c = Idiom::from(vec![Part::from("__")]);
		key_c.0.push(Part::from(key.to_hash()));
		key_c.0.push(Part::from("c"));
		match fdc.act {
			FieldAction::Add => {
				set_ops.push((
					key.clone(),
					Operator::Equal,
					Value::Subquery(Box::new(Subquery::Ifelse(IfelseStatement {
						exprs: vec![(
							Value::Expression(Box::new(Expression::Binary {
								l: Value::Expression(Box::new(Expression::Binary {
									l: Value::Idiom(key.clone()),
									o: Operator::Exact,
									r: Value::None,
								})),
								o: Operator::Or,
								r: Value::Expression(Box::new(Expression::Binary {
									l: Value::Idiom(key.clone()),
									o: Operator::MoreThan,
									r: val.clone(),
								})),
							})),
							val,
						)],
						close: Some(Value::Idiom(key)),
					}))),
				));
				set_ops.push((key_c, Operator::Inc, Value::from(1)))
			}
			FieldAction::Sub => {
				// If it is equal to the previous MIN value,
				// as we can't know what was the previous MIN value,
				// we have to recompute it
				let subquery = Self::one_group_query(fdc, field, &key, val)?;
				set_ops.push((key.clone(), Operator::Equal, subquery));
				//  Decrement the number of values
				set_ops.push((key_c.clone(), Operator::Dec, Value::from(1)));
				// Add a purge condition (delete record if the number of values is 0)
				del_ops.push((key_c, Operator::Equal, Value::from(0)));
			}
		}
		// Everything ok
		Ok(())
	}
	/// Set the new maximum value for the field in the foreign table
	fn max(
		&self,
		set_ops: &mut Ops,
		del_ops: &mut Ops,
		fdc: &FieldDataContext,
		field: &Field,
		key: Idiom,
		val: Value,
	) -> Result<(), Error> {
		// Key for the value count
		let mut key_c = Idiom::from(vec![Part::from("__")]);
		key_c.0.push(Part::from(key.to_hash()));
		key_c.0.push(Part::from("c"));
		//
		match fdc.act {
			FieldAction::Add => {
				set_ops.push((
					key.clone(),
					Operator::Equal,
					Value::Subquery(Box::new(Subquery::Ifelse(IfelseStatement {
						exprs: vec![(
							Value::Expression(Box::new(Expression::Binary {
								l: Value::Expression(Box::new(Expression::Binary {
									l: Value::Idiom(key.clone()),
									o: Operator::Exact,
									r: Value::None,
								})),
								o: Operator::Or,
								r: Value::Expression(Box::new(Expression::Binary {
									l: Value::Idiom(key.clone()),
									o: Operator::LessThan,
									r: val.clone(),
								})),
							})),
							val,
						)],
						close: Some(Value::Idiom(key)),
					}))),
				));
				set_ops.push((key_c, Operator::Inc, Value::from(1)))
			}
			FieldAction::Sub => {
				// If it is equal to the previous MAX value,
				// as we can't know what was the previous MAX value,
				// we have to recompute the MAX
				let subquery = Self::one_group_query(fdc, field, &key, val)?;
				set_ops.push((key.clone(), Operator::Equal, subquery));
				//  Decrement the number of values
				set_ops.push((key_c.clone(), Operator::Dec, Value::from(1)));
				// Add a purge condition (delete record if the number of values is 0)
				del_ops.push((key_c, Operator::Equal, Value::from(0)));
			}
		}
		// Everything ok
		Ok(())
	}

	/// Set the new average value for the field in the foreign table
	fn mean(
		&self,
		set_ops: &mut Ops,
		del_ops: &mut Ops,
		act: &FieldAction,
		key: Idiom,
		val: Value,
	) -> Result<(), Error> {
		// Key for the value count
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
							FieldAction::Sub => Operator::Sub,
							FieldAction::Add => Operator::Add,
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
							FieldAction::Sub => Operator::Sub,
							FieldAction::Add => Operator::Add,
						},
						r: Value::from(1),
					},
				))))),
			})),
		));
		match act {
			//  Increment the number of values
			FieldAction::Add => set_ops.push((key_c, Operator::Inc, Value::from(1))),
			FieldAction::Sub => {
				//  Decrement the number of values
				set_ops.push((key_c.clone(), Operator::Dec, Value::from(1)));
				// Add a purge condition (delete record if the number of values is 0)
				del_ops.push((key_c, Operator::Equal, Value::from(0)));
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
	) -> Result<Value, Error> {
		// Build the condition merging the optional user provided condition and the group
		let mut iter = fdc.groups.0.iter().enumerate();
		let cond = if let Some((i, g)) = iter.next() {
			let mut root = Value::Expression(Box::new(Expression::Binary {
				l: Value::Idiom(g.0.clone()),
				o: Operator::Equal,
				r: fdc.group_ids[i].clone(),
			}));
			for (i, g) in iter {
				let exp = Value::Expression(Box::new(Expression::Binary {
					l: Value::Idiom(g.0.clone()),
					o: Operator::Equal,
					r: fdc.group_ids[i].clone(),
				}));
				root = Value::Expression(Box::new(Expression::Binary {
					l: root,
					o: Operator::And,
					r: exp,
				}));
			}
			if let Some(c) = &fdc.view.cond {
				root = Value::Expression(Box::new(Expression::Binary {
					l: root,
					o: Operator::And,
					r: c.0.clone(),
				}));
			}
			Some(Cond(root))
		} else {
			fdc.view.cond.clone()
		};

		let group_select = Value::Subquery(Box::new(Subquery::Select(SelectStatement {
			expr: Fields(vec![field.clone()], false),
			cond,
			what: (&fdc.view.what).into(),
			group: Some(fdc.groups.clone()),
			..SelectStatement::default()
		})));
		let array_first = Value::Function(Box::new(Function::Normal(
			"array::first".to_string(),
			vec![group_select],
		)));
		let ident = match field {
			Field::Single {
				alias: Some(alias),
				..
			} => match alias.0.first() {
				Some(Part::Field(ident)) => ident.clone(),
				p => return Err(fail!("Unexpected ident type encountered: {p:?}")),
			},
			f => return Err(fail!("Unexpected field type encountered: {f:?}")),
		};
		let compute_query = Value::Idiom(Idiom(vec![Part::Start(array_first), Part::Field(ident)]));
		Ok(Value::Subquery(Box::new(Subquery::Ifelse(IfelseStatement {
			exprs: vec![(
				Value::Expression(Box::new(Expression::Binary {
					l: Value::Idiom(key.clone()),
					o: Operator::Equal,
					r: val.clone(),
				})),
				compute_query,
			)],
			close: Some(Value::Idiom(key.clone())),
		}))))
	}
}
