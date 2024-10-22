use crate::ctx::{Context, MutableContext};
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Workable;
use crate::doc::Document;
use crate::doc::Permitted::*;
use crate::err::Error;
use crate::sql::data::Data;
use crate::sql::operator::Operator;
use crate::sql::paths::EDGE;
use crate::sql::paths::IN;
use crate::sql::paths::OUT;
use crate::sql::value::Value;
use reblessive::tree::Stk;
use std::sync::Arc;

impl Document {
	/// Generates a new record id for this document.
	/// This only happens when a document does not
	/// have a record id, because we are attempting
	/// to create a new record, and are leaving the
	/// id generation up to the document processor.
	pub(crate) async fn generate_record_id(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check if we need to generate a record id
		if let Some(tb) = &self.gen {
			// This is a CREATE, UPSERT, UPDATE statement
			if let Workable::Normal = &self.extras {
				// Fetch the record id if specified
				let id = match stm.data() {
					// There is a data clause so fetch a record id
					Some(data) => match data.rid(stk, ctx, opt).await? {
						// Generate a new id from the id field
						Some(id) => id.generate(tb, false)?,
						// Generate a new random table id
						None => tb.generate(),
					},
					// There is no data clause so create a record id
					None => tb.generate(),
				};
				// The id field can not be a record range
				if id.is_range() {
					return Err(Error::IdInvalid {
						value: id.to_string(),
					});
				}
				// Set the document id
				self.id = Some(Arc::new(id));
			}
			// This is a INSERT statement
			else if let Workable::Insert(_) = &self.extras {
				// TODO(tobiemh): implement last-step id generation for INSERT statements
			}
			// This is a RELATE statement
			else if let Workable::Relate(_, _, _) = &self.extras {
				// TODO(tobiemh): implement last-step id generation for RELATE statements
			}
		}
		//
		Ok(())
	}
	/// Clears all of the content of this document.
	/// This is used to empty the current content
	/// of the document within a `DELETE` statement.
	/// This function only clears the document in
	/// memory, and does not store this on disk.
	pub async fn clear_record_data(
		&mut self,
		_ctx: &Context,
		_opt: &Options,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		self.current.doc.to_mut().clear()
	}
	/// Sets the default field data that should be
	/// present on this document. For normal records
	/// the `id` field is always specified, and for
	/// relation records, the `in`, `out`, and the
	/// hidden `edge` field are always present. This
	/// ensures that any user modifications of these
	/// fields are reset back to the original state.
	pub async fn default_record_data(
		&mut self,
		_ctx: &Context,
		_opt: &Options,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Get the record id
		let rid = self.id()?;
		// Set default field values
		self.current.doc.to_mut().def(&rid);
		// This is a RELATE statement, so reset fields
		if let Workable::Relate(l, r, _) = &self.extras {
			// Mark that this is an edge node
			self.current.doc.to_mut().put(&*EDGE, Value::Bool(true));
			// If this document existed before, check the `in` field
			match (self.initial.doc.pick(&*IN), self.is_new()) {
				// If the document id matches, then all good
				(Value::Thing(id), false) if id.eq(l) => {
					self.current.doc.to_mut().put(&*IN, l.clone().into());
				}
				// If the document is new then all good
				(_, true) => {
					self.current.doc.to_mut().put(&*IN, l.clone().into());
				}
				// Otherwise this is attempting to override the `in` field
				(v, _) => {
					return Err(Error::InOverride {
						value: v.to_string(),
					})
				}
			}
			// If this document existed before, check the `out` field
			match (self.initial.doc.pick(&*OUT), self.is_new()) {
				// If the document id matches, then all good
				(Value::Thing(id), false) if id.eq(r) => {
					self.current.doc.to_mut().put(&*OUT, r.clone().into());
				}
				// If the document is new then all good
				(_, true) => {
					self.current.doc.to_mut().put(&*OUT, r.clone().into());
				}
				// Otherwise this is attempting to override the `in` field
				(v, _) => {
					return Err(Error::OutOverride {
						value: v.to_string(),
					})
				}
			}
		}
		// This is an UPDATE of a graph edge, so reset fields
		if self.initial.doc.pick(&*EDGE).is_true() {
			self.current.doc.to_mut().put(&*EDGE, Value::Bool(true));
			self.current.doc.to_mut().put(&*IN, self.initial.doc.pick(&*IN));
			self.current.doc.to_mut().put(&*OUT, self.initial.doc.pick(&*OUT));
		}
		// Carry on
		Ok(())
	}
	/// Updates the current document using the data
	/// passed in to each document. This is relevant
	/// for INSERT and RELATE queries where each
	/// document has its own data block. This
	/// function also ensures that standard default
	/// fields are set and reset before and after the
	/// document data is modified.
	pub async fn process_merge_data(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Get the record id
		let rid = self.id()?;
		// Set default field values
		self.current.doc.to_mut().def(&rid);
		// Process the permitted documents
		match self.reduced(stk, ctx, opt, Current).await? {
			true => {
				// This is an INSERT statement
				if let Workable::Insert(v) = &self.extras {
					let v = v.compute(stk, ctx, opt, Some(&self.current_reduced)).await?;
					self.current.doc.to_mut().merge(v)?;
				}
				// This is an INSERT RELATION statement
				if let Workable::Relate(_, _, Some(v)) = &self.extras {
					let v = v.compute(stk, ctx, opt, Some(&self.current_reduced)).await?;
					self.current.doc.to_mut().merge(v)?;
				}
			}
			false => {
				// This is an INSERT statement
				if let Workable::Insert(v) = &self.extras {
					let v = v.compute(stk, ctx, opt, Some(&self.current)).await?;
					self.current.doc.to_mut().merge(v)?;
				}
				// This is an INSERT RELATION statement
				if let Workable::Relate(_, _, Some(v)) = &self.extras {
					let v = v.compute(stk, ctx, opt, Some(&self.current)).await?;
					self.current.doc.to_mut().merge(v)?;
				}
			}
		};
		// Set default field values
		self.current.doc.to_mut().def(&rid);
		// Carry on
		Ok(())
	}
	/// Updates the current document using the data
	/// clause present on the statement. This can be
	/// one of CONTENT, REPLACE, MERGE, PATCH, SET,
	/// UNSET, or ON DUPLICATE KEY UPDATE. This
	/// function also ensures that standard default
	/// fields are set and reset before and after the
	/// document data is modified.
	pub async fn process_record_data(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Get the record id
		let rid = self.id()?;
		// Set default field values
		self.current.doc.to_mut().def(&rid);
		// The statement has a data clause
		if let Some(v) = stm.data() {
			match v {
				Data::PatchExpression(data) => {
					// Process the permitted documents
					let current = match self.reduced(stk, ctx, opt, Current).await? {
						true => &self.current_reduced,
						false => &self.current,
					};
					// Process the PATCH data clause
					let data = data.compute(stk, ctx, opt, Some(current)).await?;
					self.current.doc.to_mut().patch(data)?
				}
				Data::MergeExpression(data) => {
					// Process the permitted documents
					let current = match self.reduced(stk, ctx, opt, Current).await? {
						true => &self.current_reduced,
						false => &self.current,
					};
					// Process the MERGE data clause
					let data = data.compute(stk, ctx, opt, Some(current)).await?;
					self.current.doc.to_mut().merge(data)?
				}
				Data::ReplaceExpression(data) => {
					// Process the permitted documents
					let current = match self.reduced(stk, ctx, opt, Current).await? {
						true => &self.current_reduced,
						false => &self.current,
					};
					// Process the REPLACE data clause
					let data = data.compute(stk, ctx, opt, Some(current)).await?;
					self.current.doc.to_mut().replace(data)?
				}
				Data::ContentExpression(data) => {
					// Process the permitted documents
					let current = match self.reduced(stk, ctx, opt, Current).await? {
						true => &self.current_reduced,
						false => &self.current,
					};
					// Process the CONTENT data clause
					let data = data.compute(stk, ctx, opt, Some(current)).await?;
					self.current.doc.to_mut().replace(data)?
				}
				Data::UnsetExpression(i) => {
					for i in i.iter() {
						self.current.doc.to_mut().cut(i);
					}
				}
				Data::SetExpression(x) => match self.reduced(stk, ctx, opt, Current).await? {
					true => {
						for x in x.iter() {
							#[rustfmt::skip]
							let v = x.2.compute(stk, ctx, opt, Some(&self.current_reduced)).await?;
							match &x.1 {
								#[rustfmt::skip]
								Operator::Equal => match v {
									Value::None => {
										self.current_reduced.doc.to_mut().del(stk, ctx, opt, &x.0).await?;
										self.current.doc.to_mut().del(stk, ctx, opt, &x.0).await?;
									},
									_ => {
										self.current_reduced.doc.to_mut().set(stk, ctx, opt, &x.0, v.clone()).await?;
										self.current.doc.to_mut().set(stk, ctx, opt, &x.0, v).await?;
									},
								},
								#[rustfmt::skip]
								Operator::Inc => {
									self.current_reduced.doc.to_mut().increment(stk, ctx, opt, &x.0, v.clone()).await?;
									self.current.doc.to_mut().increment(stk, ctx, opt, &x.0, v).await?;
								}
								#[rustfmt::skip]
								Operator::Dec => {
									self.current_reduced.doc.to_mut().decrement(stk, ctx, opt, &x.0, v.clone()).await?;
									self.current.doc.to_mut().decrement(stk, ctx, opt, &x.0, v).await?;
								}
								#[rustfmt::skip]
								Operator::Ext => {
									self.current_reduced.doc.to_mut().extend(stk, ctx, opt, &x.0, v.clone()).await?;
									self.current.doc.to_mut().extend(stk, ctx, opt, &x.0, v).await?;
								}
								#[rustfmt::skip]
								o => { return Err(fail!("Unexpected operator in SET clause: {o:?}")); }
							}
						}
					}
					false => {
						for x in x.iter() {
							#[rustfmt::skip]
							let v = x.2.compute(stk, ctx, opt, Some(&self.current)).await?;
							match &x.1 {
								#[rustfmt::skip]
								Operator::Equal => match v {
									Value::None => self.current.doc.to_mut().del(stk, ctx, opt, &x.0).await?,
									_ => self.current.doc.to_mut().set(stk, ctx, opt, &x.0, v).await?,
								},
								#[rustfmt::skip]
								Operator::Inc => self.current.doc.to_mut().increment(stk, ctx, opt, &x.0, v).await?,
								#[rustfmt::skip]
								Operator::Dec => self.current.doc.to_mut().decrement(stk, ctx, opt, &x.0, v).await?,
								#[rustfmt::skip]
								Operator::Ext => self.current.doc.to_mut().extend(stk, ctx, opt, &x.0, v).await?,
								#[rustfmt::skip]
								o => { return Err(fail!("Unexpected operator in SET clause: {o:?}")); }
							}
						}
					}
				},
				Data::UpdateExpression(x) => {
					// Duplicate context
					let mut ctx = MutableContext::new(ctx);
					// Add insertable value
					if let Workable::Insert(value) = &self.extras {
						ctx.add_value("input", value.clone());
					}
					if let Workable::Relate(_, _, Some(value)) = &self.extras {
						ctx.add_value("input", value.clone());
					}
					// Freeze the context
					let ctx = ctx.freeze();
					// Process ON DUPLICATE KEY clause
					match self.reduced(stk, &ctx, opt, Current).await? {
						true => {
							for x in x.iter() {
								#[rustfmt::skip]
								let v = x.2.compute(stk, &ctx, opt, Some(&self.current_reduced)).await?;
								match &x.1 {
									#[rustfmt::skip]
									Operator::Equal => match v {
										Value::None => {
											self.current_reduced.doc.to_mut().del(stk, &ctx, opt, &x.0).await?;
											self.current.doc.to_mut().del(stk, &ctx, opt, &x.0).await?;
										},
										_ => {
											self.current_reduced.doc.to_mut().set(stk, &ctx, opt, &x.0, v.clone()).await?;
											self.current.doc.to_mut().set(stk, &ctx, opt, &x.0, v).await?;
										},
									},
									#[rustfmt::skip]
									Operator::Inc => {
										self.current_reduced.doc.to_mut().increment(stk, &ctx, opt, &x.0, v.clone()).await?;
										self.current.doc.to_mut().increment(stk, &ctx, opt, &x.0, v).await?;
									}
									#[rustfmt::skip]
									Operator::Dec => {
										self.current_reduced.doc.to_mut().decrement(stk, &ctx, opt, &x.0, v.clone()).await?;
										self.current.doc.to_mut().decrement(stk, &ctx, opt, &x.0, v).await?;
									}
									#[rustfmt::skip]
									Operator::Ext => {
										self.current_reduced.doc.to_mut().extend(stk, &ctx, opt, &x.0, v.clone()).await?;
										self.current.doc.to_mut().extend(stk, &ctx, opt, &x.0, v).await?;
									}
									#[rustfmt::skip]
									o => { return Err(fail!("Unexpected operator in UPDATE clause: {o:?}")); }
								}
							}
						}
						false => {
							for x in x.iter() {
								#[rustfmt::skip]
								let v = x.2.compute(stk, &ctx, opt, Some(&self.current)).await?;
								match &x.1 {
									#[rustfmt::skip]
									Operator::Equal => match v {
										Value::None => self.current.doc.to_mut().del(stk, &ctx, opt, &x.0).await?,
										_ => self.current.doc.to_mut().set(stk, &ctx, opt, &x.0, v).await?,
									},
									#[rustfmt::skip]
									Operator::Inc => self.current.doc.to_mut().increment(stk, &ctx, opt, &x.0, v).await?,
									#[rustfmt::skip]
									Operator::Dec => self.current.doc.to_mut().decrement(stk, &ctx, opt, &x.0, v).await?,
									#[rustfmt::skip]
									Operator::Ext => self.current.doc.to_mut().extend(stk, &ctx, opt, &x.0, v).await?,
									#[rustfmt::skip]
									o => { return Err(fail!("Unexpected operator in UPDATE clause: {o:?}")); }
								}
							}
						}
					}
				}
				x => return Err(fail!("Unexpected data clause type: {x:?}")),
			};
		};
		// Set default field values
		self.current.doc.to_mut().def(&rid);
		// Carry on
		Ok(())
	}
}
