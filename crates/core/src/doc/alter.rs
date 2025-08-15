use std::sync::Arc;

use anyhow::{Result, bail, ensure};
use reblessive::tree::Stk;

use crate::ctx::{Context, MutableContext};
use crate::dbs::{Options, Statement, Workable};
use crate::doc::Document;
use crate::doc::Permitted::*;
use crate::err::Error;
use crate::expr::data::Data;
use crate::expr::paths::{ID, IN, OUT};
use crate::expr::{AssignOperator, FlowResultExt};
use crate::val::record::RecordType;
use crate::val::{RecordId, Value};

impl Document {
	/// Generate a record ID for CREATE, UPSERT, and UPDATE statements
	///
	/// This method handles record ID generation from various sources:
	/// - Existing document IDs
	/// - Data clause specified IDs (including function calls and expressions)
	/// - Randomly generated IDs when no ID is specified
	///
	/// The method ensures that all expressions are properly evaluated before
	/// being used as record IDs.
	pub(super) async fn generate_record_id(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<()> {
		// Check if we need to generate a record id
		if let Some(tb) = &self.r#gen {
			// This is a CREATE, UPSERT, UPDATE statement
			if let Workable::Normal = &self.extras {
				// Check if the document already has an ID from the current data
				let existing_id = self.current.doc.as_ref().pick(&*ID);
				if !existing_id.is_none() {
					// The document already has an ID, use it
					let id = existing_id.generate(tb.clone().into_strand(), false)?;
					self.id = Some(Arc::new(id));
					return Ok(());
				}

				// Fetch the record id if specified
				let id = match stm.data() {
					// There is a data clause so fetch a record id
					Some(data) => match data.rid(stk, ctx, opt).await? {
						Value::None => RecordId::random_for_table(tb.clone().into_string()),
						// Generate a new id from the id field
						// TODO: Handle null byte
						id => id.generate(tb.clone().into_strand(), false)?,
						// Generate a new random table id
					},
					// There is no data clause so create a record id
					None => RecordId::random_for_table(tb.clone().into_string()),
				};
				// The id field can not be a record range
				ensure!(
					!id.key.is_range(),
					Error::IdInvalid {
						value: id.to_string(),
					}
				);
				// Set the document id
				self.id = Some(Arc::new(id));
			}
			// This is a INSERT statement
			else if let Workable::Insert(_) = &self.extras {
				// TODO(tobiemh): implement last-step id generation for INSERT
				// statements
			}
			// This is a RELATE statement
			else if let Workable::Relate(_, _, _) = &self.extras {
				// TODO(tobiemh): implement last-step id generation for RELATE
				// statements
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
	pub(super) fn clear_record_data(&mut self) {
		*self.current.doc = Default::default();
	}
	/// Sets the default field data that should be
	/// present on this document. For normal records
	/// the `id` field is always specified, and for
	/// relation records, the `in`, `out`, and the
	/// hidden `edge` field are always present. This
	/// ensures that any user modifications of these
	/// fields are reset back to the original state.
	pub(super) async fn default_record_data(
		&mut self,
		_ctx: &Context,
		_opt: &Options,
		_stm: &Statement<'_>,
	) -> Result<()> {
		// Get the record id
		let rid = self.id()?;
		// Set default field values
		self.current.doc.to_mut().def(&rid);
		// This is a RELATE statement, so reset fields
		if let Workable::Relate(l, r, _) = &self.extras {
			// Mark that this is an edge node
			self.current.doc.set_record_type(RecordType::Edge);
			// If this document existed before, check the `in` field
			match (self.initial.doc.as_ref().pick(&*IN), self.is_new()) {
				// If the document id matches, then all good
				(Value::RecordId(id), false) if id.eq(l) => {
					self.current.doc.to_mut().put(&*IN, l.clone().into());
				}
				// If the document is new then all good
				(_, true) => {
					self.current.doc.to_mut().put(&*IN, l.clone().into());
				}
				// Otherwise this is attempting to override the `in` field
				(v, _) => {
					bail!(Error::InOverride {
						value: v.to_string(),
					})
				}
			}
			// If this document existed before, check the `out` field
			match (self.initial.doc.as_ref().pick(&*OUT), self.is_new()) {
				// If the document id matches, then all good
				(Value::RecordId(id), false) if id.eq(r) => {
					self.current.doc.to_mut().put(&*OUT, r.clone().into());
				}
				// If the document is new then all good
				(_, true) => {
					self.current.doc.to_mut().put(&*OUT, r.clone().into());
				}
				// Otherwise this is attempting to override the `in` field
				(v, _) => {
					bail!(Error::OutOverride {
						value: v.to_string(),
					})
				}
			}
		}
		// This is an UPDATE of a graph edge, so reset fields
		if self.initial.doc.is_edge() {
			self.current.doc.set_record_type(RecordType::Edge);
			self.current.doc.to_mut().put(&*IN, self.initial.doc.as_ref().pick(&*IN));
			self.current.doc.to_mut().put(&*OUT, self.initial.doc.as_ref().pick(&*OUT));
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
	pub(super) async fn process_merge_data(&mut self) -> Result<()> {
		// Get the record id
		let rid = self.id()?;
		// Set default field values
		self.current.doc.to_mut().def(&rid);
		// Process the permitted documents
		// This is an INSERT statement
		if let Workable::Insert(v) = &self.extras {
			self.current.doc.to_mut().merge(Value::clone(v))?;
		}
		// This is an INSERT RELATION statement
		if let Workable::Relate(_, _, Some(v)) = &self.extras {
			self.current.doc.to_mut().merge(Value::clone(v))?;
		}
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
	pub(super) async fn process_record_data(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<()> {
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
					let data = stk
						.run(|stk| data.compute(stk, ctx, opt, Some(current)))
						.await
						.catch_return()?;
					self.current.doc.to_mut().patch(data)?
				}
				Data::MergeExpression(data) => {
					// Process the permitted documents
					let current = match self.reduced(stk, ctx, opt, Current).await? {
						true => &self.current_reduced,
						false => &self.current,
					};
					// Process the MERGE data clause
					let data = stk
						.run(|stk| data.compute(stk, ctx, opt, Some(current)))
						.await
						.catch_return()?;
					self.current.doc.to_mut().merge(data)?
				}
				Data::ReplaceExpression(data) => {
					// Process the permitted documents
					let current = if self.reduced(stk, ctx, opt, Current).await? {
						&self.current_reduced
					} else {
						&self.current
					};
					// Process the REPLACE data clause
					let data = stk
						.run(|stk| data.compute(stk, ctx, opt, Some(current)))
						.await
						.catch_return()?;
					self.current.doc.to_mut().replace(data)?
				}
				Data::ContentExpression(data) => {
					// Process the permitted documents
					let current = if self.reduced(stk, ctx, opt, Current).await? {
						&self.current_reduced
					} else {
						&self.current
					};
					// Process the CONTENT data clause
					let data = stk
						.run(|stk| data.compute(stk, ctx, opt, Some(current)))
						.await
						.catch_return()?;
					self.current.doc.to_mut().replace(data)?
				}
				Data::UnsetExpression(i) => {
					for i in i.iter() {
						self.current.doc.to_mut().cut(i);
					}
				}
				Data::SetExpression(x) => {
					if self.reduced(stk, ctx, opt, Current).await? {
						for x in x.iter() {
							let v = stk
								.run(|stk| {
									x.value.compute(stk, ctx, opt, Some(&self.current_reduced))
								})
								.await
								.catch_return()?;
							match &x.operator {
								AssignOperator::Assign => match v {
									Value::None => {
										self.current_reduced
											.doc
											.to_mut()
											.del(stk, ctx, opt, &x.place)
											.await?;
										self.current
											.doc
											.to_mut()
											.del(stk, ctx, opt, &x.place)
											.await?;
									}
									_ => {
										self.current_reduced
											.doc
											.to_mut()
											.set(stk, ctx, opt, &x.place, v.clone())
											.await?;
										self.current
											.doc
											.to_mut()
											.set(stk, ctx, opt, &x.place, v)
											.await?;
									}
								},
								AssignOperator::Add => {
									self.current_reduced
										.doc
										.to_mut()
										.increment(stk, ctx, opt, &x.place, v.clone())
										.await?;
									self.current
										.doc
										.to_mut()
										.increment(stk, ctx, opt, &x.place, v)
										.await?;
								}
								AssignOperator::Subtract => {
									self.current_reduced
										.doc
										.to_mut()
										.decrement(stk, ctx, opt, &x.place, v.clone())
										.await?;
									self.current
										.doc
										.to_mut()
										.decrement(stk, ctx, opt, &x.place, v)
										.await?;
								}
								AssignOperator::Extend => {
									self.current_reduced
										.doc
										.to_mut()
										.extend(stk, ctx, opt, &x.place, v.clone())
										.await?;
									self.current
										.doc
										.to_mut()
										.extend(stk, ctx, opt, &x.place, v)
										.await?;
								}
							}
						}
					} else {
						for x in x.iter() {
							let v = stk
								.run(|stk| x.value.compute(stk, ctx, opt, Some(&self.current)))
								.await
								.catch_return()?;
							match &x.operator {
								AssignOperator::Assign => match v {
									Value::None => {
										self.current
											.doc
											.to_mut()
											.del(stk, ctx, opt, &x.place)
											.await?
									}
									_ => {
										self.current
											.doc
											.to_mut()
											.set(stk, ctx, opt, &x.place, v)
											.await?
									}
								},
								AssignOperator::Add => {
									self.current
										.doc
										.to_mut()
										.increment(stk, ctx, opt, &x.place, v)
										.await?
								}
								AssignOperator::Subtract => {
									self.current
										.doc
										.to_mut()
										.decrement(stk, ctx, opt, &x.place, v)
										.await?
								}
								AssignOperator::Extend => {
									self.current
										.doc
										.to_mut()
										.extend(stk, ctx, opt, &x.place, v)
										.await?
								}
							}
						}
					}
				}
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
					if self.reduced(stk, &ctx, opt, Current).await? {
						for x in x.iter() {
							let v = stk
								.run(|stk| {
									x.value.compute(stk, &ctx, opt, Some(&self.current_reduced))
								})
								.await
								.catch_return()?;
							match &x.operator {
								AssignOperator::Assign => match v {
									Value::None => {
										self.current_reduced
											.doc
											.to_mut()
											.del(stk, &ctx, opt, &x.place)
											.await?;
										self.current
											.doc
											.to_mut()
											.del(stk, &ctx, opt, &x.place)
											.await?;
									}
									_ => {
										self.current_reduced
											.doc
											.to_mut()
											.set(stk, &ctx, opt, &x.place, v.clone())
											.await?;
										self.current
											.doc
											.to_mut()
											.set(stk, &ctx, opt, &x.place, v)
											.await?;
									}
								},
								AssignOperator::Add => {
									self.current_reduced
										.doc
										.to_mut()
										.increment(stk, &ctx, opt, &x.place, v.clone())
										.await?;
									self.current
										.doc
										.to_mut()
										.increment(stk, &ctx, opt, &x.place, v)
										.await?;
								}
								AssignOperator::Subtract => {
									self.current_reduced
										.doc
										.to_mut()
										.decrement(stk, &ctx, opt, &x.place, v.clone())
										.await?;
									self.current
										.doc
										.to_mut()
										.decrement(stk, &ctx, opt, &x.place, v)
										.await?;
								}
								AssignOperator::Extend => {
									self.current_reduced
										.doc
										.to_mut()
										.extend(stk, &ctx, opt, &x.place, v.clone())
										.await?;
									self.current
										.doc
										.to_mut()
										.extend(stk, &ctx, opt, &x.place, v)
										.await?;
								}
							}
						}
					} else {
						for x in x.iter() {
							let v = stk
								.run(|stk| x.value.compute(stk, &ctx, opt, Some(&self.current)))
								.await
								.catch_return()?;
							match &x.operator {
								AssignOperator::Assign => match v {
									Value::None => {
										self.current
											.doc
											.to_mut()
											.del(stk, &ctx, opt, &x.place)
											.await?
									}
									_ => {
										self.current
											.doc
											.to_mut()
											.set(stk, &ctx, opt, &x.place, v)
											.await?
									}
								},
								AssignOperator::Add => {
									self.current
										.doc
										.to_mut()
										.increment(stk, &ctx, opt, &x.place, v)
										.await?
								}
								#[rustfmt::skip]
									    AssignOperator::Subtract => self.current.doc.to_mut().decrement(stk, &ctx, opt, &x.place, v).await?,
								#[rustfmt::skip]
									    AssignOperator::Extend => self.current.doc.to_mut().extend(stk, &ctx, opt, &x.place, v).await?,
							}
						}
					}
				}
				x => fail!("Unexpected data clause type: {x:?}"),
			};
		};
		// Set default field values
		self.current.doc.to_mut().def(&rid);
		// Carry on
		Ok(())
	}
}
