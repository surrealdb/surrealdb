use std::sync::Arc;

use anyhow::{Result, bail, ensure};
use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use crate::catalog::RecordType;
use crate::ctx::{Context, MutableContext};
use crate::dbs::{Options, Statement, Workable};
use crate::doc::Document;
use crate::doc::Permitted::*;
use crate::err::Error;
use crate::expr::data::Data;
use crate::expr::paths::{ID, IN, OUT};
use crate::expr::{AssignOperator, FlowResultExt, Idiom};
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
	pub(super) fn generate_record_id(&mut self) -> Result<()> {
		// Check if we need to generate a record id
		if let Some(tb) = &self.r#gen {
			// This is a CREATE, UPSERT, UPDATE, RELATE statement
			// Check if the document already has an ID from the current data
			let existing_id = self.current.doc.as_ref().pick(&*ID);
			let id = if existing_id.is_some() {
				// The document already has an ID, use it
				existing_id.generate(tb.clone(), false)?
			} else {
				// Fetch the record id if specified
				match &self.input_data {
					// There is a data clause so fetch a record id
					Some(data) => match data.rid() {
						Value::None => RecordId::random_for_table(tb.clone()),
						// Generate a new id from the id field
						// TODO: Handle null byte
						id => id.generate(tb.clone(), false)?,
						// Generate a new random table id
					},
					// There is no data clause so create a record id
					None => RecordId::random_for_table(tb.clone()),
				}
			};

			// The id field can not be a record range
			ensure!(
				!id.key.is_range(),
				Error::IdInvalid {
					value: id.to_sql(),
				}
			);
			// Set the document id
			self.id = Some(Arc::new(id));
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
				(Value::RecordId(id), false) if id == *l => {
					self.current.doc.to_mut().put(&*IN, l.clone().into());
				}
				// If the document is new then all good
				(_, true) => {
					self.current.doc.to_mut().put(&*IN, l.clone().into());
				}
				// Otherwise this is attempting to override the `in` field
				(v, _) => {
					bail!(Error::InOverride {
						value: v.to_sql(),
					})
				}
			}
			// If this document existed before, check the `out` field
			match (self.initial.doc.as_ref().pick(&*OUT), self.is_new()) {
				// If the document id matches, then all good
				(Value::RecordId(id), false) if id == *r => {
					self.current.doc.to_mut().put(&*OUT, r.clone().into());
				}
				// If the document is new then all good
				(_, true) => {
					self.current.doc.to_mut().put(&*OUT, r.clone().into());
				}
				// Otherwise this is attempting to override the `in` field
				(v, _) => {
					bail!(Error::OutOverride {
						value: v.to_sql(),
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
		// The statement has a data clause
		if let Some(v) = self.compute_input_data(stk, ctx, opt, stm).await? {
			match v.clone() {
				ComputedData::Patch(data) => {
					self.current.doc.to_mut().patch(data.as_ref().clone())?
				}
				ComputedData::Merge(data) => {
					self.current.doc.to_mut().merge(data.as_ref().clone())?
				}
				ComputedData::Replace(data) => {
					self.current.doc.to_mut().replace(data.as_ref().clone())?
				}
				ComputedData::Content(data) => {
					self.current.doc.to_mut().replace(data.as_ref().clone())?
				}
				ComputedData::Unset(i) => {
					for i in i.iter() {
						self.current.doc.to_mut().cut(i);
					}
				}
				ComputedData::Set(x, _) => {
					if self.reduced(stk, ctx, opt, Current).await? {
						apply_assignments(
							stk,
							ctx,
							opt,
							self.current_reduced.doc.to_mut(),
							x.clone(),
						)
						.await?;
					}

					apply_assignments(stk, ctx, opt, self.current.doc.to_mut(), x.clone()).await?;
				}
			};
		};
		// Carry on
		Ok(())
	}

	pub(super) async fn compute_input_data(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Option<&ComputedData>> {
		if self.input_data.is_none()
			&& let Some(data) = stm.data()
		{
			let doc = match self.reduced(stk, ctx, opt, Current).await? {
				true => &self.current_reduced,
				false => &self.current,
			};

			self.input_data = Some(match data {
				Data::PatchExpression(data) => ComputedData::Patch(Arc::new(
					data.compute(stk, ctx, opt, Some(doc)).await.catch_return()?,
				)),
				Data::MergeExpression(data) => ComputedData::Merge(Arc::new(
					data.compute(stk, ctx, opt, Some(doc)).await.catch_return()?,
				)),
				Data::ReplaceExpression(data) => ComputedData::Replace(Arc::new(
					data.compute(stk, ctx, opt, Some(doc)).await.catch_return()?,
				)),
				Data::ContentExpression(data) => ComputedData::Content(Arc::new(
					data.compute(stk, ctx, opt, Some(doc)).await.catch_return()?,
				)),
				Data::UnsetExpression(data) => ComputedData::Unset(data.clone()),
				x @ Data::SetExpression(data) | x @ Data::UpdateExpression(data) => {
					let assignments = {
						let ctx = if matches!(x, Data::UpdateExpression(_)) {
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
							ctx.freeze()
						} else {
							ctx.clone()
						};

						let mut assignments = Vec::with_capacity(data.len());
						for x in data.iter() {
							assignments.push(ComputedAssignment {
								place: x.place.clone(),
								operator: x.operator.clone(),
								value: x
									.value
									.compute(stk, &ctx, opt, Some(doc))
									.await
									.catch_return()?,
							});
						}

						assignments
					};

					let mut input = if self.reduced(stk, ctx, opt, Current).await? {
						self.initial_reduced.doc.as_ref().clone()
					} else {
						self.initial.doc.as_ref().clone()
					};
					apply_assignments(stk, ctx, opt, &mut input, assignments.clone()).await?;

					ComputedData::Set(assignments, Arc::new(input))
				}
				x => bail!("Unexpected data clause type: {x:?}"),
			});
		}

		Ok(self.input_data.as_ref())
	}

	pub(super) async fn compute_input_value(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Option<Arc<Value>>> {
		Ok(self.compute_input_data(stk, ctx, opt, stm).await?.map(|x| x.value()))
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(super) enum ComputedData {
	Patch(Arc<Value>),
	Merge(Arc<Value>),
	Replace(Arc<Value>),
	Content(Arc<Value>),
	Unset(Vec<Idiom>),
	Set(Vec<ComputedAssignment>, Arc<Value>),
}

impl ComputedData {
	pub(super) fn value(&self) -> Arc<Value> {
		match self {
			ComputedData::Patch(v) => v.clone(),
			ComputedData::Merge(v) => v.clone(),
			ComputedData::Replace(v) => v.clone(),
			ComputedData::Content(v) => v.clone(),
			ComputedData::Unset(_) => Arc::new(Value::None),
			ComputedData::Set(_, v) => v.clone(),
		}
	}

	pub(super) fn value_ref(&self) -> &Value {
		match self {
			ComputedData::Patch(v) => v.as_ref(),
			ComputedData::Merge(v) => v.as_ref(),
			ComputedData::Replace(v) => v.as_ref(),
			ComputedData::Content(v) => v.as_ref(),
			ComputedData::Unset(_) => &Value::None,
			ComputedData::Set(_, v) => v.as_ref(),
		}
	}

	pub(super) fn rid(&self) -> Value {
		self.value_ref().pick(&*ID)
	}

	pub(super) fn is_patch(&self) -> bool {
		matches!(self, ComputedData::Patch(_))
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(super) struct ComputedAssignment {
	pub place: Idiom,
	pub operator: AssignOperator,
	pub value: Value,
}

async fn apply_assignments(
	stk: &mut Stk,
	ctx: &Context,
	opt: &Options,
	doc: &mut Value,
	assignments: Vec<ComputedAssignment>,
) -> Result<()> {
	for x in assignments {
		match &x.operator {
			AssignOperator::Assign => match x.value {
				Value::None => doc.del(stk, ctx, opt, &x.place).await?,
				_ => doc.set(stk, ctx, opt, &x.place, x.value).await?,
			},
			AssignOperator::Add => doc.increment(stk, ctx, opt, &x.place, x.value).await?,
			AssignOperator::Subtract => doc.decrement(stk, ctx, opt, &x.place, x.value).await?,
			AssignOperator::Extend => doc.extend(stk, ctx, opt, &x.place, x.value).await?,
		}
	}
	Ok(())
}
