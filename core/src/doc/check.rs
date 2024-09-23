use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Workable;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::paths::ID;
use crate::sql::paths::IN;
use crate::sql::paths::OUT;
use crate::sql::permission::Permission;
use crate::sql::value::Value;
use reblessive::tree::Stk;

impl Document {
	/// Checks whether this operation is allowed on
	/// the table for this document. When inserting
	/// an edge or relation, we check that the table
	/// type is `ANY` or `RELATION`. When inserting
	/// a node or normal record, we check that the
	/// table type is `ANY` or `NORMAL`.
	pub async fn check_table_type(
		&mut self,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Get the table for this document
		let tb = self.tb(ctx, opt).await?;
		// Determine the type of statement
		match stm {
			Statement::Create(_) => {
				if !tb.allows_normal() {
					return Err(Error::TableCheck {
						thing: self.id()?.to_string(),
						relation: false,
						target_type: tb.kind.to_string(),
					});
				}
			}
			Statement::Upsert(_) => {
				if !tb.allows_normal() {
					return Err(Error::TableCheck {
						thing: self.id()?.to_string(),
						relation: false,
						target_type: tb.kind.to_string(),
					});
				}
			}
			Statement::Relate(_) => {
				if !tb.allows_relation() {
					return Err(Error::TableCheck {
						thing: self.id()?.to_string(),
						relation: true,
						target_type: tb.kind.to_string(),
					});
				}
			}
			Statement::Insert(_) => match self.extras {
				Workable::Relate(_, _, _, _) => {
					if !tb.allows_relation() {
						return Err(Error::TableCheck {
							thing: self.id()?.to_string(),
							relation: true,
							target_type: tb.kind.to_string(),
						});
					}
				}
				_ => {
					if !tb.allows_normal() {
						return Err(Error::TableCheck {
							thing: self.id()?.to_string(),
							relation: false,
							target_type: tb.kind.to_string(),
						});
					}
				}
			},
			_ => {}
		}
		// Carry on
		Ok(())
	}
	/// Checks that a specifically selected record
	/// actually exists in the underlying datastore.
	/// If the user specifies a record directly
	/// using a Record ID, and that record does not
	/// exist, then this function will exit early.
	pub async fn check_record_exists(
		&self,
		_ctx: &Context,
		_opt: &Options,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check if this record exists
		if self.id.is_some() && self.current.doc.is_none() {
			return Err(Error::Ignore);
		}
		// Carry on
		Ok(())
	}
	/// Checks that a specifically selected record
	/// actually exists in the underlying datastore.
	/// If the user specifies a record directly
	/// using a Record ID, and that record does not
	/// exist, then this function will exit early.
	pub async fn check_data_fields(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Get the record id
		let rid = self.id()?;
		// You cannot store a range id as the id field on a document
		if rid.is_range() {
			return Err(Error::IdInvalid {
				value: rid.to_string(),
			});
		}
		// This is a CREATE, UPSERT, UPDATE statement
		if let Workable::Normal = &self.extras {
			// This is a CONTENT, MERGE or SET clause
			if let Some(data) = stm.data() {
				// Check if there is an id field specified
				if let Some(field) = data.pick(stk, ctx, opt, &*ID).await? {
					match field {
						// You cannot store a range id as the id field on a document
						Value::Thing(v) if v.is_range() => {
							return Err(Error::IdInvalid {
								value: v.to_string(),
							})
						}
						// The id is a match, so don't error
						Value::Thing(v) if v.eq(&rid) => (),
						// The id is a match, so don't error
						v if rid.id.is(&v) => (),
						// The in field does not match
						v => {
							return Err(Error::IdMismatch {
								value: v.to_string(),
							})
						}
					}
				}
			}
		}
		// This is a RELATE statement
		if let Workable::Relate(l, r, v, _) = &self.extras {
			// This is a RELATE statement
			if let Some(data) = stm.data() {
				// Check that the 'id' field matches
				if let Some(field) = data.pick(stk, ctx, opt, &*ID).await? {
					match field {
						// You cannot store a range id as the id field on a document
						Value::Thing(v) if v.is_range() => {
							return Err(Error::IdInvalid {
								value: v.to_string(),
							})
						}
						// The id field is a match, so don't error
						Value::Thing(v) if v.eq(&rid) => (),
						// The id is a match, so don't error
						v if rid.id.is(&v) => (),
						// The id field does not match
						v => {
							return Err(Error::IdMismatch {
								value: v.to_string(),
							})
						}
					}
				}
				// Check that the 'in' field matches
				if let Some(field) = data.pick(stk, ctx, opt, &*IN).await? {
					match field {
						// You cannot store a range id as the in field on a document
						Value::Thing(v) if v.is_range() => {
							return Err(Error::InInvalid {
								value: v.to_string(),
							})
						}
						// The in field is a match, so don't error
						Value::Thing(v) if v.eq(l) => (),
						// The in is a match, so don't error
						v if l.id.is(&v) => (),
						// The in field does not match
						v => {
							return Err(Error::InMismatch {
								value: v.to_string(),
							})
						}
					}
				}
				// Check that the 'out' field matches
				if let Some(field) = data.pick(stk, ctx, opt, &*OUT).await? {
					match field {
						// You cannot store a range id as the out field on a document
						Value::Thing(v) if v.is_range() => {
							return Err(Error::OutInvalid {
								value: v.to_string(),
							})
						}
						// The out field is a match, so don't error
						Value::Thing(v) if v.eq(r) => (),
						// The out is a match, so don't error
						v if r.id.is(&v) => (),
						// The in field does not match
						v => {
							return Err(Error::OutMismatch {
								value: v.to_string(),
							})
						}
					}
				}
			}
			// This is a INSERT RELATION statement
			if let Some(data) = v {
				// Check that the 'id' field matches
				match data.pick(&*ID).compute(stk, ctx, opt, Some(&self.current)).await? {
					// You cannot store a range id as the id field on a document
					Value::Thing(v) if v.is_range() => {
						return Err(Error::IdInvalid {
							value: v.to_string(),
						})
					}
					// The id field is a match, so don't error
					Value::Thing(v) if v.eq(&rid) => (),
					// The id is a match, so don't error
					v if rid.id.is(&v) => (),
					// The id field does not match
					v => {
						return Err(Error::IdMismatch {
							value: v.to_string(),
						})
					}
				}
				// Check that the 'in' field matches
				match data.pick(&*IN).compute(stk, ctx, opt, Some(&self.current)).await? {
					// You cannot store a range id as the in field on a document
					Value::Thing(v) if v.is_range() => {
						return Err(Error::InInvalid {
							value: v.to_string(),
						})
					}
					// The in field is a match, so don't error
					Value::Thing(v) if v.eq(l) => (),
					// The in is a match, so don't error
					v if l.id.is(&v) => (),
					// The in field does not match
					v => {
						return Err(Error::InMismatch {
							value: v.to_string(),
						})
					}
				}
				// Check that the 'out' field matches
				match data.pick(&*OUT).compute(stk, ctx, opt, Some(&self.current)).await? {
					// You cannot store a range id as the out field on a document
					Value::Thing(v) if v.is_range() => {
						return Err(Error::OutInvalid {
							value: v.to_string(),
						})
					}
					// The out field is a match, so don't error
					Value::Thing(v) if v.eq(r) => (),
					// The out is a match, so don't error
					v if r.id.is(&v) => (),
					// The out field does not match
					v => {
						return Err(Error::OutMismatch {
							value: v.to_string(),
						})
					}
				}
			}
		}
		// Carry on
		Ok(())
	}
	/// Checks that the `WHERE` condition on a query
	/// matches before proceeding with processing
	/// the document. This ensures that records from
	/// a table, or from an index can be filtered out
	/// before being included within the query output.
	pub async fn check_where_condition(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check where condition
		if let Some(cond) = stm.conds() {
			// Process the current permitted
			self.process_permitted_current(stk, ctx, opt).await?;
			// Check if the expression is truthy
			if !cond.compute(stk, ctx, opt, Some(&self.current_permitted)).await?.is_truthy() {
				// Ignore this document
				return Err(Error::Ignore);
			}
		}
		// Carry on
		Ok(())
	}
	/// Checks the `PERMISSIONS` clause on the table
	/// for this record, returning immediately if the
	/// permissions are `NONE`. This function does not
	/// check any custom advanced table permissions,
	/// which should be checked at a later stage.
	pub async fn check_permissions_quick(
		&self,
		_stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check if this record exists
		if self.id.is_some() {
			// Should we run permissions checks?
			if opt.check_perms(stm.into())? {
				// Get the table for this document
				let table = self.tb(ctx, opt).await?;
				// Get the permissions for this table
				let perms = if stm.is_delete() {
					&table.permissions.delete
				} else if stm.is_select() {
					&table.permissions.select
				} else if self.is_new() {
					&table.permissions.create
				} else {
					&table.permissions.update
				};
				// Exit early if permissions are NONE
				if perms.is_none() {
					return Err(Error::Ignore);
				}
			}
		}
		// Carry on
		Ok(())
	}
	/// Checks the `PERMISSIONS` clause on the table for
	/// this record, processing all advanced permissions
	/// clauses and evaluating the expression. This
	/// function checks and evaluates `FULL`, `NONE`,
	/// and specific permissions clauses on the table.
	pub async fn check_permissions_table(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check if this record exists
		if self.id.is_some() {
			// Should we run permissions checks?
			if opt.check_perms(stm.into())? {
				// Check that record authentication matches session
				if opt.auth.is_record() {
					let ns = opt.ns()?;
					if opt.auth.level().ns() != Some(ns) {
						return Err(Error::NsNotAllowed {
							ns: ns.into(),
						});
					}
					let db = opt.db()?;
					if opt.auth.level().db() != Some(db) {
						return Err(Error::DbNotAllowed {
							db: db.into(),
						});
					}
				}
				// Get the table
				let table = self.tb(ctx, opt).await?;
				// Get the permission clause
				let perms = if stm.is_delete() {
					&table.permissions.delete
				} else if stm.is_select() {
					&table.permissions.select
				} else if self.is_new() {
					&table.permissions.create
				} else {
					&table.permissions.update
				};
				// Process the table permissions
				match perms {
					Permission::None => return Err(Error::Ignore),
					Permission::Full => return Ok(()),
					Permission::Specific(e) => {
						// Disable permissions
						let opt = &opt.new_with_perms(false);
						// Process the PERMISSION clause
						if !e
							.compute(
								stk,
								ctx,
								opt,
								Some(match stm.is_delete() {
									true => &self.initial,
									false => &self.current,
								}),
							)
							.await?
							.is_truthy()
						{
							return Err(Error::Ignore);
						}
					}
				}
			}
		}
		// Carry on
		Ok(())
	}
}
