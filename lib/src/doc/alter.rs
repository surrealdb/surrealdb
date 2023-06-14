use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Workable;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::data::Data;
use crate::sql::operator::Operator;
use crate::sql::value::Value;

impl<'a> Document<'a> {
	pub async fn alter(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Get the record id
		let rid = self.id.as_ref().unwrap();
		// Set default field values
		self.current.to_mut().def(rid);
		// The statement has a data clause
		if let Some(v) = stm.data() {
			match v {
				Data::PatchExpression(data) => {
					let mut current_ctx = Context::new(ctx);
					current_ctx.add_cursor_doc(&self.current);
					let data = data.compute(&current_ctx, opt).await?;
					self.current.to_mut().patch(data)?
				}
				Data::MergeExpression(data) => {
					let mut current_ctx = Context::new(ctx);
					current_ctx.add_cursor_doc(&self.current);
					let data = data.compute(&current_ctx, opt).await?;
					self.current.to_mut().merge(data)?
				}
				Data::ReplaceExpression(data) => {
					let mut current_ctx = Context::new(ctx);
					current_ctx.add_cursor_doc(&self.current);
					let data = data.compute(&current_ctx, opt).await?;
					self.current.to_mut().replace(data)?
				}
				Data::ContentExpression(data) => {
					let mut current_ctx = Context::new(ctx);
					current_ctx.add_cursor_doc(&self.current);
					let data = data.compute(&current_ctx, opt).await?;
					self.current.to_mut().replace(data)?
				}
				Data::SetExpression(x) => {
					for x in x.iter() {
						let mut current_ctx = Context::new(ctx);
						current_ctx.add_cursor_doc(&self.current);
						let v = x.2.compute(&current_ctx, opt).await?;
						match x.1 {
							Operator::Equal => match v {
								Value::None => self.current.to_mut().del(ctx, opt, &x.0).await?,
								_ => self.current.to_mut().set(ctx, opt, &x.0, v).await?,
							},
							Operator::Inc => {
								self.current.to_mut().increment(ctx, opt, &x.0, v).await?
							}
							Operator::Dec => {
								self.current.to_mut().decrement(ctx, opt, &x.0, v).await?
							}
							Operator::Ext => {
								self.current.to_mut().extend(ctx, opt, &x.0, v).await?
							}
							_ => unreachable!(),
						}
					}
				}
				Data::UnsetExpression(i) => {
					for i in i.iter() {
						self.current.to_mut().del(ctx, opt, &i).await?
					}
				}
				Data::UpdateExpression(x) => {
					// Duplicate context
					let mut ctx = Context::new(ctx);
					// Add insertable value
					if let Workable::Insert(value) = &self.extras {
						ctx.add_value("input", value);
					}
					// Process ON DUPLICATE KEY clause
					for x in x.iter() {
						let mut current_ctx = Context::new(&ctx);
						current_ctx.add_cursor_doc(&self.current);
						let v = x.2.compute(&current_ctx, opt).await?;
						match x.1 {
							Operator::Equal => match v {
								Value::None => self.current.to_mut().del(&ctx, opt, &x.0).await?,
								_ => self.current.to_mut().set(&ctx, opt, &x.0, v).await?,
							},
							Operator::Inc => {
								self.current.to_mut().increment(&ctx, opt, &x.0, v).await?
							}
							Operator::Dec => {
								self.current.to_mut().decrement(&ctx, opt, &x.0, v).await?
							}
							Operator::Ext => {
								self.current.to_mut().extend(&ctx, opt, &x.0, v).await?
							}
							_ => unreachable!(),
						}
					}
				}
				_ => unreachable!(),
			};
		};
		// Set default field values
		self.current.to_mut().def(rid);
		// Carry on
		Ok(())
	}
}
