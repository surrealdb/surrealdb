use reblessive::Stk;

use crate::{
	sql::{statements::{impersonate::ImpersonationTarget, ImpersonateStatement}, Ident},
	syn::{
		parser::{
			mac::expected,
			ParseResult, Parser,
		},
		token::t,
	},
};

impl Parser<'_> {
	pub(crate) async fn parse_impersonate_stmt(&mut self, ctx: &mut Stk) -> ParseResult<ImpersonateStatement> {
		expected!(self, t!("RECORD"));
		let thing = self.parse_value_inherit(ctx).await?;
		expected!(self, t!("VIA"));
		self.eat(t!("ACCESS"));
		let access: Ident = self.next_token_value()?;
		let target = ImpersonationTarget::Record(thing, access.0);
		
		expected!(self, t!("{"));
		let start = self.last_span();
		let then = self.parse_block(ctx, start).await?;

		Ok(ImpersonateStatement {
			target,
			then
		})
	}
}
