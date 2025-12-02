use std::io::Result;

use ast::Expr;

use crate::parse::{Parse, ParseResult};

use super::Parser;

pub enum BindingPower {
	Base,
}

impl Parse for ast::Expr {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		parse_pratt(parser, BindingPower::Base).await
	}
}

async fn parse_pratt(parser: &mut Parser<'_, '_>, bp: BindingPower) -> ParseResult<Expr> {
	todo!()
}
