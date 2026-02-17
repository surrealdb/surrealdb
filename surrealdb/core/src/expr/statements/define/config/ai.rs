use reblessive::tree::Stk;
use tracing::instrument;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::{Expr, FlowResultExt, Literal};
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct AiConfig {
	pub openai_api_key: Expr,
	pub openai_base_url: Expr,
	pub google_api_key: Expr,
	pub google_base_url: Expr,
	pub voyage_api_key: Expr,
	pub voyage_base_url: Expr,
	pub huggingface_api_key: Expr,
	pub huggingface_base_url: Expr,
}

impl Default for AiConfig {
	fn default() -> Self {
		Self {
			openai_api_key: Expr::Literal(Literal::None),
			openai_base_url: Expr::Literal(Literal::None),
			google_api_key: Expr::Literal(Literal::None),
			google_base_url: Expr::Literal(Literal::None),
			voyage_api_key: Expr::Literal(Literal::None),
			voyage_base_url: Expr::Literal(Literal::None),
			huggingface_api_key: Expr::Literal(Literal::None),
			huggingface_base_url: Expr::Literal(Literal::None),
		}
	}
}

async fn expr_to_optional_string(
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	expr: &Expr,
) -> anyhow::Result<Option<String>> {
	if matches!(expr, Expr::Literal(Literal::None)) {
		return Ok(None);
	}
	let v = stk.run(|stk| expr.compute(stk, ctx, opt, doc)).await.catch_return()?;
	Ok(match v {
		Value::String(s) => Some(s),
		_ => None,
	})
}

impl AiConfig {
	#[instrument(level = "trace", name = "AiConfig::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> anyhow::Result<crate::catalog::AiConfig> {
		Ok(crate::catalog::AiConfig {
			openai_api_key: expr_to_optional_string(stk, ctx, opt, doc, &self.openai_api_key)
				.await?,
			openai_base_url: expr_to_optional_string(stk, ctx, opt, doc, &self.openai_base_url)
				.await?,
			google_api_key: expr_to_optional_string(stk, ctx, opt, doc, &self.google_api_key)
				.await?,
			google_base_url: expr_to_optional_string(stk, ctx, opt, doc, &self.google_base_url)
				.await?,
			voyage_api_key: expr_to_optional_string(stk, ctx, opt, doc, &self.voyage_api_key)
				.await?,
			voyage_base_url: expr_to_optional_string(stk, ctx, opt, doc, &self.voyage_base_url)
				.await?,
			huggingface_api_key: expr_to_optional_string(
				stk,
				ctx,
				opt,
				doc,
				&self.huggingface_api_key,
			)
			.await?,
			huggingface_base_url: expr_to_optional_string(
				stk,
				ctx,
				opt,
				doc,
				&self.huggingface_base_url,
			)
			.await?,
		})
	}
}
