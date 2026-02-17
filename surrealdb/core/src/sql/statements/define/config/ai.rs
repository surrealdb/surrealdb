use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::sql::{Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

impl ToSql for AiConfig {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "AI ON DATABASE");
		if !matches!(&self.openai_api_key, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " OPENAI_API_KEY {}", self.openai_api_key);
		}
		if !matches!(&self.openai_base_url, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " OPENAI_BASE_URL {}", self.openai_base_url);
		}
		if !matches!(&self.google_api_key, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " GOOGLE_API_KEY {}", self.google_api_key);
		}
		if !matches!(&self.google_base_url, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " GOOGLE_BASE_URL {}", self.google_base_url);
		}
		if !matches!(&self.voyage_api_key, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " VOYAGE_API_KEY {}", self.voyage_api_key);
		}
		if !matches!(&self.voyage_base_url, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " VOYAGE_BASE_URL {}", self.voyage_base_url);
		}
		if !matches!(&self.huggingface_api_key, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " HUGGINGFACE_API_KEY {}", self.huggingface_api_key);
		}
		if !matches!(&self.huggingface_base_url, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " HUGGINGFACE_BASE_URL {}", self.huggingface_base_url);
		}
	}
}

impl From<crate::catalog::AiConfig> for AiConfig {
	fn from(v: crate::catalog::AiConfig) -> Self {
		Self {
			openai_api_key: v
				.openai_api_key
				.map(|s| Expr::Literal(Literal::String(s)))
				.unwrap_or(Expr::Literal(Literal::None)),
			openai_base_url: v
				.openai_base_url
				.map(|s| Expr::Literal(Literal::String(s)))
				.unwrap_or(Expr::Literal(Literal::None)),
			google_api_key: v
				.google_api_key
				.map(|s| Expr::Literal(Literal::String(s)))
				.unwrap_or(Expr::Literal(Literal::None)),
			google_base_url: v
				.google_base_url
				.map(|s| Expr::Literal(Literal::String(s)))
				.unwrap_or(Expr::Literal(Literal::None)),
			voyage_api_key: v
				.voyage_api_key
				.map(|s| Expr::Literal(Literal::String(s)))
				.unwrap_or(Expr::Literal(Literal::None)),
			voyage_base_url: v
				.voyage_base_url
				.map(|s| Expr::Literal(Literal::String(s)))
				.unwrap_or(Expr::Literal(Literal::None)),
			huggingface_api_key: v
				.huggingface_api_key
				.map(|s| Expr::Literal(Literal::String(s)))
				.unwrap_or(Expr::Literal(Literal::None)),
			huggingface_base_url: v
				.huggingface_base_url
				.map(|s| Expr::Literal(Literal::String(s)))
				.unwrap_or(Expr::Literal(Literal::None)),
		}
	}
}

impl From<AiConfig> for crate::expr::statements::define::config::ai::AiConfig {
	fn from(v: AiConfig) -> Self {
		Self {
			openai_api_key: v.openai_api_key.into(),
			openai_base_url: v.openai_base_url.into(),
			google_api_key: v.google_api_key.into(),
			google_base_url: v.google_base_url.into(),
			voyage_api_key: v.voyage_api_key.into(),
			voyage_base_url: v.voyage_base_url.into(),
			huggingface_api_key: v.huggingface_api_key.into(),
			huggingface_base_url: v.huggingface_base_url.into(),
		}
	}
}

impl From<crate::expr::statements::define::config::ai::AiConfig> for AiConfig {
	fn from(v: crate::expr::statements::define::config::ai::AiConfig) -> Self {
		Self {
			openai_api_key: v.openai_api_key.into(),
			openai_base_url: v.openai_base_url.into(),
			google_api_key: v.google_api_key.into(),
			google_base_url: v.google_base_url.into(),
			voyage_api_key: v.voyage_api_key.into(),
			voyage_base_url: v.voyage_base_url.into(),
			huggingface_api_key: v.huggingface_api_key.into(),
			huggingface_base_url: v.huggingface_base_url.into(),
		}
	}
}
