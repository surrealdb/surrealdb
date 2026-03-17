use sqlparser::parser::ParserError;

#[derive(Debug, thiserror::Error)]
pub enum TranslateError {
	#[error("Failed to parse SQL: {source}")]
	Parse {
		#[from]
		source: ParserError,
	},

	#[error("Unsupported SQL feature: {feature}")]
	Unsupported {
		feature: String,
		hint: Option<String>,
	},

	#[error("Failed to map expression: {message}")]
	Mapping {
		message: String,
	},
}

impl TranslateError {
	pub fn unsupported(feature: impl Into<String>) -> Self {
		Self::Unsupported {
			feature: feature.into(),
			hint: None,
		}
	}

	pub fn unsupported_with_hint(feature: impl Into<String>, hint: impl Into<String>) -> Self {
		Self::Unsupported {
			feature: feature.into(),
			hint: Some(hint.into()),
		}
	}

	pub fn mapping(message: impl Into<String>) -> Self {
		Self::Mapping {
			message: message.into(),
		}
	}
}
