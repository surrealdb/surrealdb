use token::{BaseTokenKind, T};

use crate::parse::ParseResult;
use crate::{Parse, Parser};

impl Parse for ast::RemoveNamespace {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let remove = parser.expect(T![REMOVE])?;
		let _ = parser.expect(T![NAMESPACE])?;

		let expunge = if parser.eat(T![AND])?.is_some() {
			let _ = parser.expect(T![EXPUNGE])?;
			true
		} else {
			false
		};

		let if_exists = if parser.eat(T![IF])?.is_some() {
			let _ = parser.expect(T![EXISTS])?;
			true
		} else {
			false
		};

		let name = parser.parse_enter().await?;

		let span = parser.span_since(remove.span);
		Ok(ast::RemoveNamespace {
			if_exists,
			expunge,
			name,
			span,
		})
	}
}

impl Parse for ast::RemoveDatabase {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let remove = parser.expect(T![REMOVE])?;
		let _ = parser.expect(T![DATABASE])?;

		let expunge = if parser.eat(T![AND])?.is_some() {
			let _ = parser.expect(T![EXPUNGE])?;
			true
		} else {
			false
		};

		let if_exists = if parser.eat(T![IF])?.is_some() {
			let _ = parser.expect(T![EXISTS])?;
			true
		} else {
			false
		};

		let name = parser.parse_enter().await?;

		let span = parser.span_since(remove.span);
		Ok(ast::RemoveDatabase {
			if_exists,
			expunge,
			name,
			span,
		})
	}
}

impl Parse for ast::RemoveFunction {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let remove = parser.expect(T![REMOVE])?;
		let _ = parser.expect(T![FUNCTION])?;

		let if_exists = if parser.eat(T![IF])?.is_some() {
			let _ = parser.expect(T![EXISTS])?;
			true
		} else {
			false
		};

		let name = parser.parse_sync()?;
		if let Some(x) = parser.eat(BaseTokenKind::OpenBrace)? {
			let _ = parser.expect_closing_delimiter(BaseTokenKind::CloseBrace, x.span)?;
		}

		let span = parser.span_since(remove.span);
		Ok(ast::RemoveFunction {
			if_exists,
			name,
			span,
		})
	}
}

impl Parse for ast::RemoveModule {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let remove = parser.expect(T![REMOVE])?;
		let _ = parser.expect(T![MODULE])?;

		let if_exists = if parser.eat(T![IF])?.is_some() {
			let _ = parser.expect(T![EXISTS])?;
			true
		} else {
			false
		};

		let name = parser.parse_sync()?;
		if let Some(x) = parser.eat(BaseTokenKind::OpenBrace)? {
			let _ = parser.expect_closing_delimiter(BaseTokenKind::CloseBrace, x.span)?;
		}

		let span = parser.span_since(remove.span);
		Ok(ast::RemoveModule {
			if_exists,
			name,
			span,
		})
	}
}

impl Parse for ast::RemoveAccess {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let remove = parser.expect(T![REMOVE])?;
		let _ = parser.expect(T![ACCESS])?;

		let if_exists = if parser.eat(T![IF])?.is_some() {
			let _ = parser.expect(T![EXISTS])?;
			true
		} else {
			false
		};

		let name = parser.parse_enter().await?;
		let _ = parser.expect(T![ON])?;
		let base = parser.parse_sync()?;

		let span = parser.span_since(remove.span);
		Ok(ast::RemoveAccess {
			if_exists,
			name,
			base,
			span,
		})
	}
}

impl Parse for ast::RemoveParam {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let remove = parser.expect(T![REMOVE])?;
		let _ = parser.expect(T![PARAM])?;

		let if_exists = if parser.eat(T![IF])?.is_some() {
			let _ = parser.expect(T![EXISTS])?;
			true
		} else {
			false
		};

		let name = parser.parse_sync()?;

		let span = parser.span_since(remove.span);
		Ok(ast::RemoveParam {
			if_exists,
			name,
			span,
		})
	}
}

impl Parse for ast::RemoveTable {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let remove = parser.expect(T![REMOVE])?;
		let _ = parser.expect(T![TABLE])?;

		let expunge = if parser.eat(T![AND])?.is_some() {
			let _ = parser.expect(T![EXPUNGE])?;
			true
		} else {
			false
		};

		let if_exists = if parser.eat(T![IF])?.is_some() {
			let _ = parser.expect(T![EXISTS])?;
			true
		} else {
			false
		};

		let name = parser.parse_enter().await?;

		let span = parser.span_since(remove.span);
		Ok(ast::RemoveTable {
			if_exists,
			expunge,
			name,
			span,
		})
	}
}

impl Parse for ast::RemoveEvent {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let remove = parser.expect(T![REMOVE])?;
		let _ = parser.expect(T![EVENT])?;

		let if_exists = if parser.eat(T![IF])?.is_some() {
			let _ = parser.expect(T![EXISTS])?;
			true
		} else {
			false
		};

		let name = parser.parse_enter().await?;
		let _ = parser.expect(T![ON])?;
		let _ = parser.eat(T![TABLE])?;
		let table = parser.parse_enter().await?;

		let span = parser.span_since(remove.span);
		Ok(ast::RemoveEvent {
			if_exists,
			name,
			table,
			span,
		})
	}
}

impl Parse for ast::RemoveField {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let remove = parser.expect(T![REMOVE])?;
		let _ = parser.expect(T![FIELD])?;

		let if_exists = if parser.eat(T![IF])?.is_some() {
			let _ = parser.expect(T![EXISTS])?;
			true
		} else {
			false
		};

		let name = parser.parse_enter().await?;
		let _ = parser.expect(T![ON])?;
		let _ = parser.eat(T![TABLE])?;
		let table = parser.parse_enter().await?;

		let span = parser.span_since(remove.span);
		Ok(ast::RemoveField {
			if_exists,
			name,
			table,
			span,
		})
	}
}

impl Parse for ast::RemoveIndex {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let remove = parser.expect(T![REMOVE])?;
		let _ = parser.expect(T![INDEX])?;

		let if_exists = if parser.eat(T![IF])?.is_some() {
			let _ = parser.expect(T![EXISTS])?;
			true
		} else {
			false
		};

		let name = parser.parse_enter().await?;
		let _ = parser.expect(T![ON])?;
		let _ = parser.eat(T![TABLE])?;
		let table = parser.parse_enter().await?;

		let span = parser.span_since(remove.span);
		Ok(ast::RemoveIndex {
			if_exists,
			name,
			table,
			span,
		})
	}
}

impl Parse for ast::RemoveAnalyzer {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let remove = parser.expect(T![REMOVE])?;
		let _ = parser.expect(T![ANALYZER])?;

		let if_exists = if parser.eat(T![IF])?.is_some() {
			let _ = parser.expect(T![EXISTS])?;
			true
		} else {
			false
		};

		let name = parser.parse_enter().await?;

		let span = parser.span_since(remove.span);
		Ok(ast::RemoveAnalyzer {
			if_exists,
			name,
			span,
		})
	}
}

impl Parse for ast::RemoveSequence {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let remove = parser.expect(T![REMOVE])?;
		let _ = parser.expect(T![SEQUENCE])?;

		let if_exists = if parser.eat(T![IF])?.is_some() {
			let _ = parser.expect(T![EXISTS])?;
			true
		} else {
			false
		};

		let name = parser.parse_enter().await?;

		let span = parser.span_since(remove.span);
		Ok(ast::RemoveSequence {
			if_exists,
			name,
			span,
		})
	}
}

impl Parse for ast::RemoveUser {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let remove = parser.expect(T![REMOVE])?;
		let _ = parser.expect(T![USER])?;

		let if_exists = if parser.eat(T![IF])?.is_some() {
			let _ = parser.expect(T![EXISTS])?;
			true
		} else {
			false
		};

		let name = parser.parse_enter().await?;
		let _ = parser.expect(T![ON])?;
		let base = parser.parse_sync()?;

		let span = parser.span_since(remove.span);
		Ok(ast::RemoveUser {
			if_exists,
			name,
			base,
			span,
		})
	}
}

impl Parse for ast::RemoveApi {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let remove = parser.expect(T![REMOVE])?;
		let _ = parser.expect(T![API])?;

		let if_exists = if parser.eat(T![IF])?.is_some() {
			let _ = parser.expect(T![EXISTS])?;
			true
		} else {
			false
		};

		let name = parser.parse_enter().await?;

		let span = parser.span_since(remove.span);
		Ok(ast::RemoveApi {
			if_exists,
			name,
			span,
		})
	}
}

impl Parse for ast::RemoveBucket {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let remove = parser.expect(T![REMOVE])?;
		let _ = parser.expect(T![BUCKET])?;

		let if_exists = if parser.eat(T![IF])?.is_some() {
			let _ = parser.expect(T![EXISTS])?;
			true
		} else {
			false
		};

		let name = parser.parse_enter().await?;

		let span = parser.span_since(remove.span);
		Ok(ast::RemoveBucket {
			if_exists,
			name,
			span,
		})
	}
}
