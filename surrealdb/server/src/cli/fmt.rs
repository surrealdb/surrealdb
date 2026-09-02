use core::fmt::Write;

use anyhow::{Result, bail};
use clap::Args;
use surrealdb_core::syn::lexer::Lexer;
use surrealdb_core::syn::parse_with;
use surrealdb_core::syn::parser::ParseResult;
use surrealdb_core::syn::token::{Token, TokenKind};
use surrealdb_types::ToSql;
use tokio::fs;
use tokio::io::{self, AsyncReadExt};

#[derive(Args, Debug)]
pub struct FormatCommandArguments {
	#[arg(help = "Path to the SurrealQL file to format. Use dash - to read from stdin.")]
	#[arg(index = 1)]
	file: String,

	#[arg(short, long)]
	#[arg(help = "Overwrite the file with the formatted output instead of printing to stdout.")]
	write: bool,
}

pub async fn init(
	FormatCommandArguments {
		file,
		write,
	}: FormatCommandArguments,
) -> Result<()> {
	let is_stdin = file == "-";

	if write && is_stdin {
		bail!("The --write flag cannot be used when reading from stdin.");
	}

	let content = if is_stdin {
		let mut content = String::new();

		io::stdin().read_to_string(&mut content).await?;

		content
	} else {
		fs::read_to_string(&file).await?
	};

	let formatted = parse_with(content.as_bytes(), async |parser, stk| {
		let mut output = String::new();
		let mut carry_token = Token::invalid();

		loop {
			let is_eof = parser.lex_compound(carry_token, |lexer, _| Ok(lexer.is_eof()))?;

			if is_eof.value {
				break;
			}

			let result = parser.lex_compound(carry_token, |lexer, _| parse_line(lexer))?;

			carry_token = Token {
				kind: TokenKind::Invalid,
				span: result.span.after(),
			};

			match result.value {
				ParsedLine::SingleComment(s) => _ = write!(&mut output, "#{s}"),
				ParsedLine::MultiComment(s) => output.push_str(&s),
				ParsedLine::Whitespace => output.push('\n'),
				ParsedLine::Statement => {
					let parsed = stk.run(|ctx| parser.parse_statement(ctx)).await?;
					parser.eat(TokenKind::SemiColon);

					carry_token = Token {
						kind: TokenKind::Invalid,
						span: parser.recent_span(),
					};

					output += &parsed.to_sql_pretty();
				}
			}

			let result = parser.lex_compound(carry_token, |lexer, _| {
				let cr = lexer.eat(b'\r');
				let lf = lexer.eat(b'\n');
				Ok(cr || lf)
			})?;

			carry_token = Token {
				kind: TokenKind::Invalid,
				span: result.span.after(),
			};

			if result.value {
				output += "\n";
			}
		}

		Ok(output)
	})?;

	if write {
		fs::write(file, formatted).await?
	} else {
		println!("{formatted}");
	}

	Ok(())
}

/// Indicated the state of the line, and how should it be processed
enum ParsedLine {
	/// Normalize to a single comment
	SingleComment(String),
	/// Normalize to a multi-line comment
	MultiComment(String),
	/// Consume whitespace-only lines leaving just one
	Whitespace,
	/// Parse and format a statement
	Statement,
}

fn parse_line(lexer: &mut Lexer) -> ParseResult<ParsedLine> {
	lexer.eat_single_line_whitespace();
	lexer.advance_span();

	let mut span = lexer.current_span();
	span.len = 1;

	let lookahead = lexer.span_bytes(span);

	match lookahead {
		b"#" => {
			lexer.eat(b'#');
			lexer.advance_span();
			Ok(parse_single_line_comment(lexer))
		}
		b"-" => {
			let mut span = span.after();
			span.len = 1;
			let lookahead = lexer.span_bytes(span);

			if lookahead == b"-" {
				lexer.eat(b'-');
				lexer.eat(b'-');
				lexer.advance_span();
				Ok(parse_single_line_comment(lexer))
			} else {
				Ok(ParsedLine::Statement)
			}
		}
		b"/" => {
			let mut span = span.after();
			span.len = 1;
			let lookahead = lexer.span_bytes(span);

			match lookahead {
				b"/" => {
					lexer.eat(b'/');
					lexer.eat(b'/');
					lexer.advance_span();
					Ok(parse_single_line_comment(lexer))
				}
				b"*" => {
					lexer.eat(b'/');
					lexer.eat(b'*');
					lexer.eat_multi_line_comment()?;
					let span = lexer.advance_span();
					let comment = lexer.span_str(span).to_string();
					Ok(ParsedLine::MultiComment(comment))
				}
				_ => Ok(ParsedLine::Statement),
			}
		}
		b"\n" | b"\r" => {
			lexer.eat_whitespace();
			lexer.advance_span();
			Ok(ParsedLine::Whitespace)
		}
		_ => Ok(ParsedLine::Statement),
	}
}

fn parse_single_line_comment(lexer: &mut Lexer) -> ParsedLine {
	lexer.eat_single_line_comment();
	let span = lexer.advance_span();
	let comment = lexer.span_str(span).to_string();
	ParsedLine::SingleComment(comment)
}
