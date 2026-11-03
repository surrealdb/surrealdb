//! The `BINDINGS` clause: an expression for each name the archive declares
//! under `[bindings]` in its `package.toml`.
//!
//! ```toml
//! [bindings]
//! api_key = { type = "string", description = "Mailgun API key", sensitive = true }
//! author = "option<string>"
//! ```
//!
//! ```text
//! surreal ext install silo::acme::mailer --as mailer --bind 'api_key=secret::get("mailgun")'
//! ```
//!
//! Each name takes its expression from `--bind`, else on upgrade from the
//! installed definition, else from the person at a terminal. A name whose kind
//! is `option<…>` may be left out; any other name must be given. Each answer is
//! a SurrealQL expression, parsed here so a typo fails before anything is
//! written. The server evaluates it when the extension reads the name, with
//! permissions off, so `INFO` and an export show the expression and never its
//! value: a `sensitive` name given as a literal is written into the definition
//! as it stands, and the installer warns about it.

use std::collections::BTreeMap;

use anyhow::{Context as _, Result, bail};
use surrealism_runtime::config::BindingSpec;

use super::prompt::clean;

/// How each binding's expression is decided.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BindingAnswers {
	/// `--bind NAME=EXPR`, in the order given.
	pub given: Vec<(String, String)>,
	/// Whether a name with no other answer is asked for at a terminal.
	pub ask: bool,
}

/// `NAME=EXPR` from `--bind`, with the expression checked to parse.
pub fn parse_flag(flag: &str) -> Result<(String, String)> {
	let Some((name, expr)) = flag.split_once('=') else {
		bail!("--bind takes NAME=EXPRESSION, as in --bind 'api_key=secret::get(\"mailgun\")'");
	};
	let name = name.trim();
	if name.is_empty() {
		bail!("--bind {flag}: the name is empty");
	}
	Ok((name.to_owned(), check_expression(name, expr)?))
}

/// `expr`, trimmed, once it parses as a SurrealQL expression.
fn check_expression(name: &str, expr: &str) -> Result<String> {
	let expr = expr.trim();
	surrealdb_expr::parser::expr(expr)
		.map_err(|e| anyhow::anyhow!("{e:?}"))
		.with_context(|| format!("the expression for `{name}` is not valid SurrealQL"))?;
	Ok(expr.to_owned())
}

/// Whether `kind` admits `NONE`, so the name may be left out.
fn optional(kind: &str) -> bool {
	surrealdb_expr::parser::kind(kind.trim()).is_ok_and(|k| k.can_be_none())
}

/// The `BINDINGS` entries for `declared`, in name order: each from `--bind`,
/// else from `installed`, else asked. A `--bind` for a name `declared` does not
/// hold is refused, and so is a required name nothing answers.
pub fn resolve(
	declared: &BTreeMap<String, BindingSpec>,
	answers: &BindingAnswers,
	installed: &[(String, String)],
) -> Result<Vec<(String, String)>> {
	if let Some((name, _)) = answers.given.iter().find(|(n, _)| !declared.contains_key(n)) {
		bail!("--bind {name}: the extension's package.toml declares no binding `{name}`");
	}
	let mut out = Vec::new();
	for (name, spec) in declared {
		let found = answers
			.given
			.iter()
			.rev()
			.find(|(n, _)| n == name)
			.or_else(|| installed.iter().find(|(n, _)| n == name))
			.map(|(_, e)| e.clone());
		let expr = match found {
			Some(expr) => Some(expr),
			None if answers.ask => ask(name, spec)?,
			None => None,
		};
		match expr {
			Some(expr) => {
				warn_if_literal(name, spec, &expr);
				out.push((name.clone(), expr));
			}
			None if optional(spec.kind()) => {}
			None => {
				let name = clean(name);
				bail!(
					"the extension requires the binding `{name}` ({}): pass --bind '{name}=<expression>'",
					clean(spec.kind())
				)
			}
		}
	}
	Ok(out)
}

/// Ask for `name` at a terminal. Empty leaves an optional name out. The name,
/// kind and description come from the archive, so each is printed through
/// [`clean`].
fn ask(name: &str, spec: &BindingSpec) -> Result<Option<String>> {
	let optional = optional(spec.kind());
	let shown = clean(name);
	if let Some(description) = spec.description() {
		println!("{shown}: {}", clean(description));
	}
	loop {
		let answer: String = dialoguer::Input::new()
			.with_prompt(format!(
				"SurrealQL expression for `{shown}` ({}{})",
				clean(spec.kind()),
				if optional {
					", empty to leave out"
				} else {
					""
				}
			))
			.allow_empty(optional)
			.interact_text()
			.context("failed to read the answer")?;
		if answer.trim().is_empty() {
			if optional {
				return Ok(None);
			}
			eprintln!("`{shown}` is required: give an expression");
			continue;
		}
		match check_expression(&shown, &answer) {
			Ok(expr) => return Ok(Some(expr)),
			Err(e) => eprintln!("{e:#}"),
		}
	}
}

/// A `sensitive` name written as a literal is stored in the definition's text.
fn warn_if_literal(name: &str, spec: &BindingSpec, expr: &str) {
	if spec.sensitive() && surrealdb_expr::parser::value(expr).is_ok() {
		let name = clean(name);
		eprintln!(
			"⚠ `{name}` is sensitive, and its value is written into the definition as a literal, \
			 where INFO and an export show it. Bind an expression that reads it instead, such as \
			 secret::get(\"…\")."
		);
	}
}

/// `{ name: expr, … }`.
pub fn to_sql(bindings: &[(String, String)]) -> String {
	let entries: Vec<String> = bindings
		.iter()
		.map(|(name, expr)| {
			format!(
				"{}: {expr}",
				surrealdb_types::ToSql::to_sql(&common::fmt::EscapeObjectKey(name))
			)
		})
		.collect();
	format!("{{ {} }}", entries.join(", "))
}

#[cfg(test)]
mod tests {
	use surrealism_runtime::config::DescribedBinding;

	use super::*;

	fn declared() -> BTreeMap<String, BindingSpec> {
		BTreeMap::from([
			(
				"api_key".to_owned(),
				BindingSpec::Described(DescribedBinding {
					kind: "string".into(),
					description: Some("Mailgun API key".into()),
					sensitive: true,
				}),
			),
			("author".to_owned(), BindingSpec::Kind("option<string>".into())),
		])
	}

	fn given(pairs: &[(&str, &str)]) -> BindingAnswers {
		BindingAnswers {
			given: pairs.iter().map(|(n, e)| (n.to_string(), e.to_string())).collect(),
			ask: false,
		}
	}

	/// A name or kind from the archive reaches the terminal escaped, so it
	/// cannot rewrite the screen or reorder the text around it.
	#[test]
	fn archive_text_in_an_error_is_escaped() {
		let declared = BTreeMap::from([(
			"key\u{1b}[2J".to_owned(),
			BindingSpec::Kind("string\u{202e}".into()),
		)]);
		let err = resolve(&declared, &given(&[]), &[]).unwrap_err().to_string();
		assert!(!err.contains('\u{1b}') && !err.contains('\u{202e}'), "{err:?}");
		assert!(err.contains("key\\u{001b}[2J"), "{err}");
		assert!(err.contains("string\\u{202e}"), "{err}");
	}

	#[test]
	fn flags_parse_name_and_expression() {
		assert_eq!(
			parse_flag("api_key = (SELECT VALUE key FROM ONLY vault:mailgun)").unwrap(),
			("api_key".to_owned(), "(SELECT VALUE key FROM ONLY vault:mailgun)".to_owned())
		);
		assert!(parse_flag("api_key").is_err());
		assert!(parse_flag("=1").is_err());
		assert!(parse_flag("api_key=(SELECT").is_err());
	}

	#[test]
	fn a_required_name_must_be_answered() {
		let err = resolve(&declared(), &given(&[]), &[]).unwrap_err().to_string();
		assert!(err.contains("requires the binding `api_key`"), "{err}");
	}

	#[test]
	fn an_optional_name_may_be_left_out() {
		let out = resolve(&declared(), &given(&[("api_key", "$k")]), &[]).unwrap();
		assert_eq!(out, [("api_key".to_owned(), "$k".to_owned())]);
	}

	#[test]
	fn an_undeclared_flag_is_refused() {
		let err = resolve(&declared(), &given(&[("api_key", "1"), ("nope", "2")]), &[])
			.unwrap_err()
			.to_string();
		assert!(err.contains("declares no binding `nope`"), "{err}");
	}

	/// An upgrade keeps an installed expression unless --bind replaces it, and
	/// drops one the new package no longer declares.
	#[test]
	fn an_upgrade_carries_installed_expressions() {
		let installed = [
			("api_key".to_owned(), "secret::get('old')".to_owned()),
			("author".to_owned(), "'me'".to_owned()),
			("gone".to_owned(), "1".to_owned()),
		];
		let out =
			resolve(&declared(), &given(&[("api_key", "secret::get('new')")]), &installed).unwrap();
		assert_eq!(
			out,
			[
				("api_key".to_owned(), "secret::get('new')".to_owned()),
				("author".to_owned(), "'me'".to_owned()),
			]
		);
	}

	#[test]
	fn the_clause_renders_and_parses_back() {
		let sql = to_sql(&[
			("api_key".to_owned(), "string::uppercase('mailgun')".to_owned()),
			("odd-name".to_owned(), "1".to_owned()),
		]);
		assert_eq!(sql, "{ api_key: string::uppercase('mailgun'), \"odd-name\": 1 }");
		surrealdb_expr::parser::expr(&sql).unwrap();
	}
}
