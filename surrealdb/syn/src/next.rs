//! The switch that routes this crate's entry points through parser-next.
//!
//! With the switch off (the default) every entry point parses with this
//! crate's parser. With it on, the entry points that parser-next covers parse
//! with parser-next and lower its tree into the same `surrealdb_sql` AST.
//! The switch is process-wide: the entry points are free functions that
//! callers reach without a datastore, so there is no narrower scope to hold
//! it.
//!
//! Modes, from `SURREAL_PARSER_NEXT` on first use or [`set_mode`]:
//! - [`Mode::Off`]: this crate's parser only.
//! - [`Mode::On`]: parser-next alone: its tree, or its error rendered as this crate renders errors.
//! - [`Mode::Check`]: both parsers on every input; any difference in outcome, tree or rendered
//!   error text is returned as an error. For test lanes, at twice the parse cost.

use std::sync::atomic::{AtomicU8, Ordering};

use surrealdb_parser_next::Limits;
use surrealdb_parser_next_lower::Options;
use surrealdb_parser_next_lower::entry::{self, Settings};
use surrealdb_types::ToSql;

use crate::error::{MessageKind, SyntaxError};
use crate::token::Span;
use crate::{ParseError, ParserSettings};

/// Which parser the entry points use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
	Off,
	On,
	Check,
}

const UNSET: u8 = 0;
const OFF: u8 = 1;
const ON: u8 = 2;
const CHECK: u8 = 3;

static MODE: AtomicU8 = AtomicU8::new(UNSET);

/// The mode in force: the last [`set_mode`], or else `SURREAL_PARSER_NEXT`
/// (`on`, `true` or `1` for [`Mode::On`], `check` for [`Mode::Check`], anything
/// else or unset for [`Mode::Off`]).
pub fn mode() -> Mode {
	let m = match MODE.load(Ordering::Relaxed) {
		UNSET => {
			let m = match std::env::var("SURREAL_PARSER_NEXT") {
				Ok(v) => match v.trim().to_ascii_lowercase().as_str() {
					"on" | "true" | "1" => ON,
					"check" => CHECK,
					_ => OFF,
				},
				Err(_) => OFF,
			};
			// A concurrent `set_mode` wins over the environment.
			match MODE.compare_exchange(UNSET, m, Ordering::Relaxed, Ordering::Relaxed) {
				Ok(_) => m,
				Err(set) => set,
			}
		}
		m => m,
	};
	match m {
		ON => Mode::On,
		CHECK => Mode::Check,
		_ => Mode::Off,
	}
}

/// Sets the mode for the whole process.
pub fn set_mode(mode: Mode) {
	let m = match mode {
		Mode::Off => OFF,
		Mode::On => ON,
		Mode::Check => CHECK,
	};
	MODE.store(m, Ordering::Relaxed);
}

/// Whether entry points route through parser-next.
#[inline]
pub(crate) fn routed() -> bool {
	mode() != Mode::Off
}

/// parser-next's settings for this crate's.
pub(crate) fn settings(s: &ParserSettings) -> Settings {
	let c = |v: usize| v.min(u16::MAX as usize) as u16;
	Settings {
		limits: Limits {
			expr_depth: c(s.expr_recursion_limit),
			query_depth: c(s.query_recursion_limit),
			object_depth: c(s.object_recursion_limit),
			strict_record_ids: !s.flexible_record_id,
			builtin: Some(surrealdb_parser_next_lower::is_builtin),
			regex: Some(surrealdb_parser_next_lower::is_regex),
			no_files: !s.files_enabled,
			no_surrealism: !s.surrealism_enabled,
		},
		opts: Options {
			legacy_strands: s.legacy_strands,
			files: s.files_enabled,
			surrealism: s.surrealism_enabled,
		},
	}
}

/// An error for a divergence between the parsers, located at the start of
/// the input.
fn divergence(input: &str, what: &str) -> ParseError {
	let e = SyntaxError::new(format!("parser-next diverges from surrealdb-syn: {what}")).with_span(
		Span {
			offset: 0,
			len: 0,
		},
		MessageKind::Error,
	);
	ParseError::InvalidQuery(e.render_on(input))
}

/// Parses `input` with parser-next (`new`) or this crate's parser (`old`) as
/// the mode says. Only called when [`routed`].
pub(crate) fn route<T: PartialEq + ToSql>(
	input: &str,
	new: impl FnOnce() -> Result<T, entry::Error>,
	old: impl FnOnce() -> Result<T, ParseError>,
) -> Result<T, ParseError> {
	if input.len() > u32::MAX as usize {
		return Err(ParseError::QueryTooLarge);
	}
	match mode() {
		Mode::Off => old(),
		Mode::On => new().map_err(|e| ParseError::InvalidQuery(render(input, &e))),
		Mode::Check => match (new(), old()) {
			(Ok(n), Ok(o)) => {
				if n == o || mask_keys(&n.to_sql()) == mask_keys(&o.to_sql()) {
					Ok(n)
				} else {
					Err(divergence(
						input,
						&format!(
							"trees differ: syn `{}`, parser-next `{}`",
							o.to_sql(),
							n.to_sql()
						),
					))
				}
			}
			(Err(e), Err(err)) => {
				let ours = render(input, &e).to_string();
				let theirs = match &err {
					ParseError::InvalidQuery(r) => r.to_string(),
					other => other.to_string(),
				};
				if ours == theirs {
					Err(err)
				} else {
					Err(divergence(
						input,
						&format!("error text differs:\nsyn:\n{theirs}\nparser-next:\n{ours}"),
					))
				}
			}
			(Ok(n), Err(_)) => Err(divergence(
				input,
				&format!("parser-next accepts `{}`, syn rejects", n.to_sql()),
			)),
			(Err(e), Ok(_)) => {
				Err(divergence(input, &format!("parser-next rejects ({e:?}), syn accepts")))
			}
		},
	}
}

/// Renders a parser-next error against `input` as this crate renders its
/// own, so the two can be compared and, once they agree, interchanged.
pub fn render(input: &str, e: &entry::Error) -> crate::RenderedError {
	use surrealdb_parser_next_lower::render;
	let span = |start: u32, end: u32| Span {
		offset: start,
		len: end.saturating_sub(start),
	};
	let err = match e {
		entry::Error::Syntax(d) => {
			let d = *d;
			let mut err = SyntaxError::new(render::message(input, &d));
			err = if d.label.is_empty() {
				err.with_span(span(d.start, d.end), MessageKind::Error)
			} else {
				err.with_labeled_span(
					span(d.start, d.end),
					MessageKind::Error,
					render::fill(d.label, input, d.start, d.end),
				)
			};
			if let Some(x) = d.extra {
				err = err.with_labeled_span(
					span(x.start, x.end),
					MessageKind::Error,
					render::fill(x.label, input, x.start, x.end),
				);
			}
			err
		}
		entry::Error::Lower(r) => {
			let mut err = SyntaxError::new(&r.msg);
			err = match (r.span, &r.label) {
				(None, _) => err,
				(Some((start, end)), None) => err.with_span(span(start, end), MessageKind::Error),
				(Some((start, end)), Some(label)) => {
					err.with_labeled_span(span(start, end), MessageKind::Error, label)
				}
			};
			if let Some((start, end, label)) = &r.extra {
				err = err.with_labeled_span(span(*start, *end), MessageKind::Error, label);
			}
			err
		}
	};
	err.render_on(input)
}

/// Replaces the random 128-character keys both parsers mint for access
/// methods defined without one, so that two parses compare equal.
pub(crate) fn mask_keys(sql: &str) -> String {
	let mut out = String::with_capacity(sql.len());
	let mut rest = sql;
	while let Some(i) = rest.find('\'') {
		let after = &rest[i + 1..];
		let key = after.len() > 128
			&& after.as_bytes()[128] == b'\''
			&& after.as_bytes()[..128].iter().all(|b| b.is_ascii_alphanumeric());
		if key {
			out.push_str(&rest[..i]);
			out.push_str("'random'");
			rest = &after[129..];
		} else {
			out.push_str(&rest[..i + 1]);
			rest = after;
		}
	}
	out.push_str(rest);
	out
}
