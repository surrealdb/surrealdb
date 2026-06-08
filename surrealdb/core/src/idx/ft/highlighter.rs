use std::collections::btree_map::Entry as BEntry;
use std::collections::hash_map::Entry as HEntry;
use std::collections::{BTreeMap, HashMap};

use anyhow::{Result, ensure};
use surrealdb_strand::Strand;

use crate::err::Error;
use crate::expr::Idiom;
use crate::idx::ft::Position;
use crate::idx::ft::offset::Offset;
use crate::val::{Array, Object, Value};

pub(crate) struct HighlightParams {
	pub(crate) prefix: Value,
	pub(crate) suffix: Value,
	pub(crate) match_ref: Value,
	pub(crate) partial: bool,
}

impl HighlightParams {
	pub(crate) fn match_ref(&self) -> &Value {
		&self.match_ref
	}
}

pub(super) struct Highlighter {
	prefix: Vec<char>,
	suffix: Vec<char>,
	fields: Vec<(Idiom, Value)>,
	offseter: Offseter,
}

impl Highlighter {
	pub(super) fn new(hlp: &HighlightParams, idiom: &Idiom, doc: &Value) -> Self {
		let prefix = hlp.prefix.to_raw_string().chars().collect();
		let suffix = hlp.suffix.to_raw_string().chars().collect();
		// Extract the fields we want to highlight
		let fields = doc.walk(idiom);
		Self {
			fields,
			prefix,
			suffix,
			offseter: Offseter::new(hlp.partial),
		}
	}

	pub(super) fn highlight(&mut self, term_len: u32, os: Vec<Offset>) {
		self.offseter.highlight(term_len, os);
	}

	fn extract(val: Value, vals: &mut Vec<Strand>) {
		match val {
			Value::String(s) => vals.push(s),
			Value::Number(n) => vals.push(n.to_string().into()),
			Value::Bool(b) => vals.push(b.to_string().into()),
			Value::Array(a) => {
				for v in a.0 {
					Self::extract(v, vals);
				}
			}
			Value::Object(a) => {
				for (_, v) in a.0 {
					Self::extract(v, vals);
				}
			}
			_ => {}
		}
	}
}

impl TryFrom<Highlighter> for Value {
	type Error = anyhow::Error;

	fn try_from(hl: Highlighter) -> Result<Self> {
		if hl.fields.is_empty() {
			return Ok(Self::None);
		}
		let mut vals: Vec<Strand> = vec![];
		for (_, f) in hl.fields {
			Highlighter::extract(f, &mut vals);
		}
		let mut res = Vec::with_capacity(vals.len());
		for (idx, val) in vals.into_iter().enumerate() {
			if let Some(m) = hl.offseter.offsets.get(&(idx as u32)) {
				let mut v: Vec<char> = val.chars().collect();
				let mut l = v.len();
				let mut d = 0;

				// We use a closure to append the prefix and the suffix
				let mut append = |s: u32, ix: &Vec<char>| -> Result<()> {
					let p = (s as usize) + d;
					ensure!(
						p <= l,
						Error::HighlightError(format!("position overflow: {s} - len: {l}"))
					);
					v.splice(p..p, ix.clone());
					let xl = ix.len();
					d += xl;
					l += xl;
					Ok(())
				};

				for (s, e) in m {
					append(*s, &hl.prefix)?;
					append(*e, &hl.suffix)?;
				}

				let s: String = v.iter().collect();
				res.push(Value::from(s));
			} else {
				res.push(Value::from(val));
			}
		}
		Ok(match res.len() {
			0 => Value::None,
			1 => res.remove(0),
			_ => Value::from(res),
		})
	}
}

pub(super) struct Offseter {
	partial: bool,
	offsets: HashMap<u32, BTreeMap<Position, Position>>,
}

impl Offseter {
	pub(super) fn new(partial: bool) -> Self {
		Self {
			partial,
			offsets: Default::default(),
		}
	}

	pub(super) fn highlight(&mut self, term_len: u32, os: Vec<Offset>) {
		for o in os {
			let (start, end) = if self.partial {
				let start = o.gen_start.min(o.end);
				let end = (start + term_len).min(o.end);
				(start, end)
			} else {
				(o.start, o.end)
			};
			match self.offsets.entry(o.index) {
				HEntry::Occupied(mut e) => match e.get_mut().entry(start) {
					BEntry::Vacant(e) => {
						e.insert(end);
					}
					BEntry::Occupied(mut e) => {
						if o.end.gt(e.get()) {
							e.insert(end);
						}
					}
				},
				HEntry::Vacant(e) => {
					e.insert(BTreeMap::from([(start, end)]));
				}
			}
		}
	}
}

impl From<Offseter> for Value {
	fn from(or: Offseter) -> Self {
		// If there are no offsets, return None
		if or.offsets.is_empty() {
			return Self::None;
		}
		// Create a new highlights object
		let mut highlights = Object::default();
		// Loop through the offsets and convert to highlights
		for (idx, offsets) in or.offsets {
			// For each highlight, output the start and end positions
			let mut spans = Vec::with_capacity(offsets.len());
			// Loop through the offsets and create a new range span
			for (s, e) in offsets {
				let mut span = Object::default();
				span.insert("s", Value::from(s));
				span.insert("e", Value::from(e));
				spans.push(Value::from(span));
			}
			// Add the highlight spans to the set of highlights
			highlights.insert(idx.to_string(), Value::Array(Array::from(spans)));
		}
		// Return the highlights object
		Value::Object(highlights)
	}
}
