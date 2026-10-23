use std::collections::{BTreeMap, HashMap};

use anyhow::{Result, ensure};
use surrealdb_strand::Strand;

use crate::Error;
use crate::expr::Idiom;
use crate::ft::Position;
use crate::ft::offset::Offset;
use crate::val::{Array, Object, Value};

pub struct HighlightParams {
	pub prefix: Value,
	pub suffix: Value,
	pub match_ref: Value,
	pub partial: bool,
}

impl HighlightParams {
	pub fn match_ref(&self) -> &Value {
		&self.match_ref
	}
}

pub(crate) struct Highlighter {
	prefix: Vec<char>,
	suffix: Vec<char>,
	fields: Vec<(Idiom, Value)>,
	offseter: Offseter,
}

impl Highlighter {
	pub(crate) fn new(hlp: &HighlightParams, idiom: &Idiom, doc: &Value) -> Self {
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

	pub(crate) fn highlight(&mut self, term_len: u32, os: &[Offset]) {
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

pub(crate) struct Offseter {
	partial: bool,
	offsets: HashMap<u32, BTreeMap<Position, Position>>,
}

impl Offseter {
	pub(crate) fn new(partial: bool) -> Self {
		Self {
			partial,
			offsets: Default::default(),
		}
	}

	/// Records the highlighted ranges of `os`. A range that overlaps or touches
	/// one already recorded for the same value is merged with it, so the
	/// prefix and suffix markers of each range are emitted once, in order.
	pub(crate) fn highlight(&mut self, term_len: u32, os: &[Offset]) {
		for o in os {
			let (mut start, mut end) = if self.partial {
				let start = o.gen_start.min(o.end);
				let end = (start + term_len).min(o.end);
				(start, end)
			} else {
				(o.start, o.end)
			};
			let ranges = self.offsets.entry(o.index).or_default();
			if let Some((&ps, &pe)) = ranges.range(..=start).next_back()
				&& pe >= start
			{
				start = ps;
				end = end.max(pe);
				ranges.remove(&ps);
			}
			while let Some((&ns, &ne)) = ranges.range(start..).next() {
				if ns > end {
					break;
				}
				end = end.max(ne);
				ranges.remove(&ns);
			}
			ranges.insert(start, end);
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
