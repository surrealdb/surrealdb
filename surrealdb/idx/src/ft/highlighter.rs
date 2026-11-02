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
			Value::None
			| Value::Null
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Uuid(_)
			| Value::Set(_)
			| Value::Geometry(_)
			| Value::Bytes(_)
			| Value::Table(_)
			| Value::RecordId(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_) => {}
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
				let end = start.saturating_add(term_len).min(o.end);
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

#[cfg(test)]
mod tests {
	use super::{HighlightParams, Highlighter, Offset, Offseter};
	use crate::expr::Idiom;
	use crate::val::{Number, Value};

	#[test]
	fn offseter_partial_saturates_and_merges_overlapping_ranges() {
		let mut offseter = Offseter::new(true);
		offseter.highlight(u32::MAX, &[Offset::new(0, 1, 2, 10), Offset::new(0, 5, 6, 12)]);
		let val = Value::from(offseter);
		let Value::Object(map) = val else {
			panic!("expected Object, got {val:?}");
		};
		let Some(Value::Array(spans)) = map.get("0") else {
			panic!("expected Array, got {:?}", map.get("0"));
		};
		assert_eq!(spans.len(), 1);
		let Value::Object(span) = &spans[0] else {
			panic!("expected Object span, got {:?}", spans[0]);
		};
		assert_eq!(span.get("s"), Some(&Value::from(2u32)));
		assert_eq!(span.get("e"), Some(&Value::from(12u32)));
	}

	#[test]
	fn highlighter_number_display_matches_analyzer_representation() {
		let params = HighlightParams {
			prefix: Value::from("["),
			suffix: Value::from("]"),
			match_ref: Value::from(0),
			partial: false,
		};
		let val = Value::Number(Number::Float(42.5));
		let mut hl = Highlighter::new(&params, &Idiom::default(), &val);
		// "42.5" is 4 bytes (no trailing 'f' from to_sql), so highlighting the whole token works.
		hl.highlight(4, &[Offset::new(0, 0, 0, 4)]);
		let res = Value::try_from(hl).unwrap();
		assert_eq!(res, Value::from("[42.5]"));
	}
}
