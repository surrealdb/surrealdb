use crate::err::Error;
use crate::idx::ft::offsets::{Offset, Position};
use crate::sql::{Idiom, Value};
use std::collections::btree_map::Entry as BEntry;
use std::collections::hash_map::Entry as HEntry;
use std::collections::{BTreeMap, HashMap};

pub(super) struct Highlighter {
	prefix: Vec<char>,
	suffix: Vec<char>,
	fields: Vec<(Idiom, Value)>,
	offsets: HashMap<u32, BTreeMap<Position, Position>>,
}

impl Highlighter {
	pub(super) fn new(prefix: Value, suffix: Value, idiom: &Idiom, doc: &Value) -> Self {
		let prefix = prefix.to_raw_string().chars().collect();
		let suffix = suffix.to_raw_string().chars().collect();
		// Extract the fields we want to highlight
		let fields = doc.walk(&idiom);
		Self {
			fields,
			prefix,
			suffix,
			offsets: HashMap::default(),
		}
	}

	pub(super) fn highlight(&mut self, os: Vec<Offset>) {
		for o in os {
			match self.offsets.entry(o.index) {
				HEntry::Occupied(mut e) => match e.get_mut().entry(o.start) {
					BEntry::Vacant(e) => {
						e.insert(o.end);
					}
					BEntry::Occupied(mut e) => {
						if o.end.gt(e.get()) {
							e.insert(o.end);
						}
					}
				},
				HEntry::Vacant(e) => {
					e.insert(BTreeMap::from([(o.start, o.end)]));
				}
			}
		}
	}
}

impl TryFrom<Highlighter> for Value {
	type Error = Error;

	fn try_from(hl: Highlighter) -> Result<Self, Error> {
		if hl.fields.is_empty() {
			return Ok(Self::None);
		}
		let mut vals = vec![];
		for (_, f) in hl.fields {
			let s = match f {
				Value::Strand(s) => Some(s.0),
				Value::Number(n) => Some(n.to_string()),
				Value::Bool(b) => Some(b.to_string()),
				_ => None,
			};
			vals.push(s);
		}
		let mut res = Vec::with_capacity(vals.len());
		for (idx, m) in hl.offsets {
			if let Some(v) = vals.get_mut(idx as usize) {
				if let Some(v) = v {
					let mut v: Vec<char> = v.chars().collect();
					let mut d = 0;
					for (s, e) in m {
						let p = (s as usize) + d;
						v.splice(p..p, hl.prefix.clone());
						d += hl.prefix.len();
						let p = (e as usize) + d;
						v.splice(p..p, hl.suffix.clone());
						d += hl.suffix.len();
					}
					let s: String = v.iter().collect();
					res.push(Value::from(s));
				}
			}
		}
		Ok(match res.len() {
			0 => Value::None,
			1 => res.remove(0),
			_ => Value::from(res),
		})
	}
}
