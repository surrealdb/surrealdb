use crate::err::Error;
use crate::idx::ft::offsets::{Offset, Position};
use crate::sql::{Array, Idiom, Object, Value};
use std::collections::btree_map::Entry as BEntry;
use std::collections::hash_map::Entry as HEntry;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::default::Default;

pub(super) struct Highlighter {
	prefix: Vec<char>,
	suffix: Vec<char>,
	fields: Vec<(Idiom, Value)>,
	offseter: Offseter,
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
			offseter: Offseter::default(),
		}
	}

	pub(super) fn highlight(&mut self, os: Vec<Offset>) {
		self.offseter.highlight(os);
	}

	fn extract(val: Value, vals: &mut Vec<Option<String>>) {
		match val {
			Value::Strand(s) => vals.push(Some(s.0)),
			Value::Number(n) => vals.push(Some(n.to_string())),
			Value::Bool(b) => vals.push(Some(b.to_string())),
			Value::Array(a) => {
				for v in a.0 {
					Self::extract(v, vals);
				}
			}
			_ => vals.push(None),
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
			Highlighter::extract(f, &mut vals);
		}
		let mut res = Vec::with_capacity(vals.len());
		let mut idx = 0;
		for val in vals {
			if let Some(v) = val {
				if let Some(m) = hl.offseter.offsets.get(&idx) {
					let mut v: Vec<char> = v.chars().collect();
					let mut d = 0;
					for (s, e) in m {
						let p = (*s as usize) + d;
						v.splice(p..p, hl.prefix.clone());
						d += hl.prefix.len();
						let p = (*e as usize) + d;
						v.splice(p..p, hl.suffix.clone());
						d += hl.suffix.len();
					}
					let s: String = v.iter().collect();
					res.push(Value::from(s));
				} else {
					res.push(Value::from(v));
				}
			}
			idx += 1;
		}
		Ok(match res.len() {
			0 => Value::None,
			1 => res.remove(0),
			_ => Value::from(res),
		})
	}
}

#[derive(Default)]
pub(super) struct Offseter {
	offsets: HashMap<u32, BTreeMap<Position, Position>>,
}

impl Offseter {
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

impl TryFrom<Offseter> for Value {
	type Error = Error;

	fn try_from(or: Offseter) -> Result<Self, Error> {
		if or.offsets.is_empty() {
			return Ok(Self::None);
		}
		let mut res = BTreeMap::default();
		for (idx, offsets) in or.offsets {
			let mut r = Vec::with_capacity(offsets.len());
			for (s, e) in offsets {
				let mut o = BTreeMap::default();
				o.insert("s".to_string(), Value::from(s));
				o.insert("e".to_string(), Value::from(e));
				r.push(Value::Object(Object::from(o)));
			}
			res.insert(idx.to_string(), Value::Array(Array::from(r)));
		}
		Ok(match res.len() {
			0 => Value::None,
			_ => Value::from(Object::from(res)),
		})
	}
}
