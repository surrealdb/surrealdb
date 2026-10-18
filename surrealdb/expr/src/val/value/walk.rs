use crate::expr::idiom::Idiom;
use crate::expr::part::{Next, Part};
use crate::val::Value;

impl Value {
	pub fn walk(&self, path: &[Part]) -> Vec<(Idiom, Self)> {
		self._walk(path, Idiom::default())
	}
	fn _walk(&self, path: &[Part], prev: Idiom) -> Vec<(Idiom, Self)> {
		match path.first() {
			// Get the current path part
			Some(p) => match self {
				// Current path part is an object
				Value::Object(v) => match p {
					Part::Field(f) => match v.get(f as &str) {
						Some(v) => v._walk(path.next(), prev.push(p.clone())),
						None => Value::None._walk(path.next(), prev.push(p.clone())),
					},
					Part::All => v
						.iter()
						.flat_map(|(field, v)| {
							v._walk(path.next(), prev.clone().push(Part::Field(field.clone())))
						})
						.collect::<Vec<_>>(),
					x => {
						if let Some(idx) = x.as_old_index() {
							match v.get(&idx.to_string()) {
								Some(v) => v._walk(path.next(), prev.push(p.clone())),
								None => Value::None._walk(path.next(), prev.push(p.clone())),
							}
						} else {
							vec![]
						}
					}
				},
				// Current path part is an array
				Value::Array(v) => match p {
					Part::First => match v.first() {
						Some(v) => v._walk(path.next(), prev.push(p.clone())),
						None => vec![],
					},
					Part::Last => match v.last() {
						Some(v) => v._walk(path.next(), prev.push(p.clone())),
						None => vec![],
					},
					x => {
						if let Some(idx) = x.as_old_index() {
							match v.get(idx) {
								Some(v) => v._walk(path.next(), prev.push(p.clone())),
								None => vec![],
							}
						} else {
							v.iter()
								.enumerate()
								.flat_map(|(i, v)| {
									v._walk(
										path.next(),
										prev.clone().push(Part::index_int(i as i64)),
									)
								})
								.collect::<Vec<_>>()
						}
					}
				},
				// Current path part is a set
				Value::Set(v) => match p {
					Part::First => match v.first() {
						Some(v) => v._walk(path.next(), prev.push(p.clone())),
						None => vec![],
					},
					Part::Last => match v.last() {
						Some(v) => v._walk(path.next(), prev.push(p.clone())),
						None => vec![],
					},
					x => {
						if let Some(idx) = x.as_old_index() {
							match v.nth(idx) {
								Some(v) => v._walk(path.next(), prev.push(p.clone())),
								None => vec![],
							}
						} else {
							v.iter()
								.enumerate()
								.flat_map(|(i, v)| {
									v._walk(
										path.next(),
										prev.clone().push(Part::index_int(i as i64)),
									)
								})
								.collect::<Vec<_>>()
						}
					}
				},
				// Ignore everything else
				_ => match p {
					Part::Field(_) => Value::None._walk(path.next(), prev.push(p.clone())),
					x => {
						if x.as_old_index().is_some() {
							Value::None._walk(path.next(), prev.push(p.clone()))
						} else {
							vec![]
						}
					}
				},
			},
			// No more parts so get the value
			None => vec![(prev, self.clone())],
		}
	}

	/// Visits every value [`Value::walk`] would return, in the same order, by
	/// reference.
	///
	/// `walk` answers with owned values and the paths that name them, so a
	/// caller that only measures what it finds deep-clones every match — a
	/// whole array for an array field — and drops it again. This is the same
	/// traversal with the path and the clone left out, and the two must stay
	/// in step: a measurement taken here has to be a measurement of what a
	/// caller reading `walk` would see.
	pub fn walk_ref(&self, path: &[Part], visit: &mut impl FnMut(&Self)) {
		let Some(p) = path.first() else {
			// No more parts so visit the value
			visit(self);
			return;
		};
		match self {
			// Current path part is an object
			Value::Object(v) => match p {
				Part::Field(f) => match v.get(f as &str) {
					Some(v) => v.walk_ref(path.next(), visit),
					None => Value::None.walk_ref(path.next(), visit),
				},
				Part::All => {
					for (_, v) in v.iter() {
						v.walk_ref(path.next(), visit);
					}
				}
				x => {
					if let Some(idx) = x.as_old_index() {
						match v.get(&idx.to_string()) {
							Some(v) => v.walk_ref(path.next(), visit),
							None => Value::None.walk_ref(path.next(), visit),
						}
					}
				}
			},
			// Current path part is an array
			Value::Array(v) => match p {
				Part::First => {
					if let Some(v) = v.first() {
						v.walk_ref(path.next(), visit);
					}
				}
				Part::Last => {
					if let Some(v) = v.last() {
						v.walk_ref(path.next(), visit);
					}
				}
				x => match x.as_old_index() {
					Some(idx) => {
						if let Some(v) = v.get(idx) {
							v.walk_ref(path.next(), visit);
						}
					}
					None => {
						for v in v.iter() {
							v.walk_ref(path.next(), visit);
						}
					}
				},
			},
			// Current path part is a set
			Value::Set(v) => match p {
				Part::First => {
					if let Some(v) = v.first() {
						v.walk_ref(path.next(), visit);
					}
				}
				Part::Last => {
					if let Some(v) = v.last() {
						v.walk_ref(path.next(), visit);
					}
				}
				x => match x.as_old_index() {
					Some(idx) => {
						if let Some(v) = v.nth(idx) {
							v.walk_ref(path.next(), visit);
						}
					}
					None => {
						for v in v.iter() {
							v.walk_ref(path.next(), visit);
						}
					}
				},
			},
			// Ignore everything else
			_ => match p {
				Part::Field(_) => Value::None.walk_ref(path.next(), visit),
				x => {
					if x.as_old_index().is_some() {
						Value::None.walk_ref(path.next(), visit);
					}
				}
			},
		}
	}

	/// Rebuild every set that holds a value [`Value::walk`] reaches along any
	/// of `paths`, innermost first.
	///
	/// A write that replaces the values `walk` names, through the concrete
	/// paths it answers with, leaves each set on the way holding its elements in
	/// their old positions: two of them may now be equal, or out of order. A
	/// [`Set`](crate::val::Set) must hold distinct elements in ascending order —
	/// its encoding is decoded under that contract — so each set the traversal
	/// passes through is rebuilt with [`Set::normalize`](crate::val::Set::normalize).
	///
	/// Rebuilding a set reorders its elements, which changes the element a
	/// positional step into it (`[0]`, `[$]`, `[1]`) names. The sets are
	/// therefore rebuilt one depth at a time, deepest first, across every path:
	/// a set is rebuilt only once every set within it is, and before any set
	/// holding it, so each step of every path still names the element it named
	/// when the values were written.
	///
	/// The value a path ends at is not itself rebuilt: it was replaced whole,
	/// not changed in place. The traversal is the one `walk` takes, arm for
	/// arm — on an array or a set, a step that is not a position descends into
	/// every element, as `walk` does — so the sets rebuilt are exactly those on
	/// the way to the values `walk` returns.
	pub fn normalize_sets_along(&mut self, paths: &[&[Part]]) {
		let deepest = paths.iter().map(|path| path.len()).max().unwrap_or(0);
		for depth in (0..deepest).rev() {
			for path in paths {
				self.normalize_sets_at(path, depth);
			}
		}
	}

	/// Rebuild the sets `depth` steps along `path` that a further step of
	/// `path` descends into.
	fn normalize_sets_at(&mut self, path: &[Part], depth: usize) {
		let Some(p) = path.first() else {
			return;
		};
		let Some(depth) = depth.checked_sub(1) else {
			if let Value::Set(v) = self {
				v.0.normalize();
			}
			return;
		};
		match self {
			Value::Object(v) => match p {
				Part::Field(f) => {
					if let Some(v) = v.get_mut(f.as_str()) {
						v.normalize_sets_at(path.next(), depth);
					}
				}
				Part::All => {
					for v in v.values_mut() {
						v.normalize_sets_at(path.next(), depth);
					}
				}
				x => {
					if let Some(v) = x.as_old_index().and_then(|idx| v.get_mut(&idx.to_string())) {
						v.normalize_sets_at(path.next(), depth);
					}
				}
			},
			Value::Array(v) => match p {
				Part::First => {
					if let Some(v) = v.first_mut() {
						v.normalize_sets_at(path.next(), depth);
					}
				}
				Part::Last => {
					if let Some(v) = v.last_mut() {
						v.normalize_sets_at(path.next(), depth);
					}
				}
				x => match x.as_old_index() {
					Some(idx) => {
						if let Some(v) = v.get_mut(idx) {
							v.normalize_sets_at(path.next(), depth);
						}
					}
					None => {
						for v in v.iter_mut() {
							v.normalize_sets_at(path.next(), depth);
						}
					}
				},
			},
			Value::Set(v) => match p {
				Part::First => {
					if let Some(v) = v.first_mut() {
						v.normalize_sets_at(path.next(), depth);
					}
				}
				Part::Last => {
					if let Some(v) = v.last_mut() {
						v.normalize_sets_at(path.next(), depth);
					}
				}
				x => match x.as_old_index() {
					Some(idx) => {
						if let Some(v) = v.nth_mut(idx) {
							v.normalize_sets_at(path.next(), depth);
						}
					}
					None => {
						for idx in 0..v.len() {
							if let Some(v) = v.nth_mut(idx) {
								v.normalize_sets_at(path.next(), depth);
							}
						}
					}
				},
			},
			// A value no path step descends into holds no set.
			_ => {}
		}
	}

	/// Rebuild every set within this value, at any depth, innermost first, so
	/// that each holds distinct elements in ascending order.
	pub fn normalize_all_sets(&mut self) {
		match self {
			Value::Object(v) => v.values_mut().for_each(Value::normalize_all_sets),
			Value::Array(v) => v.iter_mut().for_each(Value::normalize_all_sets),
			Value::Set(v) => {
				for idx in 0..v.len() {
					if let Some(v) = v.nth_mut(idx) {
						v.normalize_all_sets();
					}
				}
				v.0.normalize();
			}
			_ => {}
		}
	}

	/// Whether a set holds the value a concrete path — one [`Value::walk`]
	/// answers with — names, at any depth.
	pub fn path_crosses_set(&self, path: &[Part]) -> bool {
		let mut value = self;
		for p in path {
			value = match (value, p) {
				(Value::Set(_), _) => return true,
				(Value::Object(v), Part::Field(f)) => match v.get(f.as_str()) {
					Some(v) => v,
					None => return false,
				},
				(Value::Object(v), x) => {
					match x.as_old_index().and_then(|idx| v.get(&idx.to_string())) {
						Some(v) => v,
						None => return false,
					}
				}
				(Value::Array(v), Part::First) => match v.first() {
					Some(v) => v,
					None => return false,
				},
				(Value::Array(v), Part::Last) => match v.last() {
					Some(v) => v,
					None => return false,
				},
				(Value::Array(v), x) => match x.as_old_index().and_then(|idx| v.get(idx)) {
					Some(v) => v,
					None => return false,
				},
				_ => return false,
			};
		}
		false
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::expr::Idiom;
	use crate::syn;

	macro_rules! parse_val {
		($input:expr) => {
			crate::val::convert_public_value_to_internal(syn::value($input).unwrap())
		};
	}

	#[test]
	fn walk_blank() {
		let idi: Idiom = Default::default();
		let val = parse_val!("{ test: { other: null, something: 123 } }");
		let res: Vec<(Idiom, Value)> =
			vec![(Idiom::default(), parse_val!("{ test: { other: null, something: 123 } }"))];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_basic() {
		let idi: Idiom = syn::idiom("test.something").unwrap();
		let val = parse_val!("{ test: { other: null, something: 123 } }");
		let res: Vec<(Idiom, Value)> =
			vec![(syn::idiom("test.something").unwrap(), Value::from(123))];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_empty() {
		let idi: Idiom = syn::idiom("test.missing").unwrap();
		let val = parse_val!("{ test: { other: null, something: 123 } }");
		let res: Vec<(Idiom, Value)> = vec![(syn::idiom("test.missing").unwrap(), Value::None)];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_empty_object() {
		let idi: Idiom = syn::idiom("none.something.age").unwrap();
		let val = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res: Vec<(Idiom, Value)> =
			vec![(syn::idiom("none.something.age").unwrap(), Value::None)];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_empty_array() {
		let idi: Idiom = syn::idiom("none.something.*.age").unwrap();
		let val = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res: Vec<(Idiom, Value)> = vec![];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_empty_array_index() {
		let idi: Idiom = syn::idiom("none.something[0].age").unwrap();
		let val = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res: Vec<(Idiom, Value)> =
			vec![(syn::idiom("none.something[0].age").unwrap(), Value::None)];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array() {
		let idi: Idiom = syn::idiom("test.something").unwrap();
		let val = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res =
			vec![(syn::idiom("test.something").unwrap(), parse_val!("[{ age: 34 }, { age: 36 }]"))];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array_field() {
		let idi: Idiom = syn::idiom("test.something[*].age").unwrap();
		let val = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res: Vec<(Idiom, Value)> = vec![
			(syn::idiom("test.something[0].age").unwrap(), Value::from(34)),
			(syn::idiom("test.something[1].age").unwrap(), Value::from(36)),
		];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array_field_embedded() {
		let idi: Idiom = syn::idiom("test.something[*].tags").unwrap();
		let val = parse_val!(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }"
		);
		let res: Vec<(Idiom, Value)> = vec![
			(syn::idiom("test.something[0].tags").unwrap(), parse_val!("['code', 'databases']")),
			(syn::idiom("test.something[1].tags").unwrap(), parse_val!("['design', 'operations']")),
		];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array_field_embedded_index() {
		let idi: Idiom = syn::idiom("test.something[*].tags[1]").unwrap();
		let val = parse_val!(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }"
		);
		let res: Vec<(Idiom, Value)> = vec![
			(syn::idiom("test.something[0].tags[1]").unwrap(), Value::from("databases")),
			(syn::idiom("test.something[1].tags[1]").unwrap(), Value::from("operations")),
		];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_set_field_all() {
		let idi: Idiom = syn::idiom("test.*").unwrap();
		let val = parse_val!("{ test: { ' hello ', } }");
		let res: Vec<(Idiom, Value)> =
			vec![(syn::idiom("test[0]").unwrap(), Value::from(" hello "))];
		assert_eq!(res, val.walk(&idi));
	}

	#[test]
	fn walk_array_field_embedded_index_all() {
		let idi: Idiom = syn::idiom("test.something[*].tags[*]").unwrap();
		let val = parse_val!(
			"{ test: { something: [{ age: 34, tags: ['code', 'databases'] }, { age: 36, tags: ['design', 'operations'] }] } }"
		);
		let res: Vec<(Idiom, Value)> = vec![
			(syn::idiom("test.something[0].tags[0]").unwrap(), Value::from("code")),
			(syn::idiom("test.something[0].tags[1]").unwrap(), Value::from("databases")),
			(syn::idiom("test.something[1].tags[0]").unwrap(), Value::from("design")),
			(syn::idiom("test.something[1].tags[1]").unwrap(), Value::from("operations")),
		];
		assert_eq!(res, val.walk(&idi));
	}

	/// Rewriting the elements of a set's elements through the concrete paths
	/// `walk` answers with, then rebuilding along the field's path, leaves
	/// every set distinct and ascending, and never replaces an element with
	/// the set that holds it.
	#[test]
	fn normalize_sets_along_rebuilds_each_set_in_place() {
		let field: Idiom = syn::idiom("s[*][*]").unwrap();
		for (input, rewrite, expected) in [
			// Nothing rewritten: each set is kept as it is.
			("{ s: { [1, 2], [3], } }", None, "{ s: { [1, 2], [3], } }"),
			("{ s: { [1, 2], } }", None, "{ s: { [1, 2], } }"),
			("{ s: { { 1, 2, }, { 3, }, } }", None, "{ s: { { 1, 2, }, { 3, }, } }"),
			// Every element rewritten to one value: the outer set collapses.
			("{ s: { [1, 2], [3, 4], } }", Some(0), "{ s: { [0, 0], } }"),
			// The inner sets collapse first, and then the outer one.
			("{ s: { { 1, 2, }, { 3, }, } }", Some(0), "{ s: { { 0, }, } }"),
		] {
			let mut val = parse_val!(input);
			if let Some(n) = rewrite {
				for (path, _) in val.walk(&field) {
					val.put(&path, Value::from(n));
				}
			}
			val.normalize_sets_along(&[&field.0[..]]);
			assert_eq!(val, parse_val!(expected), "on `{input}`");
		}
	}

	/// An element moved out of order by an in-place write is put back in
	/// order, at every level that holds it.
	#[test]
	fn path_crosses_set_names_a_place_within_a_set() {
		let val = parse_val!("{ s: { { x: 1 }, }, a: [{ x: 1 }], o: { x: 1 } }");
		for (path, within) in [
			("s[0].x", true),
			("s[0]", true),
			// The set itself is not within one.
			("s", false),
			("a[0].x", false),
			("o.x", false),
			("missing.x", false),
		] {
			let path: Idiom = syn::idiom(path).unwrap();
			assert_eq!(val.path_crosses_set(&path), within, "at `{path:?}`");
		}
	}

	/// A path that reorders a set and a path naming one of its elements by
	/// position: whichever comes first, every inner set is rebuilt before the
	/// set holding it, so the positional path still names the element it wrote.
	#[test]
	fn normalize_sets_along_rebuilds_inner_sets_first_across_paths() {
		let reorder: Idiom = syn::idiom("s[*].x").unwrap();
		let positional: Idiom = syn::idiom("s[0].z[*]").unwrap();
		for paths in [[&reorder, &positional], [&positional, &reorder]] {
			let mut val = parse_val!("{ s: { { x: 1, z: { 1, 2, } }, { x: 2, z: { 3, } } } }");
			// The values each field writes, at the paths it wrote them.
			val.put(&syn::idiom("s[0].x").unwrap(), Value::from(2));
			val.put(&syn::idiom("s[1].x").unwrap(), Value::from(1));
			val.put(&syn::idiom("s[0].z[0]").unwrap(), Value::from(9));
			val.put(&syn::idiom("s[0].z[1]").unwrap(), Value::from(8));
			val.normalize_sets_along(&[&paths[0].0[..], &paths[1].0[..]]);
			assert_eq!(
				val,
				parse_val!("{ s: { { x: 1, z: { 3, } }, { x: 2, z: { 8, 9 } } } }"),
				"paths in the order {paths:?}"
			);
		}
	}

	#[test]
	fn normalize_sets_along_restores_order() {
		let field: Idiom = syn::idiom("o.s[*][*]").unwrap();
		let mut val = parse_val!("{ o: { s: { { 1, }, { 2, }, } } }");
		val.put(&syn::idiom("o.s[0][0]").unwrap(), Value::from(9));
		val.normalize_sets_along(&[&field.0[..]]);
		assert_eq!(val, parse_val!("{ o: { s: { { 2, }, { 9, }, } } }"));
		let Value::Object(o) = &val else {
			panic!("expected an object");
		};
		let Some(Value::Object(o)) = o.get("o") else {
			panic!("expected an object");
		};
		let Some(Value::Set(s)) = o.get("s") else {
			panic!("expected a set");
		};
		let elements: Vec<&Value> = s.iter().collect();
		assert!(elements.windows(2).all(|w| w[0] < w[1]), "the set must be ascending");
	}

	/// The borrowing walk is the owning one with the clone left out, so it must
	/// visit exactly the values `walk` returns, in the same order. A caller
	/// measuring through it is measuring what a caller reading `walk` sees.
	#[test]
	fn walk_ref_visits_what_walk_returns() {
		let val = parse_val!(
			"{ test: { other: null, something: [{ age: 34, tags: ['code', 'databases'] }, \
			 { age: 36, tags: [] }] }, tags: ['a', 'b', 'c'], empty: [], set: { 'x', 'y', }, n: 1 }"
		);
		for path in [
			"test",
			"test.something",
			"test.missing",
			"test.*",
			"test.something[*].age",
			"test.something[0].tags[1]",
			"test.something[*].tags[*]",
			"tags",
			"tags[*]",
			"tags[1]",
			"tags[5]",
			"empty",
			"empty[*]",
			"empty[0]",
			"set",
			"set[*]",
			"set[0]",
			"n",
			"n.missing",
			"missing.deep[*]",
		] {
			let idi: Idiom = syn::idiom(path).unwrap();
			let mut visited = Vec::new();
			val.walk_ref(&idi, &mut |v| visited.push(v.clone()));
			let walked: Vec<Value> = val.walk(&idi).into_iter().map(|(_, v)| v).collect();
			assert_eq!(visited, walked, "the two walks must agree on `{path}`");
		}
	}
}
