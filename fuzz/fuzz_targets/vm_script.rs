//! The SurrealQL scripts the bytecode VM is fuzzed with, and how one is run:
//! a short script over a fixed, seeded dataset, generated from the bytes of
//! an input. The fuzz target (`fuzz_vm_executor`) and the corpus test of the
//! VM's coverage (`surrealdb-core`'s `vm_declines`) both read it; each
//! supplies `Result` and `Unstructured` with the API of `arbitrary`'s.

use std::fmt::Write as _;
use std::time::Duration;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_types::ToSql;
use tokio::runtime::Builder;
use tokio::time::timeout;

use super::{Result, Unstructured};

/// Per-script wall-clock bound. A script that exceeds it on either run is
/// not compared, since a timeout is where the runs' speeds differ.
const SCRIPT_TIMEOUT: Duration = Duration::from_secs(5);

/// Statements a generated script holds, after the setup.
const MAX_STATEMENTS: u32 = 6;

/// Expression nesting depth.
const MAX_DEPTH: u32 = 4;

/// The dataset every datastore is seeded with. Its records mix types and
/// leave fields absent or `NULL`, so a path, comparison or function meets
/// every kind of operand.
const SETUP: &str = r#"
DEFINE NAMESPACE test;
DEFINE DATABASE test;
DEFINE TABLE person SCHEMALESS PERMISSIONS FOR select WHERE secret = false OR id = $auth.id, FOR create, update, delete WHERE id = $auth.id;
DEFINE TABLE post SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE likes TYPE RELATION IN person OUT person | post PERMISSIONS FULL;
DEFINE FIELD label ON person COMPUTED string::concat(name ?? '?', ':', <string> (age ?? 0));
DEFINE ACCESS user ON DATABASE TYPE RECORD;
DEFINE FUNCTION fn::inc($x: any) { RETURN $x + 1 } PERMISSIONS FULL;
DEFINE FUNCTION fn::name($p: any) { RETURN $p.name } PERMISSIONS FULL;
DEFINE FUNCTION fn::adults() { RETURN SELECT VALUE id FROM person WHERE age >= 18 ORDER BY id } PERMISSIONS FULL;
CREATE person:1 SET name = 'alice', age = 30, tags = ['a', 'b'], score = 1.5, active = true, secret = false, friend = person:2, nested = { x: 1, y: [1, 2] };
CREATE person:2 SET name = 'bob', age = 17, tags = ['b'], score = -2.25, active = false, secret = true, friend = person:3, nested = { x: 2, y: [] };
CREATE person:3 SET name = 'carol', age = 45, tags = [], score = 0, active = true, secret = false, friend = NONE, nested = { x: NULL };
CREATE person:4 SET name = 'dave', age = NULL, tags = ['c', 'a', 'a'], score = 10dec, active = NULL, secret = false, friend = person:1;
CREATE person:5 SET name = 'Eve', age = 30, score = 3f, secret = false, nested = 'flat';
CREATE person:['x', 1] SET name = 'arr', age = 5, secret = false, tags = ['z'];
CREATE post:1 SET title = 'hello', author = person:1, views = 10, words = ['hi', 'there'];
CREATE post:2 SET title = 'World', author = person:2, views = 0;
CREATE post:3 SET title = 'hello again', author = person:1, views = 7, words = [];
INSERT RELATION INTO likes [
	{ id: likes:1, in: person:1, out: post:2, weight: 2 },
	{ id: likes:2, in: person:2, out: post:1, weight: 1 },
	{ id: likes:3, in: person:1, out: person:3, weight: 5 },
	{ id: likes:4, in: person:3, out: post:1 },
];
"#;

/// Indexes the generator may add to the setup, so the same statements are
/// also answered through an index access path.
const INDEXES: &[&str] = &[
	"DEFINE INDEX person_age ON person FIELDS age;",
	"DEFINE INDEX person_name ON person FIELDS name UNIQUE;",
	"DEFINE INDEX person_tags ON person FIELDS tags;",
	"DEFINE INDEX post_views ON post FIELDS views;",
	"DEFINE INDEX person_age_name ON person FIELDS age, name;",
];

/// Deterministic builtins, with the arities the generator calls them with.
const FUNCTIONS: &[(&str, u32)] = &[
	("math::abs", 1),
	("math::max", 1),
	("math::min", 1),
	("math::sum", 1),
	("math::mean", 1),
	("math::floor", 1),
	("math::ceil", 1),
	("math::round", 1),
	("math::fixed", 2),
	("math::pow", 2),
	("string::len", 1),
	("string::lowercase", 1),
	("string::uppercase", 1),
	("string::concat", 2),
	("string::contains", 2),
	("string::starts_with", 2),
	("string::ends_with", 2),
	("string::split", 2),
	("string::join", 2),
	("string::slice", 3),
	("string::trim", 1),
	("string::reverse", 1),
	("array::len", 1),
	("array::append", 2),
	("array::concat", 2),
	("array::distinct", 1),
	("array::first", 1),
	("array::last", 1),
	("array::flatten", 1),
	("array::sort", 1),
	("array::reverse", 1),
	("array::slice", 3),
	("array::union", 2),
	("array::intersect", 2),
	("array::group", 1),
	("array::max", 1),
	("array::min", 1),
	("array::push", 2),
	("array::at", 2),
	("array::complement", 2),
	("array::difference", 2),
	("array::join", 2),
	("type::is_int", 1),
	("type::is_string", 1),
	("type::is_none", 1),
	("type::is_array", 1),
	("type::is_record", 1),
	("type::of", 1),
	("type::int", 1),
	("type::string", 1),
	("type::float", 1),
	("type::bool", 1),
	("type::array", 1),
	("type::number", 1),
	("type::record", 2),
	("type::table", 1),
	("object::keys", 1),
	("object::values", 1),
	("object::entries", 1),
	("object::len", 1),
	("record::id", 1),
	("record::tb", 1),
	("record::exists", 1),
	("count", 1),
	("value::diff", 2),
	("duration::days", 1),
	("time::year", 1),
	("time::floor", 2),
	("string::is_numeric", 1),
	("crypto::md5", 1),
	("not", 1),
];

/// Methods, as `.name(args)` on a receiver.
const METHODS: &[(&str, u32)] = &[
	("len", 0),
	("lowercase", 0),
	("uppercase", 0),
	("first", 0),
	("last", 0),
	("sort", 0),
	("distinct", 0),
	("reverse", 0),
	("keys", 0),
	("values", 0),
	("is_int", 0),
	("is_string", 0),
	("is_none", 0),
	("to_string", 0),
	("to_int", 0),
	("abs", 0),
	("join", 1),
	("concat", 1),
	("contains", 1),
	("slice", 2),
	("at", 1),
	("push", 1),
	("split", 1),
];

/// Builtins taking a closure over an array's elements, with the parameters
/// the closure declares.
const CLOSURE_FUNCTIONS: &[(&str, u32)] = &[
	("array::map", 1),
	("array::map", 2),
	("array::filter", 1),
	("array::any", 1),
	("array::all", 1),
	("array::find", 1),
	("array::find_index", 1),
	("array::filter_index", 1),
];

const FIELDS: &[&str] = &[
	"id",
	"name",
	"age",
	"tags",
	"score",
	"active",
	"secret",
	"friend",
	"nested",
	"nested.x",
	"nested.y",
	"tags[0]",
	"tags[1]",
	"friend.name",
	"friend.age",
	"friend.friend.name",
	"missing",
	"label",
	"title",
	"author",
	"author.name",
	"views",
	"words",
	"in",
	"out",
	"weight",
	"nested.y[0]",
	"tags.*",
	"friend.tags",
];

const TABLES: &[&str] = &["person", "post", "likes"];

const RECORDS: &[&str] = &[
	"person:1",
	"person:2",
	"person:3",
	"person:4",
	"person:5",
	"person:9",
	"person:['x', 1]",
	"post:1",
	"post:2",
	"post:9",
	"likes:1",
];

const BINARY: &[&str] = &[
	"+",
	"-",
	"*",
	"/",
	"%",
	"==",
	"!=",
	"<",
	"<=",
	">",
	">=",
	"&&",
	"||",
	"??",
	"?:",
	"IN",
	"NOT IN",
	"CONTAINS",
	"CONTAINSNOT",
	"CONTAINSANY",
	"CONTAINSALL",
	"INSIDE",
	"=",
	"ALLINSIDE",
	"ANYINSIDE",
	"IS",
	"IS NOT",
];

const CASTS: &[&str] = &["int", "float", "string", "bool", "decimal", "number", "array", "record"];

struct Gen<'a, 'b> {
	u: &'b mut Unstructured<'a>,
	/// Parameters bound by a `LET` earlier in the script.
	lets: Vec<String>,
	/// Closure parameters in scope.
	closure_params: Vec<String>,
}

impl Gen<'_, '_> {
	fn pick<'s>(&mut self, items: &'s [&'s str]) -> Result<&'s str> {
		Ok(items[self.u.choose_index(items.len())?])
	}

	fn expr(&mut self, out: &mut String, depth: u32) -> Result<()> {
		let leaf = depth >= MAX_DEPTH || self.u.ratio(1, 3)?;
		if leaf {
			return self.leaf(out);
		}
		match self.u.int_in_range(0..=13)? {
			0 | 1 => {
				out.push('(');
				self.expr(out, depth + 1)?;
				let op = self.pick(BINARY)?;
				let _ = write!(out, " {op} ");
				self.expr(out, depth + 1)?;
				out.push(')');
			}
			2 | 3 => {
				let (name, arity) = FUNCTIONS[self.u.choose_index(FUNCTIONS.len())?];
				out.push_str(name);
				out.push('(');
				for i in 0..arity {
					if i > 0 {
						out.push_str(", ");
					}
					self.expr(out, depth + 1)?;
				}
				out.push(')');
			}
			4 => {
				let (name, arity) = METHODS[self.u.choose_index(METHODS.len())?];
				out.push('(');
				self.expr(out, depth + 1)?;
				let _ = write!(out, ").{name}(");
				for i in 0..arity {
					if i > 0 {
						out.push_str(", ");
					}
					self.expr(out, depth + 1)?;
				}
				out.push(')');
			}
			5 => self.closure_call(out, depth)?,
			6 => {
				out.push('[');
				let n = self.u.int_in_range(0..=3)?;
				for i in 0..n {
					if i > 0 {
						out.push_str(", ");
					}
					self.expr(out, depth + 1)?;
				}
				out.push(']');
			}
			7 => {
				let n = self.u.int_in_range(0..=2)?;
				out.push('{');
				for i in 0..n {
					if i > 0 {
						out.push_str(", ");
					}
					let key = self.pick(&["a", "b", "name", "x"])?;
					let _ = write!(out, "{key}: ");
					self.expr(out, depth + 1)?;
				}
				out.push('}');
			}
			8 => {
				let prefix = self.pick(&["!", "-", "!!"])?;
				out.push_str(prefix);
				out.push('(');
				self.expr(out, depth + 1)?;
				out.push(')');
			}
			9 => {
				let cast = self.pick(CASTS)?;
				let _ = write!(out, "<{cast}> (");
				self.expr(out, depth + 1)?;
				out.push(')');
			}
			10 => {
				out.push_str("(IF ");
				self.expr(out, depth + 1)?;
				out.push_str(" THEN ");
				self.expr(out, depth + 1)?;
				out.push_str(" ELSE ");
				self.expr(out, depth + 1)?;
				out.push_str(" END)");
			}
			11 => {
				out.push('(');
				self.select(out, depth + 1)?;
				out.push(')');
			}
			12 => {
				let f = self.pick(&["fn::inc", "fn::name"])?;
				let _ = write!(out, "{f}(");
				self.expr(out, depth + 1)?;
				out.push(')');
			}
			_ => {
				// A path over an arbitrary base: a field, an index, a graph step.
				out.push('(');
				self.expr(out, depth + 1)?;
				out.push(')');
				let step = self.pick(&[
					".name",
					".age",
					"[0]",
					"[1]",
					".*",
					"->likes->?",
					"<-likes<-person",
					"->likes.weight",
					"[WHERE age > 18]",
					"[$]",
					".{ name, age }",
				])?;
				out.push_str(step);
			}
		}
		Ok(())
	}

	fn leaf(&mut self, out: &mut String) -> Result<()> {
		match self.u.int_in_range(0..=9)? {
			0 | 1 => {
				let field = self.pick(FIELDS)?;
				out.push_str(field);
			}
			2 => {
				let n = self.u.int_in_range(-3i64..=50)?;
				let _ = write!(out, "{n}");
			}
			3 => {
				let v = self.pick(&[
					"1.5", "-0.0", "2f", "3dec", "0", "1e3", "NaN", "-1", "100", "18", "30",
				])?;
				out.push_str(v);
			}
			4 => {
				let v = self.pick(&[
					"'alice'",
					"'a'",
					"''",
					"'Hello'",
					"'b'",
					"'hello again'",
					"'1'",
					"'3.5'",
					"'x,y'",
					"','",
				])?;
				out.push_str(v);
			}
			5 => {
				let v = self.pick(&[
					"true",
					"false",
					"NONE",
					"NULL",
					"[]",
					"{}",
					"[1, 2, 3]",
					"['a', 'b']",
					"1d",
					"2h30m",
					"d'2024-02-29T12:00:00Z'",
					"u'0190d8b0-8ae3-7e6b-9c1a-5f0e7a6a1c2d'",
					"1..4",
					"<bytes> 'ab'",
					"{ a: 1, b: [2] }",
					"(1, 2)",
				])?;
				out.push_str(v);
			}
			6 => {
				let r = self.pick(RECORDS)?;
				out.push_str(r);
			}
			7 if !self.closure_params.is_empty() => {
				let i = self.u.choose_index(self.closure_params.len())?;
				out.push_str(&self.closure_params[i]);
			}
			8 if !self.lets.is_empty() => {
				let i = self.u.choose_index(self.lets.len())?;
				out.push_str(&self.lets[i]);
			}
			_ => {
				let p = self.pick(&["$this", "$parent", "$auth", "$auth.id", "$value", "$nope"])?;
				out.push_str(p);
			}
		}
		Ok(())
	}

	fn closure_call(&mut self, out: &mut String, depth: u32) -> Result<()> {
		let (name, params) = CLOSURE_FUNCTIONS[self.u.choose_index(CLOSURE_FUNCTIONS.len())?];
		let method = self.u.arbitrary::<bool>()?;
		let names: Vec<String> =
			(0..params).map(|i| format!("$c{}_{i}", self.closure_params.len())).collect();
		let head = if method {
			out.push('(');
			self.expr(out, depth + 1)?;
			let short = name.trim_start_matches("array::");
			let _ = write!(out, ").{short}(");
			""
		} else {
			let _ = write!(out, "{name}(");
			self.expr(out, depth + 1)?;
			", "
		};
		out.push_str(head);
		out.push('|');
		out.push_str(&names.join(", "));
		out.push_str("| ");
		let saved = self.closure_params.len();
		self.closure_params.extend(names);
		self.expr(out, depth + 1)?;
		self.closure_params.truncate(saved);
		out.push(')');
		Ok(())
	}

	fn select(&mut self, out: &mut String, depth: u32) -> Result<()> {
		out.push_str("SELECT ");
		let value = self.u.ratio(1, 3)?;
		let grouped = !value && self.u.ratio(1, 5)?;
		let mut group_key = "";
		if value {
			out.push_str("VALUE ");
			self.expr(out, depth + 1)?;
		} else if grouped {
			let agg = self.pick(&[
				"count() AS n",
				"math::sum(age) AS s",
				"math::max(score) AS m",
				"array::group(tags) AS g",
				"array::distinct(name) AS d",
				"math::mean(views) AS v",
			])?;
			group_key = self.pick(&["age", "active", "author", "secret"])?;
			let _ = write!(out, "{agg}, {group_key}");
		} else if self.u.ratio(1, 3)? {
			out.push('*');
		} else {
			let n = self.u.int_in_range(1..=3)?;
			for i in 0..n {
				if i > 0 {
					out.push_str(", ");
				}
				self.expr(out, depth + 1)?;
				let _ = write!(out, " AS f{i}");
			}
		}
		out.push_str(" FROM ");
		match self.u.int_in_range(0..=5)? {
			0..=2 => out.push_str(self.pick(TABLES)?),
			3 => {
				let r = self.pick(RECORDS)?;
				out.push_str(r);
			}
			4 => out.push_str(self.pick(&[
				"person:1..4",
				"person:..=3",
				"person:2..",
				"post:1..",
			])?),
			_ => {
				out.push_str("ONLY ");
				out.push_str(self.pick(&["person:1", "post:1", "person:9"])?);
			}
		}
		if self.u.arbitrary::<bool>()? {
			out.push_str(" WHERE ");
			self.expr(out, depth + 1)?;
		}
		if grouped {
			let _ = write!(out, " GROUP BY {group_key}");
		} else if self.u.ratio(1, 3)? {
			let field = self.pick(&["age", "name", "score", "views", "title"])?;
			let dir = self.pick(&["ASC", "DESC"])?;
			let _ = write!(out, " ORDER BY {field} {dir}, id");
		}
		if self.u.ratio(1, 3)? {
			let n = self.u.int_in_range(0..=4)?;
			let _ = write!(out, " LIMIT {n}");
			if self.u.arbitrary::<bool>()? {
				let n = self.u.int_in_range(0..=3)?;
				let _ = write!(out, " START {n}");
			}
		}
		if !grouped && self.u.ratio(1, 6)? {
			out.push_str(" FETCH ");
			out.push_str(self.pick(&["friend", "author", "in", "out"])?);
		}
		Ok(())
	}

	fn statement(&mut self, out: &mut String, index: u32) -> Result<()> {
		match self.u.int_in_range(0..=13)? {
			0..=4 => self.select(out, 0)?,
			5 | 6 => {
				out.push_str("RETURN ");
				self.expr(out, 0)?;
			}
			7 => {
				let name = format!("$v{index}");
				let _ = write!(out, "LET {name} = ");
				self.expr(out, 0)?;
				self.lets.push(name);
			}
			8 => {
				let r = self.pick(&["person:1", "person:2", "person:4", "person:9", "post:1"])?;
				let field = self.pick(&["age", "name", "tags", "score", "extra"])?;
				let _ = write!(out, "UPDATE {r} SET {field} = ");
				self.expr(out, 1)?;
				out.push_str(self.pick(&[
					" RETURN AFTER",
					" RETURN BEFORE",
					" RETURN DIFF",
					"",
				])?);
			}
			9 => {
				let table = self.pick(&["person", "post"])?;
				let field = self.pick(&["age", "name", "tags", "extra"])?;
				let _ = write!(out, "UPDATE {table} SET {field} = ");
				self.expr(out, 1)?;
				out.push_str(" WHERE ");
				self.expr(out, 1)?;
			}
			10 => {
				let id = self.u.int_in_range(10..=13)?;
				let verb = self.pick(&["CREATE", "UPSERT"])?;
				let _ = write!(out, "{verb} person:{id} SET name = ");
				self.expr(out, 1)?;
				out.push_str(", age = ");
				self.expr(out, 1)?;
				out.push_str(", secret = false");
			}
			11 => {
				let r = self.pick(&["person:3", "person:5", "post:2", "likes:2", "person:9"])?;
				let _ = write!(out, "DELETE {r} RETURN BEFORE");
			}
			12 => {
				out.push_str("FOR $i IN ");
				self.expr(out, 1)?;
				out.push_str(" { IF ");
				self.closure_params.push("$i".to_owned());
				self.expr(out, 1)?;
				out.push_str(" { BREAK } ELSE { UPSERT person:20 SET n = (n ?? 0) + 1 } }");
				self.closure_params.pop();
			}
			_ => {
				out.push_str("RETURN fn::adults()");
			}
		}
		out.push_str(";\n");
		Ok(())
	}
}

/// The setup and the generated script for `data`, and who runs the script.
pub(crate) fn generate(data: &[u8]) -> Result<(String, String, Who)> {
	let mut u = Unstructured::new(data);
	let mut setup = String::from(SETUP);
	for index in INDEXES {
		if u.ratio(1, 3)? {
			setup.push_str(index);
			setup.push('\n');
		}
	}
	let who = match u.int_in_range(0..=5)? {
		0 => Who::Record,
		1 => Who::Viewer,
		_ => Who::Owner,
	};
	let mut script = String::new();
	let mut g = Gen {
		u: &mut u,
		lets: Vec::new(),
		closure_params: Vec::new(),
	};
	let n = g.u.int_in_range(1..=MAX_STATEMENTS)?;
	for i in 0..n {
		g.statement(&mut script, i)?;
	}
	Ok((setup, script, who))
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Who {
	Owner,
	Viewer,
	Record,
}

/// The rendered outcome of every statement of `script`, run as `who` on a
/// fresh datastore seeded with `setup`; `None` when the script did not finish
/// within [`SCRIPT_TIMEOUT`].
pub(crate) fn run(setup: &str, script: &str, who: Who) -> Option<Vec<String>> {
	let rt = Builder::new_current_thread().enable_all().build().expect("a runtime builds");
	rt.block_on(async {
		let dbs = Datastore::builder()
			.without_maintenance_tasks()
			.build_with_path("memory")
			.await
			.expect("a memory datastore builds");
		let owner = Session::owner().with_ns("test").with_db("test");
		let seeded = dbs.execute(setup, &owner, None).await.expect("the setup parses");
		for (i, r) in seeded.iter().enumerate() {
			if let Err(e) = &r.result {
				panic!("setup statement {i} failed: {}", e.message());
			}
		}
		let ses = match who {
			Who::Owner => owner,
			Who::Viewer => Session::viewer().with_ns("test").with_db("test"),
			Who::Record => Session::for_record(
				"test",
				"test",
				"user",
				surrealdb_types::Value::RecordId(surrealdb_types::RecordId::new("person", 1i64)),
			),
		};
		let results = timeout(SCRIPT_TIMEOUT, dbs.execute(script, &ses, None)).await.ok()?;
		Some(match results {
			// A script that does not parse fails the same way on every run.
			Err(e) => vec![format!("parse: {}", e.message())],
			Ok(results) => results
				.into_iter()
				.map(|r| match r.result {
					Ok(v) => format!("value: {}", v.to_sql()),
					Err(e) => format!("error: {}: {}", e.kind_str(), e.message()),
				})
				.collect(),
		})
	})
}
