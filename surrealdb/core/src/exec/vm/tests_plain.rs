//! Source scans that check two things the compile-time witness (`plain.rs`)
//! cannot see: that a lowered program is plain data, and that the run side
//! never plans.
//!
//! The type scan starts at `Program` and follows every type declared in the
//! `vm` module that a reachable declaration names. The body of each reachable
//! declaration must not name a `dyn` object, a function pointer or closure
//! trait, an interior-mutable cell or lock, a weak handle, `Any`, an engine
//! object by type name, or a `&'static str`. Audited exceptions are listed by
//! type name with their reason in [`AUDITED`]. A hand-written `PartialEq` on
//! a reachable type, or a hand-written unwind-safety implementation anywhere,
//! must also be listed there. Catalog definitions a program carries are
//! listed in [`CARRIED_CATALOG`].
//!
//! The run scan reads every file of the module that runs a program. It
//! rejects references to the planner, the lowering, or the statement bodies
//! in `ddl`. A running program never plans, binds, or lowers, and never
//! hands a statement's work to its Rust body. The files and functions that
//! plan when the query arrives, the host's answer to a fault, and the
//! declared dynamic-code services are listed by name in [`PLAN_TIME_FILES`]
//! and [`PLAN_TIME_FUNCTIONS`].
//!
//! The loader scan holds the interpreter and the host-facing modules
//! ([`LOADER_FILES`]) to loading and running units. None of them names the
//! planner, a compiled expression, or a compiler entry point. Items of the
//! call services (`calls`) are allowed only if listed in [`LOADER_CALLS`],
//! each with why it compiles nothing. No such file reaches the host's
//! compilation for a fault, imports included, except the host's answer to
//! the instruction that yields for a unit ([`FAULT_SITES`]).
//!
//! The scans are lexical: they see what a file names, not what the items it
//! names reach in turn. That a running program compiles nothing outside a
//! fault is asserted where it runs, by counting the compilations made while
//! a program runs ([`crate::exec::compiles`]). The operator scan keeps
//! `ExecOperator` to [`OPERATOR_FILES`].
//!
//! Each scan has a self-test proving it flags the token it forbids, and every
//! listed exception is checked to be still needed.

use std::collections::{BTreeMap, BTreeSet};

/// A type the type scan admits although its declaration holds a forbidden
/// token, with the tokens it may hold and why.
struct Audited {
	file: &'static str,
	name: &'static str,
	tokens: &'static [&'static str],
	partial_eq: bool,
	unwind_safe: bool,
	reason: &'static str,
}

const AUDITED: &[Audited] = &[
	Audited {
		file: "row.rs",
		name: "FieldName",
		tokens: &["Atomic"],
		partial_eq: true,
		unwind_safe: false,
		reason: "the slot is a per-run lookup hint with no semantic identity: equality \
		         compares the name alone and a name rebuilt from its text starts with no slot",
	},
	Audited {
		file: "units.rs",
		name: "StageSetUnit",
		tokens: &["LogicalWrite"],
		partial_eq: true,
		unwind_safe: false,
		reason: "a write that may reach any table names its stages by the bound write they are \
		         lowered from, which the run hands the compile cache to load a table's stages by \
		         name; equality compares the digest and then the write whole",
	},
	Audited {
		file: "plain.rs",
		name: "ValueData",
		tokens: &[],
		partial_eq: false,
		unwind_safe: true,
		reason: "a value a program holds is built only through the constructor that refuses \
		         a closure, the one engine object a value can hold",
	},
];

/// Catalog definitions a program carries, as plain data behind `Arc`: the
/// lowering is stamped by them and the host services read them. Their
/// `LogicalBody` memos are derived from the stored text and compare by it
/// (a hand-written `PartialEq` in the catalog crate).
const CARRIED_CATALOG: &[(&str, &str)] = &[
	("EventDefinition", "the events a table's event unit was lowered from, its stamp"),
	("IndexDefinition", "the indexes a write maintains, read by the index service"),
	("TableDefinition", "the views a table's view maintenance was lowered from, its stamp"),
];

/// Tokens no reachable declaration holds: what makes a type a handle rather
/// than data.
const FORBIDDEN_TOKENS: &[&str] = &[
	"dyn",
	"Mutex",
	"RwLock",
	"OnceLock",
	"OnceCell",
	"Cell",
	"RefCell",
	"Atomic",
	"ArcSwap",
	"Weak",
	"fn(",
	"Fn",
	"FnMut",
	"FnOnce",
	"Any",
	"&'static str",
];

/// Engine objects no reachable declaration names.
const FORBIDDEN_TYPES: &[&str] = &[
	"Clause",
	"ExecOperator",
	"WriteStage",
	"ClosureExec",
	"MethodDescriptor",
	"BitmapNode",
	"TopKThresholdProbe",
	"PlanningScope",
	"Kept",
	"LogicalOp",
	"LogicalWrite",
	"LogicalExpr",
	"Planner",
	"ExecutionContext",
	"Transaction",
	"FunctionRegistry",
	"CompiledGuard",
];

/// Files that lower a program when the query arrives, or name nothing that
/// runs: the run scan skips them, and every file of the lowering
/// (`ir_lower/`). `compile.rs` plans and lowers a statement through the IR,
/// and `ir_catalog.rs` reads the catalog that planning answers from.
const PLAN_TIME_FILES: &[&str] = &[
	"compile.rs",
	"footprint.rs",
	"ir_catalog.rs",
	"serve.rs",
	"inline.rs",
	"mod.rs",
	"plain.rs",
	"speculate.rs",
];

/// The directory of the lowering, whose files the run scan skips.
const LOWERING_DIR: &str = "ir_lower/";

/// Functions of run-side files the run scan skips, with why each may plan.
const PLAN_TIME_FUNCTIONS: &[(&str, &str, &str)] = &[
	(
		"fault.rs",
		"serve_stage_bundle",
		"a compile-cache load of the stages a write that may reach any table applies to a table, \
		 compiled on a miss",
	),
	(
		"fault.rs",
		"lower_closure",
		"a compile-cache miss: lowers the unit of a closure's body for the invoking frame",
	),
	("fault.rs", "serve_unit", "a compile-cache load of a closure body's unit, compiled on a miss"),
	(
		"fault.rs",
		"view_rows",
		"a fault: lowers the order of a view table the plan could not name, once per backfill",
	),
	(
		"fault.rs",
		"failure_of",
		"a compile-cache miss: the error a closure body that lowers no unit reports, which the \
		 planner answers by compiling the body",
	),
	(
		"fault.rs",
		"read_planner",
		"the host's answer to a read of a table the program holds no unit for: the planner the \
		 program's own reads were lowered with",
	),
	(
		"fault.rs",
		"link_read",
		"the host's answer to a read of a table the program holds no unit for: links the node's \
		 unit, or lowers it in a fault",
	),
	(
		"call_unit.rs",
		"unit_planner",
		"the planner a function's call unit is compiled with in a fault, for the frame the call is made in",
	),
	(
		"call_unit.rs",
		"load",
		"the host's answer to a late-bound statement's call: links the node's call unit of the \
		 function, or compiles it in a fault",
	),
	(
		"call_unit.rs",
		"compile",
		"the host's answer to a fault: compiles the call of the function the transaction reads",
	),
	(
		"call_unit.rs",
		"lower_statement",
		"the host's answer to a fault: lowers a call or a body, and again late-bound after its \
		 schema statements where their effect cannot be known",
	),
	(
		"call_unit.rs",
		"lower_once",
		"the host's answer to a fault: binds, maps and lowers a call or a body once",
	),
	(
		"schema_host.rs",
		"refuse_redefining_table",
		"declared schema service: what DEFINE TABLE refuses of a definition against the table it \
		 meets, judged as its body judges it",
	),
	(
		"schema_host.rs",
		"refuse_removing_lightweight",
		"declared schema service: what REMOVE TABLE refuses of a table whose LIGHTWEIGHT relations \
		 hold edges, judged as its body judges it",
	),
	(
		"schema_host.rs",
		"check_access_altered",
		"declared schema service: the access definition an ALTER ACCESS alters, checked as its body checks it",
	),
	(
		"schema_host.rs",
		"type_fields_paths",
		"declared schema service: the paths an evaluated index column names, read as the statement reads them",
	),
	(
		"schema_host.rs",
		"type_field_path",
		"declared schema service: the path an evaluated index column names, read as the statement reads it",
	),
	(
		"schema_host.rs",
		"substitute_evaluated",
		"declared schema service: a field path whose subscripts are evaluated, formed as the statement forms it",
	),
	(
		"schema_host.rs",
		"parse_path",
		"declared schema service: a field path that is evaluated, read as the statement reads it",
	),
	(
		"schema_host.rs",
		"define_field",
		"declared schema service: a field whose names are evaluated, defined by the code DEFINE FIELD defines with",
	),
	(
		"schema_host.rs",
		"remove_field",
		"declared schema service: a field whose names are evaluated, removed by the code REMOVE FIELD removes with",
	),
	(
		"schema_host.rs",
		"alter_field",
		"declared schema service: a field whose names are evaluated, altered by the code ALTER FIELD alters with",
	),
	(
		"schema_host.rs",
		"alter_field_clauses",
		"declared schema service: a field whose clauses are held as the definition they form, \
		 altered by the code ALTER FIELD alters with",
	),
	(
		"schema_host.rs",
		"check_index_column",
		"declared schema service: an index column that is evaluated, or a written one no field \
		 settles, checked as DEFINE INDEX checks one",
	),
	(
		"schema_host.rs",
		"check_table_altered",
		"declared schema service: a table as ALTER TABLE alters it, checked against the table it found",
	),
	(
		"schema_host.rs",
		"check_key_components",
		"declared schema service: the record id components a DEFINE FIELD's or an ALTER FIELD's \
		 definition names, checked as DEFINE FIELD checks them",
	),
	(
		"schema_host.rs",
		"refuse_dropping_key_structure",
		"declared schema service: what DEFINE FIELD and ALTER FIELD refuse of a definition that \
		 leaves a table's record id with no structure while its records are stored in component \
		 order, judged as their bodies judge it",
	),
	(
		"schema_host.rs",
		"check_computed_views",
		"declared schema service: a change to the value a field derives that an aggregated view \
		 reads, checked as DEFINE FIELD checks it",
	),
	(
		"schema_host.rs",
		"refuse_removing_id",
		"declared schema service: what REMOVE FIELD refuses of the id of a table whose \
		 definitions read its record id components, judged as its body judges it",
	),
	(
		"schema_host.rs",
		"refuse_named_id_conversion",
		"declared schema service: what DEFINE TABLE refuses of a table whose record id names its \
		 components, judged as its body judges it",
	),
	(
		"schema_host.rs",
		"validate_record_keys",
		"declared schema service: the records of a table checked against the record id structure \
		 DEFINE FIELD and ALTER FIELD give it, as their bodies check them",
	),
	(
		"schema_host.rs",
		"purge_edge_pointers",
		"declared schema service: the adjacency the edges of a table REMOVE TABLE removes left \
		 on their endpoints, purged by the code that body purges it with",
	),
	(
		"schema_host.rs",
		"validate_reference_options",
		"declared schema service: what DEFINE FIELD and ALTER FIELD refuse of a field's \
		 REFERENCE clause, judged as their bodies judge it",
	),
	(
		"schema_host.rs",
		"validate_materialize",
		"declared schema service: what DEFINE FIELD and ALTER FIELD refuse of a field's \
		 MATERIALIZE INTO clause, judged as their bodies judge it",
	),
	(
		"schema_host.rs",
		"refuse_changing_destination",
		"declared schema service: what DEFINE FIELD, ALTER FIELD and REMOVE FIELD refuse of a \
		 field a MATERIALIZE INTO clause fills, judged as their bodies judge it",
	),
	(
		"schema_host.rs",
		"refuse_removing_filled_table",
		"declared schema service: what REMOVE TABLE refuses of a table whose field a MATERIALIZE \
		 INTO clause fills, judged as its body judges it",
	),
	(
		"schema_host.rs",
		"clear_view_table",
		"declared schema service: the keys beneath a view table DEFINE TABLE ... AS SELECT clears \
		 before it fills the view, cleared by the code that body clears them with",
	),
	(
		"schema_host.rs",
		"refuse_view_cycle",
		"declared schema service: what DEFINE TABLE refuses of a view that would read itself, \
		 judged as its body judges it",
	),
	(
		"live.rs",
		"compile",
		"the compile of a subscription's unit, as its LIVE statement arrives or in a fault on a \
		 write that finds no unit held",
	),
	(
		"descriptor.rs",
		"from_cache",
		"a compile-cache miss: compiles what a table's read applies, its statements planned with \
		 it, once per catalog version and frame",
	),
	(
		"descriptor.rs",
		"unit_planner",
		"a compile-cache miss: the planner what a table's read applies is compiled with",
	),
	(
		"services.rs",
		"call_script_dynamic",
		"declared dynamic-code service: runs script source supplied as data",
	),
	("dispatch.rs", "table_unit", "plan time: links or lowers one table's read unit"),
	(
		"dispatch.rs",
		"table_read_unit",
		"plan time, or a fault: one table's read unit and its footprint, from the compile cache",
	),
	("dispatch.rs", "lower_table_unit", "plan time: lowers one table's read unit"),
];

/// What the run scan rejects outside plan time: the planner, the lowering,
/// and the statement bodies, whose checks a lowering decides and whose work
/// is the program's own.
const RUN_FORBIDDEN: &[&str] = &[
	"Planner",
	"lower::",
	"plan_logical",
	"plan_statement_in_scope",
	"lower_stages",
	"lower_repairs",
	"lower_groups",
	"lower_body",
	"lower_subroutine",
	"ddl::",
];

fn vm_dir() -> std::path::PathBuf {
	std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/exec/vm")
}

/// The non-test source files of the module, by name relative to it.
fn sources() -> BTreeMap<String, String> {
	let mut files = BTreeMap::new();
	let mut dirs = vec![vm_dir()];
	while let Some(dir) = dirs.pop() {
		for entry in std::fs::read_dir(&dir).expect("the vm module's directory") {
			let path = entry.expect("a directory entry").path();
			if path.is_dir() {
				dirs.push(path);
				continue;
			}
			let name = path
				.strip_prefix(vm_dir())
				.expect("a path inside the module")
				.to_string_lossy()
				.replace('\\', "/");
			let file =
				path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
			if !name.ends_with(".rs") || file.starts_with("tests") {
				continue;
			}
			let text = std::fs::read_to_string(&path).expect("a source file");
			files.insert(name, text);
		}
	}
	files
}

/// `text` with comments, string literals and character literals blanked,
/// keeping line breaks and lifetimes; the items under a `#[cfg(test)]`
/// attribute are blanked too.
fn strip(text: &str) -> String {
	let bytes = text.as_bytes();
	let mut out = String::with_capacity(text.len());
	let mut i = 0;
	let blank = |out: &mut String, s: &[u8]| {
		for &b in s {
			out.push(if b == b'\n' {
				'\n'
			} else {
				' '
			});
		}
	};
	while i < bytes.len() {
		let rest = &bytes[i..];
		if rest.starts_with(b"//") {
			let end = rest.iter().position(|&b| b == b'\n').unwrap_or(rest.len());
			blank(&mut out, &rest[..end]);
			i += end;
		} else if rest.starts_with(b"/*") {
			let mut depth = 0;
			let mut j = 0;
			while j < rest.len() {
				if rest[j..].starts_with(b"/*") {
					depth += 1;
					j += 2;
				} else if rest[j..].starts_with(b"*/") {
					depth -= 1;
					j += 2;
					if depth == 0 {
						break;
					}
				} else {
					j += 1;
				}
			}
			blank(&mut out, &rest[..j]);
			i += j;
		} else if let Some(len) = raw_string(rest) {
			blank(&mut out, &rest[..len]);
			i += len;
		} else if rest[0] == b'"' || rest.starts_with(b"b\"") {
			let start = usize::from(rest[0] == b'b');
			let mut j = start + 1;
			while j < rest.len() && rest[j] != b'"' {
				j += if rest[j] == b'\\' {
					2
				} else {
					1
				};
			}
			let end = (j + 1).min(rest.len());
			blank(&mut out, &rest[..end]);
			i += end;
		} else if rest[0] == b'\''
			&& let Some(len) = char_literal(&text[i..])
		{
			blank(&mut out, &rest[..len]);
			i += len;
		} else {
			let ch = text[i..].chars().next().expect("a character");
			out.push(ch);
			i += ch.len_utf8();
		}
	}
	strip_test_items(&out)
}

/// The length of the raw string literal `rest` starts with, if it does.
fn raw_string(rest: &[u8]) -> Option<usize> {
	let start = if rest.starts_with(b"br") {
		2
	} else if rest.starts_with(b"r") {
		1
	} else {
		return None;
	};
	let hashes = rest[start..].iter().take_while(|&&b| b == b'#').count();
	if rest.get(start + hashes) != Some(&b'"') {
		return None;
	}
	let close: Vec<u8> = std::iter::once(b'"').chain(std::iter::repeat_n(b'#', hashes)).collect();
	let body = start + hashes + 1;
	let end = rest[body..].windows(close.len()).position(|w| w == close.as_slice())?;
	Some(body + end + close.len())
}

/// The length of the character literal `rest` starts with, if it is one
/// rather than a lifetime.
fn char_literal(rest: &str) -> Option<usize> {
	let after = &rest[1..];
	if after.starts_with('\\') {
		// The quote, the backslash and the escaped character, then up to the
		// closing quote.
		let end = after.get(2..)?.find('\'')?;
		return Some(end + 4);
	}
	let ch = after.chars().next()?;
	let len = 1 + ch.len_utf8();
	rest[len..].starts_with('\'').then_some(len + 1)
}

/// `text` with every item under `#[cfg(test)]` blanked.
fn strip_test_items(text: &str) -> String {
	let mut out = text.to_owned();
	let marker = "#[cfg(test)]";
	let mut from = 0;
	while let Some(at) = out[from..].find(marker).map(|at| at + from) {
		let after = at + marker.len();
		let end = item_end(&out, after);
		let blanked: String = out[at..end]
			.chars()
			.map(|c| {
				if c == '\n' {
					'\n'
				} else {
					' '
				}
			})
			.collect();
		out.replace_range(at..end, &blanked);
		from = end;
	}
	out
}

/// Where the item starting at or after `from` ends: past its matched braces,
/// or past its `;` when it has none.
fn item_end(text: &str, from: usize) -> usize {
	let bytes = text.as_bytes();
	let mut i = from;
	while i < bytes.len() {
		match bytes[i] {
			b';' => return i + 1,
			b'{' => return matched(text, i, b'{', b'}'),
			_ => i += 1,
		}
	}
	bytes.len()
}

/// The index past the delimiter matching the one at `open`.
fn matched(text: &str, open: usize, left: u8, right: u8) -> usize {
	let bytes = text.as_bytes();
	let mut depth = 0usize;
	for (i, &b) in bytes.iter().enumerate().skip(open) {
		if b == left {
			depth += 1;
		} else if b == right {
			depth -= 1;
			if depth == 0 {
				return i + 1;
			}
		}
	}
	bytes.len()
}

/// The identifiers of `text`, with their byte offsets.
fn idents(text: &str) -> Vec<(usize, &str)> {
	let mut out = Vec::new();
	let bytes = text.as_bytes();
	let mut i = 0;
	while i < bytes.len() {
		if bytes[i].is_ascii_alphabetic() || bytes[i] == b'_' {
			let start = i;
			while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
				i += 1;
			}
			// A lifetime's name is not an identifier.
			if start == 0 || bytes[start - 1] != b'\'' {
				out.push((start, &text[start..i]));
			}
		} else {
			i += 1;
		}
	}
	out
}

/// One type declaration: where it is, and its body (fields or variants, or
/// an alias's definition).
struct Declaration {
	file: String,
	line: usize,
	body: String,
	is_enum: bool,
}

impl Declaration {
	/// The identifiers of the body in type position: not a field's name,
	/// and not a variant's.
	fn type_idents(&self) -> Vec<&str> {
		let bytes = self.body.as_bytes();
		let mut depth = vec![0usize; bytes.len() + 1];
		let mut d = 0usize;
		for (i, &b) in bytes.iter().enumerate() {
			if matches!(b, b')' | b']' | b'}') {
				d = d.saturating_sub(1);
			}
			depth[i] = d;
			if matches!(b, b'(' | b'[' | b'{') {
				d += 1;
			}
		}
		idents(&self.body)
			.into_iter()
			.filter(|&(at, word)| {
				let next = self.body[at + word.len()..].trim_start();
				let names_field = next.starts_with(':') && !next.starts_with("::");
				let names_variant = self.is_enum && depth[at] == 1;
				!names_field && !names_variant
			})
			.map(|(_, word)| word)
			.collect()
	}
}

/// Every `struct`, `enum` and `type` declared in `files` (stripped), by name.
fn declarations(files: &BTreeMap<String, String>) -> BTreeMap<String, Vec<Declaration>> {
	let mut out: BTreeMap<String, Vec<Declaration>> = BTreeMap::new();
	for (file, text) in files {
		let ids = idents(text);
		for (k, &(at, word)) in ids.iter().enumerate() {
			if !matches!(word, "struct" | "enum" | "type") {
				continue;
			}
			let Some(&(name_at, name)) = ids.get(k + 1) else {
				continue;
			};
			// `type` names an associated type inside an impl as well; those
			// are declarations too, which only ever widens the scan.
			let after = name_at + name.len();
			let bytes = text.as_bytes();
			let mut i = after;
			let mut angle = 0i32;
			let body = loop {
				let Some(&b) = bytes.get(i) else {
					break String::new();
				};
				match b {
					b'<' => angle += 1,
					b'>' if angle > 0 => angle -= 1,
					b'{' if angle == 0 && word != "type" => {
						break text[i..matched(text, i, b'{', b'}')].to_owned();
					}
					b'(' if angle == 0 && word == "struct" => {
						break text[i..matched(text, i, b'(', b')')].to_owned();
					}
					b'=' if angle == 0 && word == "type" => {
						let end = text[i..].find(';').map_or(text.len(), |e| e + i);
						break text[i..end].to_owned();
					}
					b';' if angle == 0 => break String::new(),
					_ => {}
				}
				i += 1;
			};
			let line = text[..at].matches('\n').count() + 1;
			out.entry(name.to_owned()).or_default().push(Declaration {
				file: file.clone(),
				line,
				body,
				is_enum: word == "enum",
			});
		}
	}
	out
}

/// The names of the declarations reachable from `root` through the bodies.
fn reachable(decls: &BTreeMap<String, Vec<Declaration>>, root: &str) -> BTreeSet<String> {
	let mut seen = BTreeSet::new();
	let mut pending = vec![root.to_owned()];
	while let Some(name) = pending.pop() {
		if !seen.insert(name.clone()) {
			continue;
		}
		for decl in decls.get(&name).into_iter().flatten() {
			for ident in decl.type_idents() {
				if decls.contains_key(ident) && !seen.contains(ident) {
					pending.push(ident.to_owned());
				}
			}
		}
	}
	seen
}

/// The forbidden tokens and type names `body` holds.
fn forbidden_in(decl: &Declaration) -> Vec<&'static str> {
	let body = decl.body.as_str();
	let mut found = BTreeSet::new();
	let compact: String = body.split_whitespace().collect::<Vec<_>>().join(" ");
	if compact.contains("& 'static str") || compact.contains("&'static str") {
		found.insert("&'static str");
	}
	let squeezed: String = body.chars().filter(|c| !c.is_whitespace()).collect();
	if idents(&squeezed)
		.iter()
		.any(|&(at, word)| word == "fn" && squeezed[at + 2..].starts_with('('))
	{
		found.insert("fn(");
	}
	for word in decl.type_idents() {
		for &token in FORBIDDEN_TOKENS {
			let hit = match token {
				"Atomic" | "ArcSwap" => word.starts_with(token),
				"dyn" | "Mutex" | "RwLock" | "OnceLock" | "OnceCell" | "Cell" | "RefCell"
				| "Weak" | "Fn" | "FnMut" | "FnOnce" | "Any" => word == token,
				_ => false,
			};
			if hit {
				found.insert(token);
			}
		}
		for &name in FORBIDDEN_TYPES {
			if word == name {
				found.insert(name);
			}
		}
	}
	found.into_iter().collect()
}

/// What the type scan finds wrong in `files`, starting at `root`.
fn type_scan(files: &BTreeMap<String, String>, root: &str) -> Vec<String> {
	let stripped: BTreeMap<String, String> =
		files.iter().map(|(name, text)| (name.clone(), strip(text))).collect();
	let decls = declarations(&stripped);
	let reach = reachable(&decls, root);
	let mut problems = Vec::new();
	for name in &reach {
		for decl in decls.get(name).into_iter().flatten() {
			let audited = AUDITED.iter().find(|a| a.name == name && a.file == decl.file);
			for token in forbidden_in(decl) {
				if audited.is_some_and(|a| a.tokens.contains(&token)) {
					continue;
				}
				problems.push(format!("{}:{} `{name}` holds `{token}`", decl.file, decl.line));
			}
			for word in decl.type_idents() {
				if word.ends_with("Definition") && !CARRIED_CATALOG.iter().any(|(c, _)| *c == word)
				{
					problems.push(format!(
						"{}:{} `{name}` carries catalog `{word}`, which is not audited",
						decl.file, decl.line
					));
				}
			}
		}
	}
	// A hand-written impl that the witness cannot see through is audited.
	for (file, text) in &stripped {
		for (at, word) in idents(text) {
			if word != "impl" {
				continue;
			}
			// The header runs to the impl's body; a trait impl names the trait
			// last before `for` and the type first after it.
			let end = text[at..].find(['{', ';']).map_or(text.len(), |e| at + e);
			let header = idents(&text[at..end]);
			let Some(k) = header.iter().position(|&(_, w)| w == "for") else {
				continue;
			};
			let (Some(&(_, trait_name)), Some(&(_, target))) =
				(header.get(k - 1), header.get(k + 1))
			else {
				continue;
			};
			let line = text[..at].matches('\n').count() + 1;
			let audited = AUDITED.iter().find(|a| a.name == target && a.file == *file);
			let unaudited = match trait_name {
				"PartialEq" => reach.contains(target) && !audited.is_some_and(|a| a.partial_eq),
				"UnwindSafe" | "RefUnwindSafe" => !audited.is_some_and(|a| a.unwind_safe),
				_ => false,
			};
			if unaudited {
				problems.push(format!(
					"{file}:{line} hand-written `{trait_name}` for `{target}`, which is not audited"
				));
			}
		}
	}
	problems
}

/// What the run scan finds in `files`: a reference to what
/// [`RUN_FORBIDDEN`] names outside plan time.
fn run_scan(files: &BTreeMap<String, String>) -> Vec<String> {
	let mut problems = Vec::new();
	for (file, text) in files {
		if file.starts_with(LOWERING_DIR) || PLAN_TIME_FILES.contains(&file.as_str()) {
			continue;
		}
		let mut text = strip(text);
		blank_uses(&mut text);
		for &(_, name, _) in PLAN_TIME_FUNCTIONS.iter().filter(|(f, _, _)| *f == file) {
			blank_functions(&mut text, name);
		}
		for (at, word) in idents(&text) {
			let line = text[..at].matches('\n').count() + 1;
			for &token in RUN_FORBIDDEN {
				let hit = match token.strip_suffix("::") {
					Some(module) => word == module && text[at + word.len()..].starts_with("::"),
					None => word == token,
				};
				if hit {
					problems.push(format!("{file}:{line} references `{token}` at run time"));
				}
			}
		}
	}
	problems
}

/// `text` with its `use` declarations blanked: an import plans nothing.
fn blank_uses(text: &mut String) {
	let ids: Vec<usize> =
		idents(text).into_iter().filter(|&(_, w)| w == "use").map(|(at, _)| at).collect();
	for at in ids.into_iter().rev() {
		let end = text[at..].find(';').map_or(text.len(), |e| at + e + 1);
		let blanked: String = text[at..end]
			.chars()
			.map(|c| {
				if c == '\n' {
					'\n'
				} else {
					' '
				}
			})
			.collect();
		text.replace_range(at..end, &blanked);
	}
}

/// `text` with every function named `name`, signature and body, blanked.
fn blank_functions(text: &mut String, name: &str) {
	loop {
		let ids = idents(text);
		let Some(at) = ids.windows(2).find(|w| w[0].1 == "fn" && w[1].1 == name).map(|w| w[0].0)
		else {
			return;
		};
		let end = item_end(text, at);
		let blanked: String = text[at..end]
			.chars()
			.map(|c| {
				if c == '\n' {
					'\n'
				} else {
					' '
				}
			})
			.collect();
		text.replace_range(at..end, &blanked);
	}
}

#[test]
fn every_type_a_program_reaches_is_plain_data() {
	let problems = type_scan(&sources(), "Program");
	assert!(problems.is_empty(), "{problems:#?}");
}

#[test]
fn the_type_scan_reaches_the_whole_program() {
	let files = sources();
	let stripped: BTreeMap<String, String> =
		files.iter().map(|(name, text)| (name.clone(), strip(text))).collect();
	let reach = reachable(&declarations(&stripped), "Program");
	for name in ["Instr", "Source", "WriteSpec", "StoreInstr", "LoweredEvents", "TableDispatch"] {
		assert!(reach.contains(name), "`{name}` is not reached from `Program`");
	}
}

#[test]
fn the_type_scan_flags_each_forbidden_token() {
	let cases: &[(&str, &str)] = &[
		("pub struct Program { e: Arc<dyn Evaluate> }", "dyn"),
		("pub struct Program { e: Box<dyn Send> }", "dyn"),
		("pub struct Program { c: Mutex<u32> }", "Mutex"),
		("pub struct Program { c: RwLock<u32> }", "RwLock"),
		("pub struct Program { c: std::sync::OnceLock<u32> }", "OnceLock"),
		("pub struct Program { c: OnceCell<u32> }", "OnceCell"),
		("pub struct Program { c: Cell<u32> }", "Cell"),
		("pub struct Program { c: RefCell<u32> }", "RefCell"),
		("pub struct Program { c: AtomicU64 }", "Atomic"),
		("pub struct Program { c: ArcSwapOption<u32> }", "ArcSwap"),
		("pub struct Program { c: Weak<Program> }", "Weak"),
		("pub struct Program { f: fn(u32) -> u32 }", "fn("),
		("pub struct Program { f: Arc<Fn(u32)> }", "Fn"),
		("pub struct Program { f: Box<FnMut()> }", "FnMut"),
		("pub struct Program { f: Box<FnOnce()> }", "FnOnce"),
		("pub struct Program { a: Box<Any> }", "Any"),
		("pub struct Program { n: &'static str }", "&'static str"),
		("pub enum Program { Op(Arc<ExecOperator>) }", "ExecOperator"),
		("pub struct Program(Arc<ClosureExec>);", "ClosureExec"),
		("pub struct Program { s: Spec } struct Spec { p: Kept }", "Kept"),
		("pub struct Program { s: Alias } type Alias = Vec<Planner>;", "Planner"),
	];
	for (source, token) in cases {
		let files = BTreeMap::from([("synthetic.rs".to_owned(), (*source).to_owned())]);
		let problems = type_scan(&files, "Program");
		assert!(
			problems.iter().any(|p| p.contains(&format!("`{token}`"))),
			"`{source}` is not flagged for `{token}`: {problems:?}"
		);
	}
	// A token in a comment, a string or a test item is not a field.
	let clean = "pub struct Program { // dyn Mutex\n n: String, s: Spec }\n\
	             struct Spec { why: u8 /* RefCell */ }\n\
	             const NOTE: &str = \"Arc<dyn Evaluate>\";\n\
	             #[cfg(test)]\nstruct Spec { c: Mutex<u8> }";
	let files = BTreeMap::from([("synthetic.rs".to_owned(), clean.to_owned())]);
	assert_eq!(type_scan(&files, "Program"), Vec::<String>::new());
}

#[test]
fn the_type_scan_flags_an_unaudited_impl_and_catalog_type() {
	let cases: &[(&str, &str)] = &[
		(
			"pub struct Program { s: Spec } struct Spec(u8); impl PartialEq for Spec { }",
			"hand-written `PartialEq` for `Spec`",
		),
		(
			"pub struct Program; struct Handle; impl std::panic::RefUnwindSafe for Handle {}",
			"hand-written `RefUnwindSafe` for `Handle`",
		),
		(
			"pub struct Program { v: Arc<crate::catalog::ViewDefinition> }",
			"carries catalog `ViewDefinition`",
		),
	];
	for (source, expected) in cases {
		let files = BTreeMap::from([("synthetic.rs".to_owned(), (*source).to_owned())]);
		let problems = type_scan(&files, "Program");
		assert!(
			problems.iter().any(|p| p.contains(expected)),
			"`{source}` is not flagged for {expected}: {problems:?}"
		);
	}
}

#[test]
fn nothing_that_runs_a_program_plans() {
	let problems = run_scan(&sources());
	assert!(problems.is_empty(), "{problems:#?}");
}

#[test]
fn the_run_scan_flags_each_planning_reference() {
	for &token in RUN_FORBIDDEN {
		let call = match token.strip_suffix("::") {
			Some(module) => format!("super::{module}::lower(planner, plan)"),
			None => format!("{token}(x)"),
		};
		let source = format!("async fn step() {{ let _ = {call}; }}");
		let files = BTreeMap::from([("host.rs".to_owned(), source.clone())]);
		let problems = run_scan(&files);
		assert!(
			problems.iter().any(|p| p.contains(&format!("`{token}`"))),
			"`{source}` is not flagged for `{token}`: {problems:?}"
		);
	}
	// Plan-time files, plan-time functions, imports and comments are not on
	// the run side.
	let files = BTreeMap::from([
		("ir_lower/x.rs".to_owned(), "fn f() { Planner::new() }".to_owned()),
		("serve.rs".to_owned(), "fn f() { Planner::new() }".to_owned()),
		(
			"dispatch.rs".to_owned(),
			"use crate::exec::planner::Planner;\n\
			 async fn table_unit(planner: &Planner<'_>) { super::compile::compile(planner) }\n\
			 fn run() { /* Planner */ }"
				.to_owned(),
		),
	]);
	assert_eq!(run_scan(&files), Vec::<String>::new());
}

/// The interpreter and the modules a running program reaches the host
/// through: they load and run units, and compile none.
const LOADER_FILES: &[&str] = &["closure.rs", "host.rs", "interp.rs", "scalar.rs", "services.rs"];

/// What the loader scan rejects in [`LOADER_FILES`]: the planner, a compiled
/// expression and the entry points that compile one, and the lowering. A
/// name ending in `(` is rejected as a call.
const LOADER_FORBIDDEN: &[&str] = &[
	"Planner",
	"Clause",
	"scalar_metadata(",
	"compile_expr",
	"plan_logical",
	"bind_statement",
	"get_or_compile",
	"compile_body",
	"lower::",
	"lower_closure_body",
	"invoke_closure",
];

/// The items of the call services (`vm::calls`) that [`LOADER_FILES`] may
/// name, each with why it compiles nothing. Every other path through the
/// module is rejected, imports included, and each item of a grouped import
/// is checked on its own. So is a glob import from it, a rename of it, or
/// the module named alone, which the scan cannot follow. A listed item is
/// matched with everything beneath it, so each is an item and none a module;
/// the closure value and its body, whose members include compilations, are
/// listed in [`LOADER_CLOSURE_PATHS`] instead.
const LOADER_CALLS: &[(&str, &str)] = &[
	("EnteredCall", "the frame of an entered user-defined function call, its auth limit"),
	("bind_closure_args", "binds an invocation's arguments to the declared kinds"),
	("helpers::validate_return", "checks a body's value against the return kind"),
	(
		"helpers::invoke_uncompiled_closure",
		"a closure value the engine did not compile, which no unit stands for: the rest of an \
		 API chain, run by its stage, or a value made outside a plan, run by it",
	),
	("check_script_allowed", "the capability check of an embedded script"),
	("run_script", "the script runtime: an effect, not a compilation"),
	("enter_model", "the frame of a model call"),
	("compute_model", "the model runtime: an effect, not a compilation"),
	("read_module", "reads a Surrealism module's bytes from storage"),
	("module_runtime_missing", "the error of a build with no module runtime"),
	("module_export", "resolves a Surrealism module's export"),
	("ModuleExport", "a resolved Surrealism module export"),
	("run_module_export", "the module runtime: an effect, not a compilation"),
];

/// The paths through `calls` to the closure value and its body that
/// [`LOADER_FILES`] may name, each matched exactly, with why it compiles
/// nothing. The members [`LOADER_FILES`] may call on a value are
/// [`LOADER_CLOSURE_MEMBERS`].
const LOADER_CLOSURE_PATHS: &[(&str, &str)] = &[
	("CompiledClosure", "the closure value, named as a type"),
	("CompiledClosure::planned", "makes a value the VM's literal makes, with no body"),
	("CompiledClosure::digest", "the code reference a unit is found by"),
];

/// The public members of the closure value ([`closure_members`]) that
/// [`LOADER_FILES`] may call, the linked-run entry points, with why each
/// compiles nothing outside a counted fault. Every other member is rejected,
/// called as a method or named by a path: it makes a value outside the run
/// (`CompiledClosure::of_stored`).
const LOADER_CLOSURE_MEMBERS: &[(&str, &str)] = &[
	(
		"invoke_linked",
		"runs the unit the instruction linked, or hands the step back for the instruction's \
		 fault",
	),
	(
		"invoke",
		"runs the linked unit; a link that stopped standing during the call is loaded again \
		 from the compile cache",
	),
	("planned", "makes a value the VM's literal makes, with no body"),
	("digest", "the code reference a unit is found by"),
	("logical", "the closure as written"),
	("captures", "the values the closure captured"),
	("frame", "the frame an invocation runs a unit in"),
	("dives", "whether an invocation spends the computation-depth budget"),
	("reading_row", "the value whose body reads the invoking row, made with no body"),
	("rows", "which rows the value's unit takes, which its unit is found by"),
	(
		"capturing_forbidden_row",
		"marks a value the VM's literal makes as having captured a KNN query vector's row being \
		 scanned",
	),
	(
		"captured_forbidden_row",
		"the KNN query vector's row being scanned the value captured, which refuses the \
		 invocation",
	),
];

/// The public members of the closure value (`CompiledClosure`), as its impl
/// blocks declare them.
fn closure_members() -> Vec<String> {
	let path =
		std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/exec/vm/calls/closure.rs");
	let text = strip(&std::fs::read_to_string(path).expect("the closure value's source"));
	let ids = idents(&text);
	let mut members = Vec::new();
	for pair in ids.windows(2).filter(|w| w[0].1 == "impl" && w[1].1 == "CompiledClosure") {
		let at = pair[0].0;
		let end = item_end(&text, at);
		let block = &text[at..end];
		for pair in idents(block).windows(2).filter(|w| w[0].1 == "fn") {
			let line_start = block[..pair[0].0].rfind('\n').map_or(0, |n| n + 1);
			if block[line_start..pair[0].0].contains("pub") {
				members.push(pair[1].1.to_owned());
			}
		}
	}
	members.sort();
	members.dedup();
	members
}

/// The functions of [`LOADER_FILES`] that may reach the host's compilation
/// for a fault (`fault.rs`), with why: the answer to the instruction that
/// yields for a unit, and the invocation inside the call that instruction
/// makes whose linked unit stopped standing during it, and nothing else.
const FAULT_SITES: &[(&str, &str, &str)] = &[(
	"closure.rs",
	"load_unit",
	"the load of a closure body's unit from the compile cache, compiled on a miss, for \
		 `Step::NeedUnit` and for an invocation whose link stopped standing during the call",
)];

/// The paths through `calls` that `text`, stripped, names, with the
/// offset of each: the items after the module, joined by `::`, one for each
/// item a grouped import names, or `None` for a mention of the module the
/// scan cannot follow — a glob import from it, a rename of it, or the module
/// named alone.
fn calls_paths(text: &str) -> Vec<(usize, Option<String>)> {
	let mut paths = Vec::new();
	for (at, word) in idents(text) {
		let rest = &text[at + word.len()..];
		if word == "calls" {
			let before = text[..at].trim_end();
			let import = text[..at]
				.rsplit(';')
				.next()
				.is_some_and(|item| item.trim_start().starts_with("use "));
			if before.ends_with('.')
				|| !(rest.trim_start().starts_with("::") || before.ends_with("::") || import)
			{
				continue;
			}
		}
		if word != "calls" || rest.trim_start().starts_with('(') {
			continue;
		}
		let mut named = Vec::new();
		match use_tree(rest, &[], &mut named) {
			Some(_) => paths.extend(named.into_iter().map(|path| (at, Some(path)))),
			None => paths.push((at, None)),
		}
	}
	paths
}

/// How many bytes of identifier `text` starts with.
fn ident_len(text: &str) -> usize {
	text.bytes().take_while(|b| b.is_ascii_alphanumeric() || *b == b'_').count()
}

/// Read the path or use tree `rest` starts with, just after a module or item
/// whose path below the module is `prefix`: push onto `named` every path it
/// names, and answer the text after it, or `None` for a tree the scan cannot
/// follow — a glob, or one naming the module itself.
fn use_tree<'a>(mut rest: &'a str, prefix: &[&'a str], named: &mut Vec<String>) -> Option<&'a str> {
	let mut segments = prefix.to_vec();
	while let Some(after) = rest.trim_start().strip_prefix("::") {
		let after = after.trim_start();
		if let Some(mut group) = after.strip_prefix('{') {
			loop {
				group = group.trim_start();
				if let Some(after_group) = group.strip_prefix('}') {
					return Some(after_group);
				}
				let len = ident_len(group);
				if len == 0 {
					return None;
				}
				let mut item = segments.clone();
				if &group[..len] != "self" {
					item.push(&group[..len]);
				}
				group = use_tree(&group[len..], &item, named)?.trim_start();
				// A rename names the item it renames.
				if let Some(renamed) = group.strip_prefix("as")
					&& renamed.starts_with(char::is_whitespace)
				{
					let renamed = renamed.trim_start();
					group = &renamed[ident_len(renamed)..];
				}
				group = group.trim_start();
				group = group.strip_prefix(',').unwrap_or(group);
			}
		}
		let len = ident_len(after);
		if len == 0 {
			// A turbofish closes the path; a glob cannot be followed.
			if after.starts_with('<') {
				break;
			}
			return None;
		}
		segments.push(&after[..len]);
		rest = &after[len..];
	}
	if segments.is_empty() {
		return None;
	}
	named.push(segments.join("::"));
	Some(rest)
}

/// What the loader scan finds in `files`: a reference [`LOADER_FORBIDDEN`]
/// names in one of [`LOADER_FILES`], a path through `calls` to an
/// item neither [`LOADER_CALLS`] nor [`LOADER_CLOSURE_PATHS`] lists,
/// a member of the closure value or its body outside
/// [`LOADER_CLOSURE_MEMBERS`], and a reference to the fault module, an import
/// included, outside [`FAULT_SITES`].
fn loader_scan(files: &BTreeMap<String, String>) -> Vec<String> {
	let mut problems = Vec::new();
	let members = closure_members();
	for (file, text) in files.iter().filter(|(file, _)| LOADER_FILES.contains(&file.as_str())) {
		let mut text = strip(text);
		for (at, path) in calls_paths(&text) {
			let line = text[..at].matches('\n').count() + 1;
			let listed = path.as_deref().is_some_and(|path| {
				LOADER_CALLS.iter().any(|(item, _)| {
					path == *item
						|| path.strip_prefix(item).is_some_and(|below| below.starts_with("::"))
				}) || LOADER_CLOSURE_PATHS.iter().any(|(item, _)| path == *item)
			});
			if !listed {
				let path = path.unwrap_or_else(|| "a path the scan cannot follow".to_owned());
				problems.push(format!("{file}:{line} names `calls::{path}`"));
			}
		}
		let mut faulting = text.clone();
		for &(_, name, _) in FAULT_SITES.iter().filter(|(f, _, _)| f == file) {
			blank_functions(&mut faulting, name);
		}
		for (at, word) in idents(&faulting) {
			if word == "fault" && faulting[at + word.len()..].trim_start().starts_with("::") {
				let line = faulting[..at].matches('\n').count() + 1;
				problems.push(format!("{file}:{line} reaches the fault module"));
			}
		}
		blank_uses(&mut text);
		for (at, word) in idents(&text) {
			let line = text[..at].matches('\n').count() + 1;
			let rest = text[at + word.len()..].trim_start();
			let before = text[..at].trim_end();
			if (before.ends_with('.') || before.ends_with("::"))
				&& members.iter().any(|member| member == word)
				&& !LOADER_CLOSURE_MEMBERS.iter().any(|(member, _)| *member == word)
			{
				problems.push(format!(
					"{file}:{line} names `{word}`, a member of the closure value that is no \
					 linked-run entry point"
				));
			}
			for &token in LOADER_FORBIDDEN {
				let hit = match (token.strip_suffix("::"), token.strip_suffix('(')) {
					(Some(module), _) => word == module && rest.starts_with("::"),
					(_, Some(call)) => word == call && rest.starts_with('('),
					_ => word == token,
				};
				if hit {
					problems.push(format!("{file}:{line} names `{token}`"));
				}
			}
		}
	}
	problems
}

#[test]
fn the_interpreter_and_the_host_facing_modules_reach_no_compiler() {
	let files = sources();
	for file in LOADER_FILES {
		assert!(files.contains_key(*file), "loader file {file} does not exist");
	}
	let problems = loader_scan(&files);
	assert!(problems.is_empty(), "{problems:#?}");
}

#[test]
fn the_loader_scan_flags_each_compiler_reference() {
	for &token in LOADER_FORBIDDEN {
		let call = match (token.strip_suffix("::"), token.strip_suffix('(')) {
			(Some(module), _) => format!("super::{module}::lower(planner, plan)"),
			(_, Some(call)) => format!("planner.{call}(body)"),
			_ => format!("{token}(x)"),
		};
		let source = format!("async fn step() {{ let _ = {call}; }}");
		let files = BTreeMap::from([("interp.rs".to_owned(), source.clone())]);
		let problems = loader_scan(&files);
		assert!(
			problems.iter().any(|p| p.contains(&format!("`{token}`"))),
			"`{source}` is not flagged for `{token}`: {problems:?}"
		);
	}
	// The fault module is reached from the host's answer to the instruction
	// alone: a call or an import anywhere else is flagged.
	for (file, source) in [
		("interp.rs", "fn step() { super::fault::serve(need, exec) }"),
		("closure.rs", "async fn run_unit() { super::fault::serve(need, exec).await }"),
		("closure.rs", "use super::fault::serve;\nasync fn link() { serve(need, exec).await }"),
		("host.rs", "async fn step() { super::fault::serve(need, exec).await }"),
	] {
		let files = BTreeMap::from([(file.to_owned(), source.to_owned())]);
		assert!(
			loader_scan(&files).iter().any(|p| p.contains("reaches the fault module")),
			"`{source}` in {file} is not flagged"
		);
	}
	let files = BTreeMap::from([(
		"closure.rs".to_owned(),
		"async fn load_unit() { super::fault::serve_unit(c, k, f, exec, txn).await }".to_owned(),
	)]);
	assert_eq!(loader_scan(&files), Vec::<String>::new());
	// A member of the closure value or its body outside the linked-run entry
	// points is flagged, called as a method or named by a path, imported or
	// not; so is a path beneath the closure value the list does not hold.
	for (source, flagged) in [
		("fn make() { CompiledClosure::of_stored(args, returns, body, captures) }", "`of_stored`"),
		(
			"use crate::exec::vm::calls::CompiledClosure;\n\
			 fn make() { CompiledClosure::of_stored(args, returns, body, captures) }",
			"`of_stored`",
		),
		(
			"fn make() { crate::exec::vm::calls::CompiledClosure::of_stored(a, r, b, c) }",
			"`calls::CompiledClosure::of_stored`",
		),
	] {
		let files = BTreeMap::from([("closure.rs".to_owned(), source.to_owned())]);
		assert!(
			loader_scan(&files).iter().any(|p| p.contains(flagged)),
			"`{source}` is not flagged for {flagged}: {:?}",
			loader_scan(&files)
		);
	}
	// The members the scan rejects are read from the closure value's source,
	// so a member added there is rejected unless it is listed.
	let members = closure_members();
	for member in ["of_stored", "invoke", "invoke_linked"] {
		assert!(members.iter().any(|m| m == member), "`{member}` not found in {members:?}");
	}
	for (member, _) in LOADER_CLOSURE_MEMBERS {
		assert!(members.iter().any(|m| m == member), "`{member}` is listed but not a member");
	}
	// The linked-run entry points are not flagged.
	let files = BTreeMap::from([(
		"host.rs".to_owned(),
		"async fn step() { compiled.invoke_linked(&ctx, values, relinking).await; \
		 compiled.invoke(&ctx, args).await; }"
			.to_owned(),
	)]);
	assert_eq!(loader_scan(&files), Vec::<String>::new());
	// A path through `calls` to an item the list does not hold is
	// flagged, imported or not, and so is a mention the scan cannot follow.
	for source in [
		"async fn step() { crate::exec::vm::calls::helpers::invoke_closure(c, a, x).await }",
		"use crate::exec::vm::calls::helpers::invoke_closure as run;",
		"fn f(e: &crate::exec::vm::calls::Unlisted) {}",
		"fn f() { crate::exec::vm::calls::CompiledClosureX::new() }",
		"use crate::exec::vm::calls::{EnteredCall, Unlisted};",
		"use crate::exec::vm::calls::{CompiledClosure, helpers::{invoke_closure as run}};",
		"use crate::exec::vm::calls::{self as expressions, EnteredCall};",
		"use crate::exec::vm::calls::*;",
		"use crate::exec::vm::calls as expressions;",
		"use crate::exec::vm::calls;",
	] {
		let files = BTreeMap::from([("host.rs".to_owned(), source.to_owned())]);
		assert!(
			loader_scan(&files).iter().any(|p| p.contains("names `calls::")),
			"`{source}` is not flagged"
		);
	}
	// A listed item, and anything beneath it, is not.
	let files = BTreeMap::from([(
		"host.rs".to_owned(),
		"fn f(v: &crate::exec::vm::calls::CompiledClosure) { \
		 crate::exec::vm::calls::CompiledClosure::planned(a, b, c); \
		 let _ = crate::exec::vm::subquery::subquery_context::<()>; }\n\
		 use crate::exec::vm::calls::CompiledClosure as Value;"
			.to_owned(),
	)]);
	assert_eq!(loader_scan(&files), Vec::<String>::new());
	// A listed path, a file the scan does not hold, imports and comments are
	// not compiler references.
	let files = BTreeMap::from([
		("interp.rs".to_owned(), "fn f(c: &crate::exec::EvalContext) {}".to_owned()),
		("fault.rs".to_owned(), "fn f() { Planner::new() }".to_owned()),
		(
			"services.rs".to_owned(),
			"use crate::exec::planner::Planner;
fn run() { /* Planner */ }"
				.to_owned(),
		),
	]);
	assert_eq!(loader_scan(&files), Vec::<String>::new());
}

/// The files that name `ExecOperator`: the adapter a
/// program runs behind and the one a request's statement runs behind, the
/// planner entry that hands it over, `EXPLAIN`, which renders a plan around
/// it, and the compile cache's keeping of the statements reached at run time
/// through an expression a program did not compile.
const OPERATOR_FILES: &[&str] =
	&["explain.rs", "host.rs", "mod.rs", "request.rs", "serve.rs", "units.rs"];

/// The files of `files` naming `ExecOperator` outside [`OPERATOR_FILES`].
fn operator_scan(files: &BTreeMap<String, String>) -> Vec<String> {
	files
		.iter()
		.filter(|(file, _)| !OPERATOR_FILES.contains(&file.as_str()))
		.filter_map(|(file, text)| {
			let text = strip(text);
			let at = idents(&text).into_iter().find(|&(_, w)| w == "ExecOperator")?.0;
			let line = text[..at].matches('\n').count() + 1;
			Some(format!("{file}:{line} names `ExecOperator`"))
		})
		.collect()
}

#[test]
fn only_the_adapter_names_the_operator_tree() {
	let problems = operator_scan(&sources());
	assert!(problems.is_empty(), "{problems:#?}");
	let files = BTreeMap::from([
		("nested.rs".to_owned(), "fn f(op: Arc<dyn ExecOperator>) {}".to_owned()),
		("host.rs".to_owned(), "fn f(op: Arc<dyn ExecOperator>) {}".to_owned()),
	]);
	assert_eq!(operator_scan(&files), vec!["nested.rs:1 names `ExecOperator`".to_owned()]);
}

/// A closure value: the one value a program never holds.
fn closure() -> crate::val::Value {
	crate::val::Value::Closure(Box::new(crate::val::Closure::Expr {
		args: Vec::new(),
		returns: None,
		body: crate::expr::Expr::Literal(crate::expr::Literal::None),
		captures: Default::default(),
	}))
}

#[test]
fn a_program_refuses_a_closure_value_as_an_internal_error() {
	use super::NotCoveredKind;
	use super::plain::ValueData;
	use crate::exec::index::access_path::{BTreeAccess, BitmapPlan};
	use crate::val::{Array, Object, RecordId, RecordIdKey, Value};
	let array = |items: Vec<Value>| Value::Array(Array(items));
	let in_object = Value::Object(Object::from(vec![("f".to_owned(), closure())]));
	let in_key = Value::RecordId(RecordId {
		table: "t".into(),
		key: RecordIdKey::from(Array(vec![Value::from(1), closure()])),
	});
	let in_range = Value::Range(Box::new(crate::val::Range {
		start: std::ops::Bound::Included(closure()),
		end: std::ops::Bound::Unbounded,
	}));
	for value in
		[closure(), array(vec![Value::None, array(vec![closure()])]), in_object, in_key, in_range]
	{
		let refused = ValueData::new(value.clone(), "a constant")
			.expect_err(&format!("{value:?} holds a closure"));
		assert_eq!(
			refused.kind,
			NotCoveredKind::Internal,
			"{value:?} is declined rather than refused"
		);
		// An internal refusal fails the statement as an internal error.
		let error = super::declined(refused);
		assert!(error.to_string().contains("closure"), "{error}");
	}
	assert!(ValueData::new(vec![Value::from(1), closure()], "the constant pool").is_err());
	assert!(ValueData::new(BTreeAccess::literal_equality(closure()), "an index access").is_err());
	let compound = BTreeAccess::Compound {
		prefix: vec![Value::from(1).into()],
		range: Some((crate::expr::BinaryOperator::MoreThan, closure().into())),
		upper: None,
	};
	assert!(ValueData::new(compound, "an index access").is_err());
	let graph = |key: RecordIdKey| BitmapPlan::Graph {
		source: RecordId {
			table: "t".into(),
			key,
		},
		direction: crate::expr::Dir::Out,
		edge_tables: Vec::new(),
	};
	let bitmap = BitmapPlan::And(vec![
		BitmapPlan::Or(Vec::new()),
		BitmapPlan::AndNot {
			base: Box::new(graph(RecordIdKey::from(1))),
			subtract: Box::new(graph(RecordIdKey::from(Object::from(vec![(
				"f".to_owned(),
				closure(),
			)])))),
		},
	]);
	assert!(ValueData::new(bitmap, "a bitmap plan").is_err());
	assert!(ValueData::new(graph(RecordIdKey::from(1)), "a bitmap plan").is_ok());
	// Plain values are held as they are.
	let plain = array(vec![Value::from(1), Value::Object(Object::default()), Value::None]);
	assert_eq!(*ValueData::new(plain.clone(), "a constant").expect("plain data"), plain);
}

#[test]
fn every_audited_exception_is_still_needed() {
	let files = sources();
	let stripped: BTreeMap<String, String> =
		files.iter().map(|(name, text)| (name.clone(), strip(text))).collect();
	let decls = declarations(&stripped);
	let reach = reachable(&decls, "Program");
	for audited in AUDITED {
		assert!(!audited.reason.is_empty(), "`{}` is audited with no reason", audited.name);
		let decl = decls
			.get(audited.name)
			.into_iter()
			.flatten()
			.find(|decl| decl.file == audited.file)
			.unwrap_or_else(|| panic!("`{}` is not declared in {}", audited.name, audited.file));
		assert!(reach.contains(audited.name), "`{}` is audited but unreached", audited.name);
		let held = forbidden_in(decl);
		for token in audited.tokens {
			assert!(held.contains(token), "`{}` no longer holds `{token}`", audited.name);
		}
		let text = &stripped[audited.file];
		let squeezed: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
		if audited.partial_eq {
			assert!(
				squeezed.contains(&format!("impl PartialEq for {}", audited.name)),
				"`{}` no longer implements `PartialEq` by hand",
				audited.name
			);
		}
		if audited.unwind_safe {
			assert!(
				squeezed.contains(&format!("RefUnwindSafe for {}", audited.name)),
				"`{}` no longer implements the unwind-safety traits by hand",
				audited.name
			);
		}
	}
	for (name, reason) in CARRIED_CATALOG {
		assert!(!reason.is_empty(), "`{name}` is carried with no reason");
		assert!(
			reach.iter().any(|reached| {
				decls[reached].iter().any(|decl| decl.type_idents().contains(name))
			}),
			"catalog `{name}` is listed as carried but no program type carries it"
		);
	}
	for (file, name, reason) in PLAN_TIME_FUNCTIONS {
		assert!(!reason.is_empty(), "`{name}` is listed with no reason");
		let text = stripped.get(*file).unwrap_or_else(|| panic!("{file} is not a source file"));
		assert!(
			idents(text).windows(2).any(|w| w[0].1 == "fn" && w[1].1 == *name),
			"plan-time function `{name}` is not declared in {file}"
		);
	}
	for file in PLAN_TIME_FILES {
		assert!(stripped.contains_key(*file), "plan-time file {file} does not exist");
	}
	let named: BTreeSet<String> = stripped
		.iter()
		.filter(|(file, _)| LOADER_FILES.contains(&file.as_str()))
		.flat_map(|(_, text)| calls_paths(text))
		.filter_map(|(_, path)| path)
		.collect();
	for (item, reason) in LOADER_CALLS {
		assert!(!reason.is_empty(), "`{item}` is listed with no reason");
		assert!(
			named.iter().any(|path| path == item
				|| path.strip_prefix(item).is_some_and(|below| below.starts_with("::"))),
			"`calls::{item}` is listed but no loader file names it"
		);
	}
	for (file, name, reason) in FAULT_SITES {
		assert!(!reason.is_empty(), "`{name}` is listed with no reason");
		let text = stripped.get(*file).unwrap_or_else(|| panic!("{file} is not a source file"));
		let ids = idents(text);
		let at = ids
			.windows(2)
			.find(|w| w[0].1 == "fn" && w[1].1 == *name)
			.map(|w| w[0].0)
			.unwrap_or_else(|| panic!("fault site `{name}` is not declared in {file}"));
		let body = &text[at..item_end(text, at)];
		assert!(body.contains("fault::serve"), "fault site `{name}` no longer serves a fault");
	}
}

/// Host kernels consume values and compiled units and hold no expression-tree evaluator.
#[test]
fn host_services_contain_no_tree_evaluator() {
	let mut files = sources();
	let exec = vm_dir().parent().expect("VM belongs to exec").to_path_buf();
	for file in [
		"operators/fetch.rs",
		"operators/recursion.rs",
		"operators/recursion/common.rs",
		"operators/recursion/default.rs",
		"operators/recursion/collect.rs",
		"operators/recursion/path.rs",
		"operators/recursion/shortest.rs",
		"operators/recursion/repeat.rs",
	] {
		files.insert(file.to_owned(), std::fs::read_to_string(exec.join(file)).expect(file));
	}
	let mut problems = Vec::new();
	let calls = strip(files.get("calls.rs").expect("native call services"));
	for (file, text) in files.iter().filter(|(file, _)| {
		if let Some(module) = file.strip_prefix("calls/").and_then(|file| file.strip_suffix(".rs"))
			&& !calls.contains(&format!("mod {module};"))
		{
			return false;
		}
		file.starts_with("calls/")
			|| file.starts_with("access/")
			|| file.starts_with("operators/")
			|| matches!(file.as_str(), "matches.rs" | "subquery.rs" | "degree.rs")
	}) {
		let text = strip(text);
		for (at, word) in idents(&text) {
			if matches!(
				word,
				"Clause" | "evaluate_args" | "evaluate_physical_path" | "plan_logical"
			) {
				let line = text[..at].matches('\n').count() + 1;
				problems.push(format!("{file}:{line} reaches tree evaluator `{word}`"));
			}
		}
	}
	assert!(problems.is_empty(), "{problems:#?}");
}
