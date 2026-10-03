//! Canonical structural projection $\pi$ from surrealdb-pgsql AST into pg_query JSON
//! representation.
//!
//! Provides a test-only structural projection that maps typed PostgreSQL AST nodes
//! into canonical JSON objects matching PostgreSQL's raw parse tree schema.

use serde_json::{Map, Value, json};
use surrealdb_pgsql::ast::*;

/// A trait for types that can be structurally projected to canonical `pg_query` JSON.
pub trait ToPgQueryJson {
	/// Project the AST node into a canonical `pg_query` JSON value.
	fn to_pg_query_json(&self) -> Value;
}

fn pg_node(name: &str, body: Value) -> Value {
	let mut map = Map::new();
	map.insert(name.to_string(), body);
	Value::Object(map)
}

fn pg_str(s: &str) -> Value {
	json!({
		"String": {
			"sval": s
		}
	})
}

fn pg_int(i: i64) -> Value {
	json!({
		"Integer": {
			"ival": i
		}
	})
}

fn pg_bool(b: bool) -> Value {
	json!({
		"Boolean": {
			"boolval": b
		}
	})
}

impl ToPgQueryJson for Statement {
	fn to_pg_query_json(&self) -> Value {
		let stmt_node = match self {
			Self::Select(s) => s.to_pg_query_json(),
			Self::Insert(s) => s.to_pg_query_json(),
			Self::Update(s) => s.to_pg_query_json(),
			Self::Delete(s) => s.to_pg_query_json(),
			Self::CreateTable(s) => s.to_pg_query_json(),
			Self::Drop(s) => s.to_pg_query_json(),
			Self::Transaction(s) => s.to_pg_query_json(),
			Self::Set(s) => s.to_pg_query_json(),
			Self::Show(s) => s.to_pg_query_json(),
			Self::Reset(s) => s.to_pg_query_json(),
			Self::CreateIndex(s) => s.to_pg_query_json(),
			Self::CreateView(s) => s.to_pg_query_json(),
			Self::CreateSchema(s) => s.to_pg_query_json(),
			Self::AlterTable(s) => s.to_pg_query_json(),
			Self::Prepare(s) => s.to_pg_query_json(),
			Self::Execute(s) => s.to_pg_query_json(),
			Self::Deallocate(s) => s.to_pg_query_json(),
			Self::Explain(s) => s.to_pg_query_json(),
			Self::CreatePropertyGraph(_) | Self::DropPropertyGraph(_) => {
				json!({
					"PropertyGraphStmt": {}
				})
			}
			Self::Empty => Value::Null,
		};

		json!({
			"RawStmt": {
				"stmt": stmt_node,
				"stmt_location": 0,
				"stmt_len": 0
			}
		})
	}
}

impl ToPgQueryJson for SelectStmt {
	fn to_pg_query_json(&self) -> Value {
		let mut obj = Map::new();

		if let Some(d) = &self.distinct {
			match d {
				DistinctClause::All => {
					obj.insert("distinctClause".to_string(), json!([Value::Null]));
				}
				DistinctClause::On(exprs) => {
					let list: Vec<Value> = exprs.iter().map(|e| e.to_pg_query_json()).collect();
					obj.insert("distinctClause".to_string(), Value::Array(list));
				}
			}
		}

		if !self.targets.is_empty() {
			let list: Vec<Value> = self.targets.iter().map(|t| t.to_pg_query_json()).collect();
			obj.insert("targetList".to_string(), Value::Array(list));
		}

		if !self.from.is_empty() {
			let list: Vec<Value> = self.from.iter().map(|f| f.to_pg_query_json()).collect();
			obj.insert("fromClause".to_string(), Value::Array(list));
		}

		if let Some(w) = &self.where_clause {
			obj.insert("whereClause".to_string(), w.to_pg_query_json());
		}

		if !self.group_by.is_empty() {
			let list: Vec<Value> = self.group_by.iter().map(|g| g.to_pg_query_json()).collect();
			obj.insert("groupClause".to_string(), Value::Array(list));
		}

		if let Some(h) = &self.having {
			obj.insert("havingClause".to_string(), h.to_pg_query_json());
		}

		if !self.window.is_empty() {
			let list: Vec<Value> = self.window.iter().map(|w| w.to_pg_query_json()).collect();
			obj.insert("windowClause".to_string(), Value::Array(list));
		}

		if !self.order_by.is_empty() {
			let list: Vec<Value> = self.order_by.iter().map(|o| o.to_pg_query_json()).collect();
			obj.insert("sortClause".to_string(), Value::Array(list));
		}

		if let Some(lim) = &self.limit {
			obj.insert("limitCount".to_string(), lim.to_pg_query_json());
		}

		if let Some(off) = &self.offset {
			obj.insert("limitOffset".to_string(), off.to_pg_query_json());
		}

		if !self.locking.is_empty() {
			let list: Vec<Value> = self.locking.iter().map(|l| l.to_pg_query_json()).collect();
			obj.insert("lockingClause".to_string(), Value::Array(list));
		}

		if let Some(with) = &self.with {
			obj.insert("withClause".to_string(), with.to_pg_query_json());
		}

		if let Some(set_op) = &self.set_op {
			let op_str = match set_op.op {
				SetOpKind::Union => "SETOP_UNION",
				SetOpKind::Intersect => "SETOP_INTERSECT",
				SetOpKind::Except => "SETOP_EXCEPT",
			};
			obj.insert("op".to_string(), json!(op_str));
			obj.insert("all".to_string(), json!(set_op.all));
			obj.insert("rarg".to_string(), set_op.right.to_pg_query_json());
		}

		pg_node("SelectStmt", Value::Object(obj))
	}
}

impl ToPgQueryJson for ResTarget {
	fn to_pg_query_json(&self) -> Value {
		let mut obj = Map::new();
		if let Some(name) = &self.alias {
			obj.insert("name".to_string(), json!(name));
		}
		obj.insert("val".to_string(), self.expr.to_pg_query_json());
		pg_node("ResTarget", Value::Object(obj))
	}
}

impl ToPgQueryJson for TableRef {
	fn to_pg_query_json(&self) -> Value {
		match self {
			Self::Table {
				name,
				alias,
				..
			} => {
				let mut obj = Map::new();
				if name.parts.len() > 1 {
					obj.insert("schemaname".to_string(), json!(name.parts[0]));
					obj.insert("relname".to_string(), json!(name.parts[1]));
				} else if !name.parts.is_empty() {
					obj.insert("relname".to_string(), json!(name.parts[0]));
				}
				obj.insert("inh".to_string(), json!(true));
				obj.insert("relpersistence".to_string(), json!("p"));
				if let Some(al) = alias {
					let mut alias_obj = Map::new();
					alias_obj.insert("aliasname".to_string(), json!(al.name));
					if !al.columns.is_empty() {
						let cols: Vec<Value> = al.columns.iter().map(|c| pg_str(c)).collect();
						alias_obj.insert("colnames".to_string(), Value::Array(cols));
					}
					obj.insert("alias".to_string(), pg_node("Alias", Value::Object(alias_obj)));
				}
				pg_node("RangeVar", Value::Object(obj))
			}
			Self::Join {
				left,
				join_type,
				right,
				condition,
				..
			} => {
				let mut obj = Map::new();
				let jtype = match join_type {
					JoinType::Inner => "JOIN_INNER",
					JoinType::Left => "JOIN_LEFT",
					JoinType::Right => "JOIN_RIGHT",
					JoinType::Full => "JOIN_FULL",
					JoinType::Cross => "JOIN_CROSS",
				};
				obj.insert("jointype".to_string(), json!(jtype));
				obj.insert("larg".to_string(), left.to_pg_query_json());
				obj.insert("rarg".to_string(), right.to_pg_query_json());
				if let Some(cond) = condition {
					match cond {
						JoinCondition::On(expr) => {
							obj.insert("quals".to_string(), expr.to_pg_query_json());
						}
						JoinCondition::Using(cols) => {
							let col_list: Vec<Value> = cols.iter().map(|c| pg_str(c)).collect();
							obj.insert("usingClause".to_string(), Value::Array(col_list));
						}
					}
				}
				pg_node("JoinExpr", Value::Object(obj))
			}
			Self::Subquery {
				query,
				alias,
				..
			} => {
				let mut obj = Map::new();
				obj.insert("subquery".to_string(), query.to_pg_query_json());
				let mut alias_obj = Map::new();
				alias_obj.insert("aliasname".to_string(), json!(alias.name));
				if !alias.columns.is_empty() {
					let cols: Vec<Value> = alias.columns.iter().map(|c| pg_str(c)).collect();
					alias_obj.insert("colnames".to_string(), Value::Array(cols));
				}
				obj.insert("alias".to_string(), pg_node("Alias", Value::Object(alias_obj)));
				pg_node("RangeSubselect", Value::Object(obj))
			}
			Self::GraphTable {
				..
			}
			| Self::JsonTable(_) => pg_node("RangeTableFunc", Value::Object(serde_json::Map::new())),
		}
	}
}

impl ToPgQueryJson for Expr {
	fn to_pg_query_json(&self) -> Value {
		match self {
			Self::Literal {
				lit,
				..
			} => lit.to_pg_query_json(),
			Self::ColumnRef(col) => {
				let fields: Vec<Value> = col.fields.iter().map(|f| pg_str(f)).collect();
				pg_node("ColumnRef", json!({ "fields": fields }))
			}
			Self::Param {
				id,
				..
			} => pg_node("ParamRef", json!({ "number": id })),
			Self::UnaryOp {
				op,
				expr,
				..
			} => match op {
				UnaryOperator::Not => pg_node(
					"BoolExpr",
					json!({
						"boolop": "NOT_EXPR",
						"args": [ expr.to_pg_query_json() ]
					}),
				),
				UnaryOperator::Plus => pg_node(
					"A_Expr",
					json!({
						"kind": "AEXPR_OP",
						"name": [ pg_str("+") ],
						"rexpr": expr.to_pg_query_json()
					}),
				),
				UnaryOperator::Minus => pg_node(
					"A_Expr",
					json!({
						"kind": "AEXPR_OP",
						"name": [ pg_str("-") ],
						"rexpr": expr.to_pg_query_json()
					}),
				),
				UnaryOperator::BitNot => pg_node(
					"A_Expr",
					json!({
						"kind": "AEXPR_OP",
						"name": [ pg_str("~") ],
						"rexpr": expr.to_pg_query_json()
					}),
				),
				UnaryOperator::Custom(s) => pg_node(
					"A_Expr",
					json!({
						"kind": "AEXPR_OP",
						"name": [ pg_str(s) ],
						"rexpr": expr.to_pg_query_json()
					}),
				),
			},
			Self::BinaryOp {
				left,
				op,
				right,
				..
			} => match op {
				BinaryOperator::And => pg_node(
					"BoolExpr",
					json!({
						"boolop": "AND_EXPR",
						"args": [ left.to_pg_query_json(), right.to_pg_query_json() ]
					}),
				),
				BinaryOperator::Or => pg_node(
					"BoolExpr",
					json!({
						"boolop": "OR_EXPR",
						"args": [ left.to_pg_query_json(), right.to_pg_query_json() ]
					}),
				),
				_ => {
					let op_str = match op {
						BinaryOperator::Add => "+",
						BinaryOperator::Sub => "-",
						BinaryOperator::Mul => "*",
						BinaryOperator::Div => "/",
						BinaryOperator::Mod => "%",
						BinaryOperator::Exp => "^",
						BinaryOperator::Eq => "=",
						BinaryOperator::NotEq => "<>",
						BinaryOperator::Lt => "<",
						BinaryOperator::LtEq => "<=",
						BinaryOperator::Gt => ">",
						BinaryOperator::GtEq => ">=",
						BinaryOperator::Concat => "||",
						BinaryOperator::BitAnd => "&",
						BinaryOperator::BitOr => "|",
						BinaryOperator::BitXor => "#",
						BinaryOperator::ShiftLeft => "<<",
						BinaryOperator::ShiftRight => ">>",
						BinaryOperator::RegexMatch => "~",
						BinaryOperator::RegexMatchCi => "~*",
						BinaryOperator::RegexNotMatch => "!~",
						BinaryOperator::RegexNotMatchCi => "!~*",
						BinaryOperator::Like => "~~",
						BinaryOperator::ILike => "~~*",
						BinaryOperator::NotLike => "!~~",
						BinaryOperator::NotILike => "!~~*",
						BinaryOperator::SimilarTo => "~",
						BinaryOperator::NotSimilarTo => "!~",
						BinaryOperator::Arrow => "->",
						BinaryOperator::LongArrow => "->>",
						BinaryOperator::HashArrow => "#>",
						BinaryOperator::HashLongArrow => "#>>",
						BinaryOperator::Contains => "@>",
						BinaryOperator::ContainedBy => "<@",
						BinaryOperator::Custom(s) => s.as_str(),
						BinaryOperator::And | BinaryOperator::Or => unreachable!(),
					};
					pg_node(
						"A_Expr",
						json!({
							"kind": "AEXPR_OP",
							"name": [ pg_str(op_str) ],
							"lexpr": left.to_pg_query_json(),
							"rexpr": right.to_pg_query_json()
						}),
					)
				}
			},
			Self::TypeCast {
				expr,
				data_type,
				..
			} => pg_node(
				"TypeCast",
				json!({
					"arg": expr.to_pg_query_json(),
					"typeName": data_type.to_pg_query_json()
				}),
			),
			Self::FunctionCall {
				name,
				args,
				distinct,
				over,
				..
			} => {
				let mut obj = Map::new();
				let name_parts: Vec<Value> = name.parts.iter().map(|p| pg_str(p)).collect();
				obj.insert("funcname".to_string(), Value::Array(name_parts));

				if !args.is_empty() {
					let arg_list: Vec<Value> = args.iter().map(|a| a.to_pg_query_json()).collect();
					obj.insert("args".to_string(), Value::Array(arg_list));
				}
				if *distinct {
					obj.insert("agg_distinct".to_string(), json!(true));
				}
				if let Some(o) = over {
					obj.insert("over".to_string(), o.to_pg_query_json());
				}
				pg_node("FuncCall", Value::Object(obj))
			}
			Self::Subscript {
				expr,
				index,
				..
			} => pg_node(
				"A_Indirection",
				json!({
					"arg": expr.to_pg_query_json(),
					"indirection": [
						pg_node("A_Indices", json!({
							"uidx": index.to_pg_query_json()
						}))
					]
				}),
			),
			Self::Slice {
				expr,
				lower,
				upper,
				..
			} => {
				let mut ind_obj = Map::new();
				ind_obj.insert("is_slice".to_string(), json!(true));
				if let Some(l) = lower {
					ind_obj.insert("lidx".to_string(), l.to_pg_query_json());
				}
				if let Some(u) = upper {
					ind_obj.insert("uidx".to_string(), u.to_pg_query_json());
				}
				pg_node(
					"A_Indirection",
					json!({
						"arg": expr.to_pg_query_json(),
						"indirection": [ pg_node("A_Indices", Value::Object(ind_obj)) ]
					}),
				)
			}
			Self::Between {
				expr,
				low,
				high,
				negated,
				..
			} => {
				let kind = if *negated {
					"AEXPR_NOT_BETWEEN"
				} else {
					"AEXPR_BETWEEN"
				};
				pg_node(
					"A_Expr",
					json!({
						"kind": kind,
						"name": [ pg_str("between") ],
						"lexpr": expr.to_pg_query_json(),
						"rexpr": pg_node("A_ArrayExpr", json!({
							"elements": [ low.to_pg_query_json(), high.to_pg_query_json() ]
						}))
					}),
				)
			}
			Self::InList {
				expr,
				list,
				negated,
				..
			} => {
				let items: Vec<Value> = list.iter().map(|e| e.to_pg_query_json()).collect();
				pg_node(
					"A_Expr",
					json!({
						"kind": "AEXPR_IN",
						"name": [ pg_str(if *negated { "<>" } else { "=" }) ],
						"lexpr": expr.to_pg_query_json(),
						"rexpr": pg_node("A_ArrayExpr", json!({ "elements": items }))
					}),
				)
			}
			Self::IsNull {
				expr,
				negated,
				..
			} => pg_node(
				"NullTest",
				json!({
					"arg": expr.to_pg_query_json(),
					"nulltesttype": if *negated { "IS_NOT_NULL" } else { "IS_NULL" }
				}),
			),
			Self::IsTrue {
				expr,
				negated,
				..
			} => pg_node(
				"BooleanTest",
				json!({
					"arg": expr.to_pg_query_json(),
					"booltesttype": if *negated { "IS_NOT_TRUE" } else { "IS_TRUE" }
				}),
			),
			Self::IsFalse {
				expr,
				negated,
				..
			} => pg_node(
				"BooleanTest",
				json!({
					"arg": expr.to_pg_query_json(),
					"booltesttype": if *negated { "IS_NOT_FALSE" } else { "IS_FALSE" }
				}),
			),
			Self::IsUnknown {
				expr,
				negated,
				..
			} => pg_node(
				"BooleanTest",
				json!({
					"arg": expr.to_pg_query_json(),
					"booltesttype": if *negated { "IS_NOT_UNKNOWN" } else { "IS_UNKNOWN" }
				}),
			),
			Self::IsDistinctFrom {
				left,
				right,
				negated,
				..
			} => pg_node(
				"A_Expr",
				json!({
					"kind": if *negated { "AEXPR_NOT_DISTINCT" } else { "AEXPR_DISTINCT" },
					"name": [ pg_str("=") ],
					"lexpr": left.to_pg_query_json(),
					"rexpr": right.to_pg_query_json()
				}),
			),
			Self::Case {
				operand,
				branches,
				else_expr,
				..
			} => {
				let mut obj = Map::new();
				if let Some(arg) = operand {
					obj.insert("arg".to_string(), arg.to_pg_query_json());
				}
				let whens: Vec<Value> = branches
					.iter()
					.map(|b| {
						pg_node(
							"CaseWhen",
							json!({
								"expr": b.when.to_pg_query_json(),
								"result": b.then.to_pg_query_json()
							}),
						)
					})
					.collect();
				obj.insert("args".to_string(), Value::Array(whens));
				if let Some(def) = else_expr {
					obj.insert("defresult".to_string(), def.to_pg_query_json());
				}
				pg_node("CaseExpr", Value::Object(obj))
			}
			Self::Collate {
				expr,
				collation,
				..
			} => {
				let coll_parts: Vec<Value> = collation.parts.iter().map(|p| pg_str(p)).collect();
				pg_node(
					"CollateClause",
					json!({
						"arg": expr.to_pg_query_json(),
						"collname": coll_parts
					}),
				)
			}
			Self::ArrayConstructor {
				elements,
				..
			} => {
				let items: Vec<Value> = elements.iter().map(|e| e.to_pg_query_json()).collect();
				pg_node("A_ArrayExpr", json!({ "elements": items }))
			}
			Self::Row {
				fields,
				..
			} => {
				let items: Vec<Value> = fields.iter().map(|e| e.to_pg_query_json()).collect();
				pg_node("RowExpr", json!({ "args": items }))
			}
			Self::Grouping {
				expr,
				..
			} => expr.to_pg_query_json(),
			Self::IsBound {
				..
			}
			| Self::JsonObject {
				..
			}
			| Self::JsonArray {
				..
			}
			| Self::JsonExists {
				..
			}
			| Self::JsonValue {
				..
			}
			| Self::JsonQuery {
				..
			} => pg_node("JsonExpr", Value::Object(serde_json::Map::new())),
		}
	}
}

impl ToPgQueryJson for Literal {
	fn to_pg_query_json(&self) -> Value {
		match self {
			Self::Integer(i) => pg_node("A_Const", json!({ "ival": pg_int(*i)["Integer"] })),
			Self::Float(f) => pg_node("A_Const", json!({ "fval": { "fval": f.to_string() } })),
			Self::String(s) => pg_node("A_Const", json!({ "sval": pg_str(s)["String"] })),
			Self::Boolean(b) => pg_node("A_Const", json!({ "boolval": pg_bool(*b)["Boolean"] })),
			Self::Null => pg_node("A_Const", json!({ "isnull": true })),
			Self::BitString {
				value,
				..
			} => pg_node("A_Const", json!({ "bsval": { "bsval": value } })),
		}
	}
}

impl ToPgQueryJson for TypeName {
	fn to_pg_query_json(&self) -> Value {
		let names: Vec<Value> = self.name.parts.iter().map(|p| pg_str(p)).collect();
		pg_node("TypeName", json!({ "names": names }))
	}
}

impl ToPgQueryJson for OrderByItem {
	fn to_pg_query_json(&self) -> Value {
		let dir = match self.direction {
			Some(OrderDirection::Asc) => "SORTBY_ASC",
			Some(OrderDirection::Desc) => "SORTBY_DESC",
			None => "SORTBY_DEFAULT",
		};
		let nulls = match self.nulls {
			Some(NullsOrder::First) => "SORTBY_NULLS_FIRST",
			Some(NullsOrder::Last) => "SORTBY_NULLS_LAST",
			None => "SORTBY_NULLS_DEFAULT",
		};
		pg_node(
			"SortBy",
			json!({
				"node": self.expr.to_pg_query_json(),
				"sortby_dir": dir,
				"sortby_nulls": nulls
			}),
		)
	}
}

impl ToPgQueryJson for WindowDef {
	fn to_pg_query_json(&self) -> Value {
		let mut obj = Map::new();
		obj.insert("name".to_string(), json!(self.name));
		let spec_val = self.spec.to_pg_query_json();
		if let Some(spec_obj) = spec_val.get("WindowDef").and_then(|v| v.as_object()) {
			for (k, v) in spec_obj {
				obj.insert(k.clone(), v.clone());
			}
		}
		pg_node("WindowDef", Value::Object(obj))
	}
}

impl ToPgQueryJson for WindowSpec {
	fn to_pg_query_json(&self) -> Value {
		let mut obj = Map::new();
		if let Some(name) = &self.window_name {
			obj.insert("refname".to_string(), json!(name));
		}
		if !self.partition_by.is_empty() {
			let parts: Vec<Value> =
				self.partition_by.iter().map(|p| p.to_pg_query_json()).collect();
			obj.insert("partitionClause".to_string(), Value::Array(parts));
		}
		if !self.order_by.is_empty() {
			let orders: Vec<Value> = self.order_by.iter().map(|o| o.to_pg_query_json()).collect();
			obj.insert("orderClause".to_string(), Value::Array(orders));
		}
		pg_node("WindowDef", Value::Object(obj))
	}
}

impl ToPgQueryJson for WithClause {
	fn to_pg_query_json(&self) -> Value {
		let ctes: Vec<Value> = self.ctes.iter().map(|c| c.to_pg_query_json()).collect();
		pg_node(
			"WithClause",
			json!({
				"recursive": self.recursive,
				"ctes": ctes
			}),
		)
	}
}

impl ToPgQueryJson for CommonTableExpr {
	fn to_pg_query_json(&self) -> Value {
		let mut obj = Map::new();
		obj.insert("ctename".to_string(), json!(self.name));
		if !self.columns.is_empty() {
			let cols: Vec<Value> = self.columns.iter().map(|c| pg_str(c)).collect();
			obj.insert("aliascolnames".to_string(), Value::Array(cols));
		}
		let mat = match self.materialized {
			Some(true) => "CTEMaterializeAlways",
			Some(false) => "CTEMaterializeNever",
			None => "CTEMaterializeDefault",
		};
		obj.insert("ctematerialized".to_string(), json!(mat));
		obj.insert("ctequery".to_string(), self.query.to_pg_query_json());
		pg_node("CommonTableExpr", Value::Object(obj))
	}
}

impl ToPgQueryJson for LockingClause {
	fn to_pg_query_json(&self) -> Value {
		let strength = match self.strength {
			LockingStrength::Update => "LCS_FORUPDATE",
			LockingStrength::NoKeyUpdate => "LCS_FORNOKEYUPDATE",
			LockingStrength::Share => "LCS_FORSHARE",
			LockingStrength::KeyShare => "LCS_FORKEYSHARE",
		};
		let wait = match self.wait_policy {
			LockingWaitPolicy::Block => "LockWaitBlock",
			LockingWaitPolicy::NoWait => "LockWaitError",
			LockingWaitPolicy::SkipLocked => "LockWaitSkip",
		};
		let mut obj = Map::new();
		obj.insert("strength".to_string(), json!(strength));
		obj.insert("waitPolicy".to_string(), json!(wait));
		if !self.tables.is_empty() {
			let rels: Vec<Value> = self
				.tables
				.iter()
				.map(|t| {
					TableRef::Table {
						name: t.clone(),
						alias: None,
						span: t.span,
					}
					.to_pg_query_json()
				})
				.collect();
			obj.insert("lockedRels".to_string(), Value::Array(rels));
		}
		pg_node("LockingClause", Value::Object(obj))
	}
}

impl ToPgQueryJson for InsertStmt {
	fn to_pg_query_json(&self) -> Value {
		let mut obj = Map::new();
		obj.insert(
			"relation".to_string(),
			TableRef::Table {
				name: self.relation.clone(),
				alias: None,
				span: self.relation.span,
			}
			.to_pg_query_json(),
		);

		if !self.columns.is_empty() {
			let cols: Vec<Value> =
				self.columns.iter().map(|c| pg_node("ResTarget", json!({ "name": c }))).collect();
			obj.insert("cols".to_string(), Value::Array(cols));
		}

		if let Some(vals) = &self.values {
			let rows: Vec<Value> = vals
				.iter()
				.map(|row| {
					let items: Vec<Value> = row.iter().map(|e| e.to_pg_query_json()).collect();
					Value::Array(items)
				})
				.collect();
			obj.insert(
				"selectStmt".to_string(),
				pg_node("SelectStmt", json!({ "valuesLists": rows })),
			);
		} else if let Some(q) = &self.query {
			obj.insert("selectStmt".to_string(), q.to_pg_query_json());
		}

		if let Some(on_conflict) = &self.on_conflict {
			obj.insert("onConflictClause".to_string(), on_conflict.to_pg_query_json());
		}

		if let Some(ret) = &self.returning {
			let ret_list: Vec<Value> = ret.iter().map(|t| t.to_pg_query_json()).collect();
			obj.insert("returningList".to_string(), Value::Array(ret_list));
		}

		if let Some(with) = &self.with {
			obj.insert("withClause".to_string(), with.to_pg_query_json());
		}

		pg_node("InsertStmt", Value::Object(obj))
	}
}

impl ToPgQueryJson for OnConflictClause {
	fn to_pg_query_json(&self) -> Value {
		let mut obj = Map::new();
		match &self.action {
			OnConflictAction::DoNothing => {
				obj.insert("action".to_string(), json!("ONCONFLICT_NOTHING"));
			}
			OnConflictAction::DoUpdate(sets) => {
				obj.insert("action".to_string(), json!("ONCONFLICT_UPDATE"));
				let targets: Vec<Value> = sets
					.iter()
					.map(|s| {
						pg_node(
							"ResTarget",
							json!({
								"name": s.column,
								"val": s.value.to_pg_query_json()
							}),
						)
					})
					.collect();
				obj.insert("targetList".to_string(), Value::Array(targets));
			}
		}
		if let Some(targets) = &self.target {
			let infer_elems: Vec<Value> = targets.iter().map(|t| pg_str(t)).collect();
			obj.insert(
				"infer".to_string(),
				pg_node("InferClause", json!({ "indexElems": infer_elems })),
			);
		}
		pg_node("OnConflictClause", Value::Object(obj))
	}
}

impl ToPgQueryJson for UpdateStmt {
	fn to_pg_query_json(&self) -> Value {
		let mut obj = Map::new();
		obj.insert(
			"relation".to_string(),
			TableRef::Table {
				name: self.relation.clone(),
				alias: None,
				span: self.relation.span,
			}
			.to_pg_query_json(),
		);

		let targets: Vec<Value> = self
			.assignments
			.iter()
			.map(|a| {
				pg_node(
					"ResTarget",
					json!({
						"name": a.column,
						"val": a.value.to_pg_query_json()
					}),
				)
			})
			.collect();
		obj.insert("targetList".to_string(), Value::Array(targets));

		if let Some(f) = &self.from {
			let froms: Vec<Value> = f.iter().map(|t| t.to_pg_query_json()).collect();
			obj.insert("fromClause".to_string(), Value::Array(froms));
		}

		if let Some(w) = &self.where_clause {
			obj.insert("whereClause".to_string(), w.to_pg_query_json());
		}

		if let Some(ret) = &self.returning {
			let ret_list: Vec<Value> = ret.iter().map(|t| t.to_pg_query_json()).collect();
			obj.insert("returningList".to_string(), Value::Array(ret_list));
		}

		if let Some(with) = &self.with {
			obj.insert("withClause".to_string(), with.to_pg_query_json());
		}

		pg_node("UpdateStmt", Value::Object(obj))
	}
}

impl ToPgQueryJson for DeleteStmt {
	fn to_pg_query_json(&self) -> Value {
		let mut obj = Map::new();
		obj.insert(
			"relation".to_string(),
			TableRef::Table {
				name: self.relation.clone(),
				alias: None,
				span: self.relation.span,
			}
			.to_pg_query_json(),
		);

		if let Some(u) = &self.using {
			let usings: Vec<Value> = u.iter().map(|t| t.to_pg_query_json()).collect();
			obj.insert("usingClause".to_string(), Value::Array(usings));
		}

		if let Some(w) = &self.where_clause {
			obj.insert("whereClause".to_string(), w.to_pg_query_json());
		}

		if let Some(ret) = &self.returning {
			let ret_list: Vec<Value> = ret.iter().map(|t| t.to_pg_query_json()).collect();
			obj.insert("returningList".to_string(), Value::Array(ret_list));
		}

		if let Some(with) = &self.with {
			obj.insert("withClause".to_string(), with.to_pg_query_json());
		}

		pg_node("DeleteStmt", Value::Object(obj))
	}
}

impl ToPgQueryJson for CreateTableStmt {
	fn to_pg_query_json(&self) -> Value {
		let mut obj = Map::new();
		obj.insert(
			"relation".to_string(),
			TableRef::Table {
				name: self.name.clone(),
				alias: None,
				span: self.name.span,
			}
			.to_pg_query_json(),
		);
		obj.insert("if_not_exists".to_string(), json!(self.if_not_exists));

		let mut elts = Vec::new();
		for col in &self.columns {
			elts.push(col.to_pg_query_json());
		}
		for con in &self.constraints {
			elts.push(con.to_pg_query_json());
		}
		obj.insert("tableElts".to_string(), Value::Array(elts));

		pg_node("CreateStmt", Value::Object(obj))
	}
}

impl ToPgQueryJson for ColumnDef {
	fn to_pg_query_json(&self) -> Value {
		let mut obj = Map::new();
		obj.insert("colname".to_string(), json!(self.name));
		obj.insert("typeName".to_string(), self.data_type.to_pg_query_json());
		if !self.constraints.is_empty() {
			let cons: Vec<Value> = self.constraints.iter().map(|c| c.to_pg_query_json()).collect();
			obj.insert("constraints".to_string(), Value::Array(cons));
		}
		pg_node("ColumnDef", Value::Object(obj))
	}
}

impl ToPgQueryJson for ColumnConstraint {
	fn to_pg_query_json(&self) -> Value {
		let mut obj = Map::new();
		let contype = match self {
			Self::NotNull => "CONSTR_NOTNULL",
			Self::Null => "CONSTR_NULL",
			Self::PrimaryKey => "CONSTR_PRIMARY",
			Self::Unique => "CONSTR_UNIQUE",
			Self::Default(expr) => {
				obj.insert("raw_expr".to_string(), expr.to_pg_query_json());
				"CONSTR_DEFAULT"
			}
			Self::References {
				table,
				column,
			} => {
				obj.insert(
					"pktable".to_string(),
					TableRef::Table {
						name: table.clone(),
						alias: None,
						span: table.span,
					}
					.to_pg_query_json(),
				);
				if let Some(col) = column {
					obj.insert("fk_attrs".to_string(), json!([pg_str(col)]));
				}
				"CONSTR_FOREIGN"
			}
		};
		obj.insert("contype".to_string(), json!(contype));
		pg_node("Constraint", Value::Object(obj))
	}
}

impl ToPgQueryJson for TableConstraint {
	fn to_pg_query_json(&self) -> Value {
		match self {
			Self::PrimaryKey(columns) => {
				let keys: Vec<Value> = columns.iter().map(|c| pg_str(c)).collect();
				pg_node(
					"Constraint",
					json!({
						"contype": "CONSTR_PRIMARY",
						"keys": keys
					}),
				)
			}
			Self::Unique(columns) => {
				let keys: Vec<Value> = columns.iter().map(|c| pg_str(c)).collect();
				pg_node(
					"Constraint",
					json!({
						"contype": "CONSTR_UNIQUE",
						"keys": keys
					}),
				)
			}
			Self::ForeignKey {
				columns,
				ref_table,
				ref_columns,
			} => {
				let fk_attrs: Vec<Value> = columns.iter().map(|c| pg_str(c)).collect();
				let pk_attrs: Vec<Value> = ref_columns.iter().map(|c| pg_str(c)).collect();
				pg_node(
					"Constraint",
					json!({
						"contype": "CONSTR_FOREIGN",
						"fk_attrs": fk_attrs,
						"pktable": TableRef::Table {
							name: ref_table.clone(),
							alias: None,
							span: ref_table.span,
						}.to_pg_query_json(),
						"pk_attrs": pk_attrs
					}),
				)
			}
		}
	}
}

impl ToPgQueryJson for DropStmt {
	fn to_pg_query_json(&self) -> Value {
		let mut obj = Map::new();
		let remove_type = match self.object_type {
			DropObjectType::Table => "OBJECT_TABLE",
			DropObjectType::View => "OBJECT_VIEW",
			DropObjectType::Index => "OBJECT_INDEX",
			DropObjectType::Schema => "OBJECT_SCHEMA",
			DropObjectType::Type => "OBJECT_TYPE",
		};
		obj.insert("removeType".to_string(), json!(remove_type));
		obj.insert("missing_ok".to_string(), json!(self.if_exists));
		obj.insert(
			"behavior".to_string(),
			json!(if self.cascade {
				"DROP_CASCADE"
			} else {
				"DROP_RESTRICT"
			}),
		);

		let objs: Vec<Value> = self
			.names
			.iter()
			.map(|q| {
				let parts: Vec<Value> = q.parts.iter().map(|p| pg_str(p)).collect();
				Value::Array(parts)
			})
			.collect();
		obj.insert("objects".to_string(), Value::Array(objs));

		pg_node("DropStmt", Value::Object(obj))
	}
}

impl ToPgQueryJson for TransactionStmt {
	fn to_pg_query_json(&self) -> Value {
		let mut obj = Map::new();
		let (kind, savepoint) = match self {
			Self::Begin {
				..
			} => ("TRANS_STMT_BEGIN", None),
			Self::Commit {
				..
			} => ("TRANS_STMT_COMMIT", None),
			Self::Rollback {
				..
			} => ("TRANS_STMT_ROLLBACK", None),
			Self::Savepoint {
				name,
				..
			} => ("TRANS_STMT_SAVEPOINT", Some(name.as_str())),
			Self::ReleaseSavepoint {
				name,
				..
			} => ("TRANS_STMT_RELEASE", Some(name.as_str())),
			Self::RollbackToSavepoint {
				name,
				..
			} => ("TRANS_STMT_ROLLBACK_TO", Some(name.as_str())),
		};
		obj.insert("kind".to_string(), json!(kind));
		if let Some(name) = savepoint {
			obj.insert("savepoint_name".to_string(), json!(name));
		}
		pg_node("TransactionStmt", Value::Object(obj))
	}
}

impl ToPgQueryJson for SetStmt {
	fn to_pg_query_json(&self) -> Value {
		pg_node(
			"VariableSetStmt",
			json!({
				"kind": "VAR_SET_VALUE",
				"name": self.name,
				"args": [ pg_node("A_Const", json!({ "sval": pg_str(&self.value)["String"] })) ]
			}),
		)
	}
}

impl ToPgQueryJson for ShowStmt {
	fn to_pg_query_json(&self) -> Value {
		pg_node(
			"VariableShowStmt",
			json!({
				"name": self.name
			}),
		)
	}
}

impl ToPgQueryJson for ResetStmt {
	fn to_pg_query_json(&self) -> Value {
		pg_node(
			"VariableSetStmt",
			json!({
				"kind": "VAR_RESET",
				"name": self.name
			}),
		)
	}
}

impl ToPgQueryJson for CreateIndexStmt {
	fn to_pg_query_json(&self) -> Value {
		let mut obj = Map::new();
		if let Some(name) = &self.name {
			obj.insert("idxname".to_string(), json!(name));
		}
		obj.insert(
			"relation".to_string(),
			TableRef::Table {
				name: self.table.clone(),
				alias: None,
				span: self.table.span,
			}
			.to_pg_query_json(),
		);
		if let Some(m) = &self.method {
			obj.insert("accessMethod".to_string(), json!(m));
		}
		obj.insert("unique".to_string(), json!(self.unique));
		obj.insert("if_not_exists".to_string(), json!(self.if_not_exists));

		let params: Vec<Value> = self
			.columns
			.iter()
			.map(|col| {
				pg_node(
					"IndexElem",
					json!({
						"expr": col.expr.to_pg_query_json(),
						"ordering": match col.direction {
							Some(OrderDirection::Asc) => "SORTBY_ASC",
							Some(OrderDirection::Desc) => "SORTBY_DESC",
							None => "SORTBY_DEFAULT",
						}
					}),
				)
			})
			.collect();
		obj.insert("indexParams".to_string(), Value::Array(params));

		pg_node("IndexStmt", Value::Object(obj))
	}
}

impl ToPgQueryJson for CreateViewStmt {
	fn to_pg_query_json(&self) -> Value {
		let mut obj = Map::new();
		obj.insert(
			"view".to_string(),
			TableRef::Table {
				name: self.name.clone(),
				alias: None,
				span: self.name.span,
			}
			.to_pg_query_json(),
		);
		obj.insert("replace".to_string(), json!(self.or_replace));
		if !self.columns.is_empty() {
			let cols: Vec<Value> = self.columns.iter().map(|c| pg_str(c)).collect();
			obj.insert("aliases".to_string(), Value::Array(cols));
		}
		obj.insert("query".to_string(), self.query.to_pg_query_json());
		pg_node("ViewStmt", Value::Object(obj))
	}
}

impl ToPgQueryJson for CreateSchemaStmt {
	fn to_pg_query_json(&self) -> Value {
		pg_node(
			"CreateSchemaStmt",
			json!({
				"schemaname": self.name,
				"if_not_exists": self.if_not_exists
			}),
		)
	}
}

impl ToPgQueryJson for AlterTableStmt {
	fn to_pg_query_json(&self) -> Value {
		let mut obj = Map::new();
		obj.insert(
			"relation".to_string(),
			TableRef::Table {
				name: self.name.clone(),
				alias: None,
				span: self.name.span,
			}
			.to_pg_query_json(),
		);
		obj.insert("missing_ok".to_string(), json!(self.if_exists));
		obj.insert("objtype".to_string(), json!("OBJECT_TABLE"));

		let cmds: Vec<Value> = self.actions.iter().map(|a| a.to_pg_query_json()).collect();
		obj.insert("cmds".to_string(), Value::Array(cmds));

		pg_node("AlterTableStmt", Value::Object(obj))
	}
}

impl ToPgQueryJson for AlterTableAction {
	fn to_pg_query_json(&self) -> Value {
		let mut obj = Map::new();
		match self {
			Self::AddColumn {
				column,
				if_not_exists,
			} => {
				obj.insert("subtype".to_string(), json!("AT_AddColumn"));
				obj.insert("def".to_string(), column.to_pg_query_json());
				obj.insert("missing_ok".to_string(), json!(if_not_exists));
			}
			Self::DropColumn {
				name,
				if_exists,
				cascade,
			} => {
				obj.insert("subtype".to_string(), json!("AT_DropColumn"));
				obj.insert("name".to_string(), json!(name));
				obj.insert("missing_ok".to_string(), json!(if_exists));
				obj.insert(
					"behavior".to_string(),
					json!(if *cascade {
						"DROP_CASCADE"
					} else {
						"DROP_RESTRICT"
					}),
				);
			}
			Self::AlterColumnType {
				name,
				data_type,
			} => {
				obj.insert("subtype".to_string(), json!("AT_AlterColumnType"));
				obj.insert("name".to_string(), json!(name));
				obj.insert("def".to_string(), data_type.to_pg_query_json());
			}
			Self::SetDefault {
				column,
				default,
			} => {
				obj.insert("subtype".to_string(), json!("AT_SetDefault"));
				obj.insert("name".to_string(), json!(column));
				obj.insert("def".to_string(), default.to_pg_query_json());
			}
			Self::DropDefault {
				column,
			} => {
				obj.insert("subtype".to_string(), json!("AT_DropDefault"));
				obj.insert("name".to_string(), json!(column));
			}
			Self::SetNotNull {
				column,
			} => {
				obj.insert("subtype".to_string(), json!("AT_SetNotNull"));
				obj.insert("name".to_string(), json!(column));
			}
			Self::DropNotNull {
				column,
			} => {
				obj.insert("subtype".to_string(), json!("AT_DropNotNull"));
				obj.insert("name".to_string(), json!(column));
			}
			Self::AddConstraint(constraint) => {
				obj.insert("subtype".to_string(), json!("AT_AddConstraint"));
				obj.insert("def".to_string(), constraint.to_pg_query_json());
			}
			Self::DropConstraint {
				name,
				if_exists,
				cascade,
			} => {
				obj.insert("subtype".to_string(), json!("AT_DropConstraint"));
				obj.insert("name".to_string(), json!(name));
				obj.insert("missing_ok".to_string(), json!(if_exists));
				obj.insert(
					"behavior".to_string(),
					json!(if *cascade {
						"DROP_CASCADE"
					} else {
						"DROP_RESTRICT"
					}),
				);
			}
			Self::RenameColumn {
				old_name,
				new_name,
			} => {
				obj.insert("subtype".to_string(), json!("AT_RenameColumn"));
				obj.insert("old_name".to_string(), json!(old_name));
				obj.insert("new_name".to_string(), json!(new_name));
			}
			Self::RenameTable {
				new_name,
			} => {
				obj.insert("subtype".to_string(), json!("AT_RenameTable"));
				obj.insert("new_name".to_string(), json!(new_name));
			}
		}
		pg_node("AlterTableCmd", Value::Object(obj))
	}
}

impl ToPgQueryJson for PrepareStmt {
	fn to_pg_query_json(&self) -> Value {
		let types: Vec<Value> = self.param_types.iter().map(|t| t.to_pg_query_json()).collect();
		pg_node(
			"PrepareStmt",
			json!({
				"name": self.name,
				"argtypes": types,
				"query": self.statement.to_pg_query_json()
			}),
		)
	}
}

impl ToPgQueryJson for ExecuteStmt {
	fn to_pg_query_json(&self) -> Value {
		let params: Vec<Value> = self.params.iter().map(|p| p.to_pg_query_json()).collect();
		pg_node(
			"ExecuteStmt",
			json!({
				"name": self.name,
				"params": params
			}),
		)
	}
}

impl ToPgQueryJson for DeallocateStmt {
	fn to_pg_query_json(&self) -> Value {
		let mut obj = Map::new();
		if let Some(name) = &self.name {
			obj.insert("name".to_string(), json!(name));
		}
		pg_node("DeallocateStmt", Value::Object(obj))
	}
}

impl ToPgQueryJson for ExplainStmt {
	fn to_pg_query_json(&self) -> Value {
		let opts: Vec<Value> = self.options.iter().map(|o| pg_str(o)).collect();
		pg_node(
			"ExplainStmt",
			json!({
				"query": self.statement.to_pg_query_json(),
				"options": opts
			}),
		)
	}
}
