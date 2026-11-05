#![no_main]

use libfuzzer_sys::fuzz_target;
use std::hint::black_box;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_expr::expr::statements::{CreateStatement, DeleteStatement, SelectStatement};
use surrealdb_expr::expr::{BinaryOperator, Cond, Data, Expr, Fields, Literal, ObjectEntry};
use surrealdb_core::syn::runtime::{Query, TopLevelExpr};

/// Build trees directly, including shapes a text parser would reject.
/// Depth and statement count bound the work performed for each fuzz input.
fn expression(bytes: &mut std::slice::Iter<'_, u8>, depth: u8) -> Expr {
    let Some(&tag) = bytes.next() else { return Expr::Literal(Literal::None) };
    if depth == 0 {
        return Expr::Literal(Literal::Integer(i64::from(tag)));
    }
    match tag % 7 {
        0 => Expr::Literal(Literal::Integer(i64::from(tag))),
        1 => Expr::Literal(Literal::Bool(tag & 8 != 0)),
        2 => Expr::Param(format!("p{}", tag % 4).into()),
        3 => Expr::Literal(Literal::Array(vec![expression(bytes, depth - 1), expression(bytes, depth - 1)])),
        4 => Expr::Literal(Literal::Object(vec![ObjectEntry {
            key: format!("f{}", tag % 4).into(),
            value: expression(bytes, depth - 1),
        }])),
        5 => Expr::Binary {
            left: Box::new(expression(bytes, depth - 1)),
            op: match tag / 7 % 4 {
                0 => BinaryOperator::Add,
                1 => BinaryOperator::Equal,
                2 => BinaryOperator::And,
                _ => BinaryOperator::Divide,
            },
            right: Box::new(expression(bytes, depth - 1)),
        },
        _ => Expr::Table(format!("t{}", tag % 4).into()),
    }
}

fn select(what: Expr, cond: Option<Cond>) -> Expr {
    Expr::Select(Box::new(SelectStatement {
        fields: Fields::all(), omit: Vec::new(), only: false,
        what: vec![what], with: None, cond, split: None, group: None,
        order: None, limit: None, start: None, fetch: None,
        version: Expr::Literal(Literal::None),
        timeout: Expr::Literal(Literal::None),
        explain: None, tempfiles: false, for_update: false,
    }))
}

fn query(input: &[u8]) -> Query {
    let mut bytes = input.iter();
    let mut expressions = Vec::new();
    while let Some(&tag) = bytes.next() {
        let value = expression(&mut bytes, 6);
        let target = Expr::Table(format!("t{}", tag % 4).into());
        let statement = match tag % 5 {
            0 => value,
            1 => Expr::Create(Box::new(CreateStatement {
                what: vec![target], data: Some(Data::ContentExpression(value)),
                ..Default::default()
            })),
            2 => select(target, Some(Cond(value))),
            3 => Expr::Delete(Box::new(DeleteStatement {
                what: vec![target], cond: Some(Cond(value)), ..Default::default()
            })),
            _ => select(value, None),
        };
        expressions.push(TopLevelExpr::Expr(statement));
        if expressions.len() == 16 { break; }
    }
    Query { expressions }
}

fuzz_target!(|input: &[u8]| {
	let query = query(input);
	tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
		let dbs = Datastore::builder()
			.without_maintenance_tasks()
			.build_with_path("memory")
			.await
			.unwrap();
		let ses = Session::owner().with_ns("test").with_db("test");
		_ = black_box(dbs.process(query, &ses, None).await);
	})
});
