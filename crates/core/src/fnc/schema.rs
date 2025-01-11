use std::collections::HashMap;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::sql::statements::{DefineFieldStatement, DefineIndexStatement, DefineTableStatement};
use crate::sql::value::Value;
use crate::sql::{Index, Object, Table, TableType};

pub async fn field(
    (ctx, opt): (&Context, &Options),
    (table, name): (Table, String)
) -> Result<Value, Error> {
    // Get the NS and DB
    let ns = opt.ns()?;
    let db = opt.db()?;
    // Get the transaction
    let txn = ctx.tx();
    // Retrieve statement
    let statement = txn.get_tb_field(ns, db, &table.0, &name).await.ok();

    // Map statements to fields
    let v = match statement {
        Some(s) => map_field_statement(&s),
        None => Value::None,
    };

    Ok(v)
}

pub async fn fields(
    (ctx, opt): (&Context, &Options),
    (table,): (Table,)
) -> Result<Value, Error> {
    // Get the NS and DB
    let ns = opt.ns()?;
    let db = opt.db()?;
    // Only last version for now
    let version = None;
    // Get the transaction
    let txn = ctx.tx();
    // Retrieve statements
    let statements = txn.all_tb_fields(ns, db, &table.0, version).await?;

    // Map statements to fields
    let fields = statements.iter().map(map_field_statement).collect::<Vec<_>>();

    Ok(fields.into())
}

fn map_field_statement(s: &DefineFieldStatement) -> Value {
    let computed = match &s.value {
        Some(value) => !value.is_static(),
        None => false,
    };
    let _type = match &s.kind {
        Some(kind) => kind.to_string().into(),
        None => Value::None,
    };

    let mut h = HashMap::<&str, Value>::new();
    h.insert("name", s.name.to_string().into());
    h.insert("type", _type);
    h.insert("flexible", s.flex.into());
    h.insert("readonly", s.readonly.into());
    h.insert("computed", computed.into());
    h.insert("comment", s.comment.clone().into());

    Value::Object(Object::from(h))
}

pub async fn index(
    (ctx, opt): (&Context, &Options),
    (table, name): (Table, String)
) -> Result<Value, Error> {
    // Get the NS and DB
    let ns = opt.ns()?;
    let db = opt.db()?;
    // Get the transaction
    let txn = ctx.tx();
    // Retrieve statement
    let statement = txn.get_tb_index(ns, db, &table.0, &name).await.ok();

    // Map statements to indexes
    let v = match statement {
        Some(s) => map_index_statement(&s),
        None => Value::None,
    };

    Ok(v)
}

pub async fn indexes(
    (ctx, opt): (&Context, &Options),
    (table,): (Table,)
) -> Result<Value, Error> {
    // Get the NS and DB
    let ns = opt.ns()?;
    let db = opt.db()?;
    // Get the transaction
    let txn = ctx.tx();
    // Retrieve statements
    let statements = txn.all_tb_indexes(ns, db, &table.0).await?;

    // Map statements to indexes
    let indexes = statements.iter().map(|s| map_index_statement(s)).collect::<Vec<_>>();

    Ok(indexes.into())
}

fn map_index_statement(s: &DefineIndexStatement) -> Value {
    let _type = match &s.index {
        Index::Idx => "INDEX",
        Index::Uniq => "UNIQUE",
        Index::Search(_) => "SEARCH",
        Index::MTree(_) => "MTREE",
        Index::Hnsw(_) => "HNSW",
    };
    let columns = s.cols.iter().map(|c| c.to_string()).collect::<Vec<_>>();

    let mut h = HashMap::<&str, Value>::new();
    h.insert("name", s.name.to_string().into());
    h.insert("type", _type.into());
    h.insert("columns", columns.into());
    h.insert("comment", s.comment.clone().into());

    Value::Object(Object::from(h))
}

pub async fn table(
    (ctx, opt): (&Context, &Options),
    (name,): (String,)
) -> Result<Value, Error> {
    // Get the NS and DB
    let ns = opt.ns()?;
    let db = opt.db()?;
    // Get the transaction
    let txn = ctx.tx();
    // Retrieve statement
    let statement = txn.get_tb(ns, db, &name).await.ok();

    // Map statements to tables
    let v = match statement {
        Some(s) => map_table_statement(&s),
        None => Value::None,
    };

    Ok(v)
}

pub async fn tables(
    (ctx, opt): (&Context, &Options),
    _: ()
) -> Result<Value, Error> {
    // Get the NS and DB
    let ns = opt.ns()?;
    let db = opt.db()?;
    // Only last version for now
    let version = None;
    // Get the transaction
    let txn = ctx.tx();
    // Retrieve statements
    let statements = txn.all_tb(ns, db, version).await?;

    // Map statements to tables
    let tables = statements.iter().map(|s| map_table_statement(s)).collect::<Vec<_>>();

    Ok(tables.into())
}

fn map_table_statement(s: &DefineTableStatement) -> Value {
    let computed = s.view != None;
    let _type = match s.kind {
        TableType::Any => "ANY",
        TableType::Normal => "NORMAL",
        TableType::Relation(_) => "RELATION",
    };

    let mut h = HashMap::<&str, Value>::new();
    h.insert("name", s.name.0.to_string().into());
    h.insert("type", _type.into());
    h.insert("schemafull", s.full.into());
    h.insert("computed", computed.into());
    h.insert("drop", s.drop.into());
    h.insert("comment", s.comment.clone().into());

    Value::Object(Object::from(h))
}