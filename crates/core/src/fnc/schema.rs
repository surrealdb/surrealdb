use std::collections::HashMap;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::sql::statements::{DefineEventStatement, DefineFieldStatement, DefineFunctionStatement, DefineIndexStatement, DefineTableStatement};
use crate::sql::value::Value;
use crate::sql::{Ident, Index, Kind, Object, Table, TableType};

pub async fn event(
    (ctx, opt): (&Context, &Options),
    (table, name): (Table, String)
) -> Result<Value, Error> {
    // Get the NS and DB
    let ns = opt.ns()?;
    let db = opt.db()?;
    // Get the transaction
    let txn = ctx.tx();
    // Retrieve statement
    let statement = txn.get_tb_event(ns, db, &table.0, &name).await.ok();

    // Map statements to events
    let v = match statement {
        Some(s) => map_event_statement(&s),
        None => Value::None,
    };

    Ok(v)
}

pub async fn events(
    (ctx, opt): (&Context, &Options),
    (table,): (Table,)
) -> Result<Value, Error> {
    // Get the NS and DB
    let ns = opt.ns()?;
    let db = opt.db()?;
    // Get the transaction
    let txn = ctx.tx();
    // Retrieve statements
    let statements = txn.all_tb_events(ns, db, &table.0).await?;

    // Map statements to events
    let events = statements.iter().map(map_event_statement).collect::<Vec<_>>();

    Ok(events.into())
}

fn map_event_statement(s: &DefineEventStatement) -> Value {
    let then = s.then.iter().map(|v| Value::from(v.to_string())).collect::<Vec<_>>().into();

    let mut h = HashMap::<&str, Value>::new();
    h.insert("name", s.name.to_string().into());
    h.insert("condition", s.when.to_string().into());
    h.insert("actions", then);
    h.insert("comment", s.comment.clone().into());

    Value::Object(Object::from(h))
}

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


pub async fn function(
    (ctx, opt): (&Context, &Options),
    (name,): (String,)
) -> Result<Value, Error> {
    // Get the NS and DB
    let ns = opt.ns()?;
    let db = opt.db()?;
    // Get the transaction
    let txn = ctx.tx();
    // Retrieve statement
    let statement = txn.get_db_function(ns, db, &name).await.ok();

    // Map statements to functions
    let v = match statement {
        Some(s) => map_function_statement(&s),
        None => Value::None,
    };

    Ok(v)
}

pub async fn functions(
    (ctx, opt): (&Context, &Options),
    _: ()
) -> Result<Value, Error> {
    // Get the NS and DB
    let ns = opt.ns()?;
    let db = opt.db()?;
    // Get the transaction
    let txn = ctx.tx();
    // Retrieve statements
    let statements = txn.all_db_functions(ns, db).await?;

    // Map statements to functions
    let events = statements.iter().map(map_function_statement).collect::<Vec<_>>();

    Ok(events.into())
}

fn map_function_statement(s: &DefineFunctionStatement) -> Value {
    let args = s.args.iter().map(map_function_arg).collect::<Vec<_>>();
    let returns = match &s.returns {
        Some(v) => v.to_string().into(),
        None => Value::None,
    };

    let mut h = HashMap::<&str, Value>::new();
    h.insert("name", s.name.to_string().into());
    h.insert("args", args.into());
    h.insert("body", s.block.to_string().into());
    h.insert("returns", returns);
    h.insert("comment", s.comment.clone().into());

    Value::Object(Object::from(h))
}

fn map_function_arg((name, kind): &(Ident, Kind)) -> Value {
    let mut h = HashMap::<&str, Value>::new();
    h.insert("name", name.to_string().into());
    h.insert("type", kind.to_string().into());
    
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
    let relation = match &s.kind {
        TableType::Relation(rel) => {
            let mut h = HashMap::<&str, Value>::new();
            let _in = if let Some(Kind::Record(tables)) = &rel.from {
                tables.into_iter().map(|t| Value::from(Table(t.0.clone()))).collect::<Vec<_>>().into()
            } else {
                Value::None
            };
            h.insert("in", _in);
            let out = if let Some(Kind::Record(tables)) = &rel.to {
                tables.into_iter().map(|t| Value::from(Table(t.0.clone()))).collect::<Vec<_>>().into()
            } else {
                Value::None
            };
            h.insert("out", out);
            h.insert("enforced", rel.enforced.into());

            Value::Object(Object::from(h))
        },
        _ => Value::None,
    };

    let mut h = HashMap::<&str, Value>::new();
    h.insert("name", Value::from(Table(s.name.0.to_string())));
    h.insert("drop", s.drop.into());
    h.insert("schemafull", s.full.into());
    h.insert("type", _type.into());
    h.insert("relation", relation);
    h.insert("computed", computed.into());
    h.insert("comment", s.comment.clone().into());

    Value::Object(Object::from(h))
}