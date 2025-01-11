use std::collections::HashMap;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::sql::value::Value;
use crate::sql::{Object, Table, TableType};

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
    let fields = statements.iter().map(|s| {
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
    }).collect::<Vec<_>>();

    Ok(fields.into())
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
    let tables = statements.iter().map(|s| {
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
    }).collect::<Vec<_>>();

    Ok(tables.into())
}