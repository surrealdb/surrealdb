use async_trait::async_trait;
use crate::syn;
use crate::ctx::Context;
use crate::doc::CursorDoc;
use reblessive::tree::Stk;
use crate::types::{PublicObject, PublicValue};
use crate::dbs::Options;
use surrealism_runtime::config::SurrealismConfig;
use surrealism_runtime::host::InvocationContext;
use surrealism_runtime::kv::KVStore;
use anyhow::Result;

pub struct Host<'a> {
    stk: &'a mut Stk,
    ctx: &'a Context,
    opt: &'a Options,
    doc: &'a Option<CursorDoc>,
}

#[async_trait(?Send)]
impl<'a> InvocationContext for Host<'a> {
    async fn sql(&self, config: &SurrealismConfig, query: String, vars: PublicObject) -> Result<PublicValue> {
        let expr = syn::expr(&query)?;

        todo!()
    }

    async fn run(&self, config: &SurrealismConfig, fnc: String, version: Option<String>, args: Vec<PublicValue>) -> Result<PublicValue> {
        todo!()
    }

    fn kv(&self) -> &dyn KVStore {
        todo!()
    }
    
    async fn ml_invoke_model(&self, config: &SurrealismConfig, model: String, input: PublicValue, weight: i64, weight_dir: String) -> Result<PublicValue> {
        todo!()
    }

    async fn ml_tokenize(&self, config: &SurrealismConfig, model: String, input: PublicValue) -> Result<Vec<f64>> {
        todo!()
    }

    fn stdout(&self, output: &str) -> Result<()> {
        todo!()
    }

    fn stderr(&self, output: &str) -> Result<()> {
        todo!()
    }
}