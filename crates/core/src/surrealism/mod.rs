pub(crate) mod host;
pub(crate) mod cache;
use anyhow::Result;
use surrealism_runtime::controller::Controller;
use async_trait::async_trait;

use crate::{expr::Kind, val::Value};

#[async_trait]
pub(crate) trait InternalSurrealismController {
    async fn args(&self, sub: Option<String>) -> Result<Vec<Kind>>;
    async fn returns(&self, sub: Option<String>) -> Result<Kind>;
    async fn run(&self, sub: Option<String>, args: Vec<Value>) -> Result<Value>;
}

#[async_trait]
impl InternalSurrealismController for Controller {
    async fn args(&self, sub: Option<String>) -> Result<Vec<Kind>> {
        self
            .args(sub)
            .await
            .map(|x| x.into_iter().map(|x| x.into()).collect())
    }

    async fn returns(&self, sub: Option<String>) -> Result<Kind> {
        self
            .returns(sub)
            .await
            .map(Into::into)
    }

    async fn run(&self, sub: Option<String>, args: Vec<Value>) -> Result<Value> {
        let args = args.into_iter().map(|x| x.into()).collect();
        self
            .run(sub, args)
            .await
            .map(|x| x.into())
    }
}