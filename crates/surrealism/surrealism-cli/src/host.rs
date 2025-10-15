use anyhow::{Context, Result};
use async_trait::async_trait;
use candle_core::DType;
use surrealdb_types::{SurrealValue, ToSql};
use std::{io::BufRead, sync::Arc};
use std::path::PathBuf;
use surrealism_runtime::{
    config::SurrealismConfig,
    host::InvocationContext,
    kv::{BTreeMapStore, KVStore},
};
use surrealml_llms::{
    interface::{load_model::load_model, run_model::run_model},
    models::model_spec::{model_spec_trait::ModelSpec, models::gemma::Gemma},
};

use crate::parse_value;

pub struct DemoHost {
    kv: BTreeMapStore,
}

impl DemoHost {
    pub fn new() -> Self {
        Self {
            kv: BTreeMapStore::new(),
        }
    }
}

#[async_trait(?Send)]
impl InvocationContext for DemoHost {
    fn kv(&mut self) -> &dyn KVStore {
        &self.kv
    }

    async fn sql(&mut self, _config: &SurrealismConfig, query: String, vars: surrealdb_types::Object) -> Result<surrealdb_types::Value> {
        println!("The module is running a SQL query:");
        println!("SQL: {query}");
        println!("Vars: {:#}", vars.to_sql());
        println!("Please enter the result:");

        loop {
            match parse_value(&mut std::io::stdin().lock().lines().next().unwrap().unwrap()) {
                Ok(x) => {
                    println!(" ");
                    return Ok(x);
                }
                Err(e) => {
                    println!("Failed to parse value: {e}");
                    println!("Please try again");
                }
            }
        }
    }

    async fn run(
        &mut self,
        _config: &SurrealismConfig,
        fnc: String,
        version: Option<String>,
        args: Vec<surrealdb_types::Value>,
    ) -> Result<surrealdb_types::Value> {
        let version = version.map(|x| format!("<{x}>")).unwrap_or_default();
        println!("The module is running a function:");
        println!(
            " - {fnc}{version}({})",
            args.iter()
                .map(|x| format!("{:}", x.to_sql()))
                .collect::<Vec<String>>()
                .join(", ")
        );
        println!("\nPlease enter the result:");

        loop {
            match parse_value(&mut std::io::stdin().lock().lines().next().unwrap().unwrap()) {
                Ok(x) => {
                    println!(" ");
                    return Ok(x);
                }
                Err(e) => {
                    println!("Failed to parse value: {e}");
                    println!("Please try again");
                }
            }
        }
    }

    // "google/gemma-7b"
    async fn ml_invoke_model(
        &mut self,
        _config: &SurrealismConfig,
        model: String,
        input: surrealdb_types::Value,
        weight: i64,
        weight_dir: String,
    ) -> Result<surrealdb_types::Value> {
        let surrealdb_types::Value::String(input) = input else {
            anyhow::bail!("Expected string input")
        };
        let home = std::env::var("HOME")?;
        // For HF cached weights at to be loaded but we can store the weights somewhere for all
        // later and reference them.
        // let weight_path = "google--gemma-7b";
        let base = PathBuf::from(home).join(
            format!(".cache/huggingface/hub/models--{}/snapshots", &weight_dir).replace("'", ""),
        );

        let snapshot = std::fs::read_dir(&base)?
            .next()
            .ok_or_else(|| anyhow::anyhow!("No snapshot found"))??
            .path();

        let names = Gemma.return_tensor_filenames();
        let paths: Vec<PathBuf> = names.into_iter().map(|f| snapshot.join(f)).collect();
        let mut wrapper = load_model(&model, DType::F16, Some(paths), None)
            .context("Gemma should load from local cache")?;
        let input = input.to_string();
        Ok(run_model(&mut wrapper, input, 20)
            .context("run_model should succeed")?
            .into_value())
    }

    async fn ml_tokenize(&mut self, _config: &SurrealismConfig, model: String, input: surrealdb_types::Value) -> Result<Vec<f64>> {
        println!("The module is running a ML tokenizer:");
        println!("Model: {model}");
        println!("Input: {:}", input.to_sql());
        println!("Please enter the result:");

        loop {
            match parse_value(&mut std::io::stdin().lock().lines().next().unwrap().unwrap()) {
                Ok(x) => {
                    if let surrealdb_types::Value::Array(x) = x {
                        let arr = x
                            .into_iter()
                            .map(|x| -> Result<f64> {
                                if let surrealdb_types::Value::Number(surrealdb_types::Number::Float(x)) = x {
                                    Ok(x)
                                } else {
                                    Err(anyhow::anyhow!("Expected array of f64"))
                                }
                            })
                            .collect::<Result<Vec<f64>>>()?;

                        println!(" ");
                        return Ok(arr);
                    }
                    return Err(anyhow::anyhow!("Expected array of f64"));
                }
                Err(e) => {
                    println!("Failed to parse value: {e}");
                    println!("Please try again");
                }
            }
        }
    }

    fn stdout(&mut self, output: &str) -> Result<()> {
        println!("[surli::out] {}", output);
        Ok(())
    }

    fn stderr(&mut self, output: &str) -> Result<()> {
        eprintln!("[surli::err] {}", output);
        Ok(())
    }
}
