use crate::{
    config::SurrealismConfig, host::{implement_host_functions, Host}, package::SurrealismPackage
};
use anyhow::Result;
use surrealism_types::{
    args::Args,
    controller::MemoryController,
    err::PrefixError,
    transfer::Transfer,
};
use wasmtime::*;
use wasmtime_wasi::preview1::{self, WasiP1Ctx};
use std::sync::Arc;

pub struct StoreData {
    pub wasi: WasiP1Ctx,
    pub host: Arc<dyn Host>,
    pub config: SurrealismConfig,
}

pub struct Controller {
    pub store: Store<StoreData>,
    pub instance: Instance,
    pub memory: Memory,
}

impl Controller {
    pub fn new(
        SurrealismPackage { wasm, config }: SurrealismPackage,
        host: Arc<dyn Host>,
    ) -> Result<Self> {
        let engine = Engine::default();
        let module =
            Module::new(&engine, wasm).prefix_err(|| "Failed to construct module from bytes")?;

        let mut linker: Linker<StoreData> = Linker::new(&engine);
        preview1::add_to_linker_sync(&mut linker, |data| &mut data.wasi)
            .prefix_err(|| "failed to add WASI to linker")?;

        implement_host_functions(&mut linker)
            .prefix_err(|| "failed to implement host functions")?;

        let wasi_ctx = super::wasi_context::build(host.clone())?;

        let store_data = StoreData {
            wasi: wasi_ctx,
            host,
            config,
        };
        let mut store = Store::new(&engine, store_data);
        let instance = linker
            .instantiate(&mut store, &module)
            .prefix_err(|| "failed to instantiate WASM module")?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .prefix_err(|| "WASM module must export 'memory'")?;

        Ok(Self {
            store,
            instance,
            memory,
        })
    }

    pub fn alloc(&mut self, len: u32, align: u32) -> Result<u32> {
        let alloc = self
            .instance
            .get_typed_func::<(u32, u32), i32>(&mut self.store, "__sr_alloc")?;
        let result = alloc.call(&mut self.store, (len, align))?;
        if result == -1 {
            anyhow::bail!("Memory allocation failed");
        }
        Ok(result as u32)
    }

    pub fn free(&mut self, ptr: u32, len: u32) -> Result<()> {
        let free = self
            .instance
            .get_typed_func::<(u32, u32), i32>(&mut self.store, "__sr_free")?;
        let result = free.call(&mut self.store, (ptr, len))?;
        if result == -1 {
            anyhow::bail!("Memory deallocation failed");
        }
        Ok(())
    }

    pub fn init(&mut self) -> Result<()> {
        let init = self.instance.get_export(&mut self.store, "__sr_init");
        if init.is_none() {
            return Ok(());
        }

        let init = self
            .instance
            .get_typed_func::<(), ()>(&mut self.store, "__sr_init")?;
        init.call(&mut self.store, ())
    }

    pub fn invoke<A: Args>(&mut self, name: Option<String>, args: A) -> Result<surrealdb_types::Value> {
        let name = format!("__sr_fnc__{}", name.unwrap_or_default());
        let args = args.to_values().transfer(self)?;
        let invoke = self
            .instance
            .get_typed_func::<(u32,), (i32,)>(&mut self.store, &name)?;
        let (ptr,) = invoke.call(&mut self.store, (*args,))?;
        Result::<surrealdb_types::Value>::receive(ptr.try_into()?, self)?
    }

    pub fn args(&mut self, name: Option<String>) -> Result<Vec<surrealdb_types::Kind>> {
        let name = format!("__sr_args__{}", name.unwrap_or_default());
        let args = self
            .instance
            .get_typed_func::<(), (i32,)>(&mut self.store, &name)?;
        let (ptr,) = args.call(&mut self.store, ())?;
        Vec::<surrealdb_types::Kind>::receive(ptr.try_into()?, self)
    }

    pub fn returns(&mut self, name: Option<String>) -> Result<surrealdb_types::Kind> {
        let name = format!("__sr_returns__{}", name.unwrap_or_default());
        let returns = self
            .instance
            .get_typed_func::<(), (i32,)>(&mut self.store, &name)?;
        let (ptr,) = returns.call(&mut self.store, ())?;
        surrealdb_types::Kind::receive(ptr.try_into()?, self)
    }

    pub fn list(&mut self) -> Result<Vec<String>> {
        // scan the exported functions and return a list of available functions
        let mut functions = Vec::new();

        // First, collect all export names that start with __sr_fnc__
        let function_names: Vec<String> = {
            let exports = self.instance.exports(&mut self.store);
            exports
                .filter_map(|export| {
                    let name = export.name();
                    if name.starts_with("__sr_fnc__") {
                        Some(name.to_string())
                    } else {
                        None
                    }
                })
                .collect()
        };

        // Then check each one to see if it's actually a function
        for name in function_names {
            if let Some(export) = self.instance.get_export(&mut self.store, &name) {
                if let ExternType::Func(_) = export.ty(&self.store) {
                    // strip the prefix
                    let function_name =
                        name.strip_prefix("__sr_fnc__").unwrap_or(&name).to_string();
                    functions.push(function_name);
                }
            }
        }

        Ok(functions)
    }
}

impl MemoryController for Controller {
    fn alloc(&mut self, len: u32, align: u32) -> Result<u32> {
        Controller::alloc(self, len, align)
    }

    fn free(&mut self, ptr: u32, len: u32) -> Result<()> {
        Controller::free(self, ptr, len)
    }

    fn mut_mem(&mut self, ptr: u32, len: u32) -> &mut [u8] {
        let mem = self.memory.data_mut(&mut self.store);
        &mut mem[(ptr as usize)..(ptr as usize) + (len as usize)]
    }
}
