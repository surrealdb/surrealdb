//! Here is where we link ML modules with the wasmtime runtime.
//!
//! # Engine + Config
//! Global compilation configurations such as CPU features, caching, pooling, debug info
//! One engine can compile many modules and create many stores. It is not a per-process inside the guest
//!
//! # Store
//! per instance execution sandbox: keeps the guest's memories, tables, and host "context" `WasiCtx`.
//! exists as long as you keep the store alive
//!
//! # WasiCtx
//! built by WasiCtxBuilder this is the guest's visable "process environment"
//! lives inside a `Store`. Immutable once built, but sticks around for every call into the instance until
//! you drop the store.
//!
//! ## Inside the context
//! - standard streams => `stdin`, `stdout`, `stderr`
use surrealml_tokenizers::{encode, load_local_tokenizer};
use wasmtime::AsContextMut;
use wasmtime::{Caller, Linker, Memory};

fn guest_str(mem: &[u8], ptr: i32, len: i32) -> &str {
    std::str::from_utf8(&mem[ptr as usize..(ptr + len) as usize]).unwrap()
}

// helper: write &[u32] to guest memory, return (ptr,len)
fn write_u32_slice(caller: &mut Caller<'_, ()>, mem: &Memory, data: &[u32]) -> (i32, i32) {
    let bytes = bytemuck::cast_slice(data);
    let size = bytes.len() as i32;

    // naive bump-allocator: grow memory and drop the pointer at the old size
    let mut store = caller.as_context_mut();
    let old_size = mem.data(&store).len();
    let extra_pages = ((size + 0xFFFF) / 0x10000) as u64; // 64 KiB pages
    mem.grow(&mut store, extra_pages).unwrap();

    let dst = &mut mem.data_mut(&mut store)[old_size..old_size + size as usize];
    dst.copy_from_slice(bytes);

    (old_size as i32, data.len() as i32) // ptr, *element* count
}

fn tokenizer_encode_raw(
    mut caller: Caller<'_, ()>,
    model_ptr: i32,
    model_len: i32,
    input_ptr: i32,
    input_len: i32,
) -> (i32, i32) {
    let mem = caller.get_export("memory").unwrap().into_memory().unwrap();

    let data = mem.data(&caller);

    let model = guest_str(data, model_ptr, model_len);
    let input = guest_str(data, input_ptr, input_len);

    let tokenizer_model = load_local_tokenizer(model.to_owned()).unwrap();
    let tokens = encode(&tokenizer_model, input).unwrap();
    write_u32_slice(&mut caller, &mem, &tokens)
}

/// Links the host with the ML functions.
///
/// # Notes
/// Right now this is just to prove it works but happy to refine the
/// interface later with input from others in the team
///
/// # Arguments
/// - `linker`: the linker to the WASM host
pub fn link_ml(linker: &mut Linker<()>) {
    linker
        .func_wrap("host", "tokenizer_encode", tokenizer_encode_raw)
        .unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytemuck::cast_slice;
    use surrealml_tokenizers::PresetTokenizers;
    use wasmtime::TypedFunc;
    use wasmtime::{Engine, Linker, Module, Store};

    fn add(a: i32, b: i32) -> i32 {
        a + b
    }

    fn call_tokenizer_encode(model: &str, input: &str) -> Vec<u32> {
        let tokenizer_model = load_local_tokenizer(model.to_owned()).unwrap();
        let output = encode(&tokenizer_model, input.into()).unwrap();
        return output;
    }

    /// To test that the basic raw linking just works. Other tests will focus on the linking of ML modules.
    #[test]
    fn test_basic_linking() {
        // 1 Engine – compiles & caches modules.
        let engine = Engine::default();
        let mut linker = Linker::new(&engine);

        // 2 ️⃣  Linker – link the add functio
        linker.func_wrap("host", "add", add).unwrap();

        // 3 Store – per-instance state (no WASI needed here).
        let mut store = Store::new(&engine, ());

        // 4 Tiny text-format Wasm that calls the host function.
        let wat = r#"
            (module
                (import "host" "add" (func $add (param i32 i32) (result i32)))
                (func (export "run") (result i32)
                    i32.const 2
                    i32.const 3
                    call $add)
            )
        "#;

        // Compile → instantiate → grab the `run` export.
        let module = Module::new(&engine, wat).unwrap();
        let instance = linker.instantiate(&mut store, &module).unwrap();
        let run = instance
            .get_typed_func::<(), i32>(&mut store, "run")
            .unwrap();

        // Call it!
        let result = run.call(&mut store, ()).unwrap();
        println!("2 + 3 = {}", result); // → “2 + 3 = 5”
    }

    #[test]
    fn test_basic_load() {
        let _ = call_tokenizer_encode(
            &PresetTokenizers::Mixtral8x7Bv01.to_string(),
            "Hello from a preset!",
        );
        let _ = call_tokenizer_encode(
            &PresetTokenizers::Gemma2B.to_string(),
            "Hello from a preset!",
        );
    }

    #[test]
    fn guest_tokens_same_as_host() {
        // -------- WAT with the correct string lengths --------
        let wat = r#"
        (module
          (import "host" "tokenizer_encode"
            (func $tokenizer_encode (param i32 i32 i32 i32) (result i32 i32)))

          (memory (export "memory") 1)

          ;; 27-byte model name
          (data (i32.const 0)  "mistralai/Mixtral-8x7B-v0.1")

          ;; 23-byte prompt
          (data (i32.const 64) "The cat sat on the mat.")

          (func (export "run") (result i32 i32)
            i32.const 0      ;; model ptr
            i32.const 27     ;; model len  (correct)
            i32.const 64     ;; input ptr
            i32.const 23     ;; input len  (**** fixed ****)
            call $tokenizer_encode))
        "#;

        // ---------- set-up ----------
        let engine = Engine::default();
        let module = Module::new(&engine, wat).expect("WASM module to be created");
        let mut linker = Linker::new(&engine);
        link_ml(&mut linker); // registers the wrapper
        let mut store = Store::new(&engine, ());
        let instance = linker
            .instantiate(&mut store, &module)
            .expect("WASM module to be instantiated");

        // ---------- call guest ----------
        let run: TypedFunc<(), (i32, i32)> = instance
            .get_typed_func(&mut store, "run")
            .expect("run function to be extracted");
        let (ptr, len) = run.call(&mut store, ()).expect("run function to be called");

        // ---------- pull tokens back out ----------
        let memory: Memory = instance.get_memory(&mut store, "memory").unwrap();
        let bytes = &memory.data(&store)[ptr as usize..(ptr + len * 4) as usize];
        let guest_tokens: Vec<u32> = cast_slice::<u8, u32>(bytes).to_vec();

        // ---------- compute expected ----------
        let expected = call_tokenizer_encode(
            &PresetTokenizers::Mixtral8x7Bv01.to_string(),
            "The cat sat on the mat.",
        );

        assert_eq!(guest_tokens, expected);
    }
}
