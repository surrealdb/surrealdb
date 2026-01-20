# Modules

Here is where we house the rust modules for surrealml.

- **c-wrapper:** Wraps the `core` library in C bindings so the clients can access it.
- **core:** This pure rust module handles the storage, loading, and running of ML models. `Core` is compiled into the SurrealDB server and the `c-wrapper` so the same ML execution code runs on both clients and servers.
- **llms:** This module is currently isolated and doesn't need the onnxruntime to run. The module can execute open-source LLMs. We need a decision on how to integrate this module into the main surrealML. (can compile to WASM)
- **tokenizers:** This module houses tokenizers that are imported into the `llms` module to convert the text inputs into numbers so the `llm` can process the input. (can compile to WASM)
- **transformers:** This module is also not linked at the moment to surrealML. This module just houses the `BERT` model for sentiment analysis. (can compile to WASM)  

- **wasm-linker:** A testing harness to test how the modules behave in WASM. This module is not to be used for production, just a testing harness to test WASM interactions.