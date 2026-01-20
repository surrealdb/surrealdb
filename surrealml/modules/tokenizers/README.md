# SurrealML Tokenizers

This crate is an interface to hugging face tokenizer files. It allows us to easily pull tokenizer files from hugging facen and load them into **Tokenizer** objects.


## Why tokenizers? 

Large-language-model text must be converted to integer “tokens” before a model can
process it. A tokenizer defines that mapping (encode) and its reverse (decode),
and different models use different rules.


## Installation

Add to *Cargo.toml*:

```toml
[dependencies]
surrealml-tokenizers = "0.1"
```

The library is re-exported under `surrealml_transformers` (see `[lib]` in
*Cargo.toml*), so you `use surrealml_transformers::...` in code.


## Optional features

| Feature            | What it does                                                                           |
|--------------------|----------------------------------------------------------------------------------------|
| `http-access`      | Enables tokenizer downloads via `hf-hub/tokio`.                                        |

**Note** - when `http-access` is enabled and used with the `cargo test` command, we run a full integration test 
where we pull remotely from hf-hub.

## Quick start

```rust
use surrealml_transformers::{load_tokenizer, encode, decode};

fn main() -> anyhow::Result<()> {
    // 1. Load a tokenizer (preset or HF repo id)
    let tok = load_tokenizer("gpt2".into(), None)?; // No HF access token as this is a public repo

    // 2. Encode some text
    let ids = encode(&tok, "Hello world!")?;
    println!("Token IDs: {ids:?}");

    // 3. Decode back
    let text = decode(&tok, &ids)?;
    println!("Round-trip: {text}");

    Ok(())
}
```


## Built-in vs remote tokenizers 

**Built-in tokenizers**

| Preset enum       | `model` string to pass         | Embedded file                                           |
|-------------------|--------------------------------|---------------------------------------------------------|
| `Mixtral8x7Bv01`  | `mistralai/Mixtral-8x7B-v0.1`  | `tokenizers/mistralai-Mixtral-8x7B-v0.1-tokenizer.json` |
| `Mistral7Bv01`    | `mistralai/Mistral-7B-v0.1`    | `tokenizers/mistralai-Mistral-7B-v0.1-tokenizer.json`   |
| `MistralLite`     | `amazon/MistralLite`           | `tokenizers/amazon-MistralLite-tokenizer.json`          |
| `Gemma7B`         | `google/gemma-7b`              | `tokenizers/google-gemma-7b-tokenizer.json`             |
| `Gemma2B`         | `google/gemma-2b`              | `tokenizers/google-gemma-2b-tokenizer.json`             |
| `Gemma3_4BIt`     | `google/gemma-3-4b-it`         | `tokenizers/google-gemma-3-4b-it-tokenizer.json`        |
| `Falcon7B`        | `tiiuae/falcon-7b`             | `tokenizers/tiiuae-falcon-7b-tokenizer.json`            |


**Remote tokenizers**
Anything else (e.g. `"meta-llama/Meta-Llama-3-8B"`) triggers a download. So if we did the below, we'd pull the `tokenizer.json` file from hugging face and cache it in the standard HF directory `~/.cache/huggingface/hub.

```rust
let tok = load_tokenizer(
    "meta-llama/Meta-Llama-3-8B".to_string(),
    Some("hf_XXXXXXXXXXXXXXXX".to_string())   // Pass in HF token if gated model
)?;
```


## `scripts/tokenizer_download.sh` 

A convenience script that bulk-downloads the **public** presets to
`./tokenizers/` so they are bundled into the crate on the next build
(handy for offline environments).

```bash
# Pass token as arg …
./scripts/tokenizer_download.sh hf_XXXXXXXXXXXXXXXX

# …or via env var.
export HF_TOKEN=hf_XXXXXXXXXXXXXXXX
./scripts/tokenizer_download.sh
```

What it does:

1. Creates a `tokenizers/` folder next to the script.
2. Downloads `tokenizer.json` for each model in its `models=( … )` array.
3. Writes files like `gpt2-tokenizer.json`, `EleutherAI-gpt-neo-125M-tokenizer.json`, …


## Running tests

| Command                                             | What runs                                              |
|-----------------------------------------------------|--------------------------------------------------------|
| `cargo test`                                        | All **unit & offline** tests (only presets).           |
| `cargo test --features http-access`                 | Adds an **integration** test that fetches `gpt2` live. |


## Public API summary 

| Function / enum                                | Use-case                                                                                        |
|------------------------------------------------|-------------------------------------------------------------------------------------------------|
| `load_tokenizer(model, hf_token)`              | Load preset or remote tokenizer (if `http-access` enabled), returning `tokenizers::Tokenizer`.  |
| `encode(&tokenizer, text)`                     | Convert `&str` → `Vec<u32>` token IDs.                                                          |
| `decode(&tokenizer, ids)`                      | Convert token IDs back to `String`.                                                             |


## Future improvements

* Custom cache directory – allow users to override the HF cache path.
* Lazy global `Api` instance – share a single `hf_hub::Api` across calls.
* Trait-based hugging face hub fetcher – make network access swappable for easy mocking in tests.
* Add error macros to reduce repetitiveness.


## License 

See the *LICENSE* file for details.
