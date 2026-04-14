# Surrealism Macros

Essential context for AI assistants working with the proc macros.

## Architecture

`#[surrealism]` applies to functions and modules. It expands into inventory registration plus WIT-compatible exports. The macro is attribute-based; parsing handles both `#[surrealism]` and `#[surrealism(...)]` forms.

## Attribute Parsing

Supported args (in `attr.rs`):

- **default** — marks the default (unnamed) export
- **name = "..."** — override export name; segments separated by `::`
- **init** — marks the module init hook (runs once after instantiation)

Invalid: `default` on modules, `init` inside modules.

## Sentinel System

Compile-time duplicate detection via unique const names:

- Default export → `__sr_export_default`
- Named export → `__sr_export__` + encoded name
- Encoding: `_` → `_u`, `::` → `_s` (so `_s` in names becomes `_us`)

Duplicate export names produce identical const identifiers and thus link errors.

## Export Naming

- **Default:** `#[surrealism(default)]` → `None` (unnamed)
- **Named:** `#[surrealism(name = "foo")]` → `Some("foo")`
- **Modules:** `#[surrealism] mod bar { ... }` → prefixed names like `bar::baz` for nested items

## Module Recursion

Nested `#[surrealism]` mods are processed recursively. Each mod's prefix is `parent_prefix::segment`. Functions inside get names like `prefix::fn_name`; nested mods extend the prefix.

## File Structure

| File | Purpose |
|------|---------|
| `lib.rs` | Entry: `#[proc_macro_attribute] fn surrealism`, dispatch to `handle_function` or `handle_module` |
| `attr.rs` | `parse_surrealism_attrs`, `parse_surrealism_attr`, `validate_export_name` |
| `extract.rs` | `extract_fn_signature` — arg patterns, types, return type, Result detection |
| `generate.rs` | `generate_sentinel`, `generate_registration_body`, `sentinel_const_name` |
| `handler.rs` | `handle_function`, `handle_module`, `process_mod_items` |

## Generated Code

For each exported function (non-init):

- `__sr_invoke_*` — decodes args, calls user fn, encodes result
- `__sr_args_*` — returns kind list
- `__sr_returns_*` — returns kind
- `inventory::submit!(SurrealismEntry { ... })`

For init:

- `__sr_init_*` — wrapper that calls user fn
- `inventory::submit!(SurrealismInit(...))`
