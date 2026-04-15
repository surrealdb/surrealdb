# Surrealism change review checklist

Use this checklist when reviewing Surrealism-related changes (host runtime, guest SDK, WIT, package format, and integration points).

---

## Security

- [ ] **Capability enforcement**: Module capabilities are checked both at load time and at runtime where needed (e.g. `allow_arbitrary_queries` before `sql()`, `allow_functions` before `run()`). Server capabilities are never exceeded.
- [ ] **Deny-all defaults**: Capability fields default to deny (`#[serde(default)]`). Omitted `allow_functions` means no function calls. Explicit opt-in only.
- [ ] **Path traversal protection**: Archive extraction rejects entries with `..` components. FS roots are canonicalised; module FS access is sandboxed.
- [ ] **No credentials in logs**: Auth tokens, passwords, and other secrets are never logged. Scrub values before tracing.

---

## Performance

- [ ] **Minimize allocations in hot paths**: Host/guest crossings, serialization, and per-invocation logic should avoid unnecessary allocations. Prefer re-use and streaming where feasible.
- [ ] **Pool lifecycle**: Controllers are pooled and reused. Avoid per-request instantiation when possible. Pool size and `max_memory_bytes` are respected.
- [ ] **Epoch overhead**: Epoch ticker runs at a fixed interval. Epoch deadline computation must not overflow (`u64::MAX - current_epoch`). Keep tick granularity appropriate for timeout accuracy.

---

## Serialization boundary

- [ ] **FlatBuffers at every crossing**: All structured data crossing host/guest uses FlatBuffers via `surrealdb_types::{encode, decode}`. No ad hoc serialization.
- [ ] **Type safety**: Schemas in `surrealdb/types/src/flatbuffers/` must stay in sync between host and guest. Changes require coordinated updates.

---

## Test coverage

- [ ] **Unit tests**: Add tests for capabilities validation, config parsing, package load/unpack, and exports manifest round-trip.
- [ ] **Integration tests**: Verify function calls (default and named), KV operations, FS access, and timeouts (epoch interrupt) in `tests/surrealism_integration.rs` or equivalent.

---

## Common pitfalls

- [ ] **Mutex poisoning**: Use `parking_lot::Mutex` (not `std::sync::Mutex`) to avoid poisoning and reduce overhead. No panic recovery across lock boundaries.
- [ ] **Epoch overflow**: Passing `u64::MAX` to `set_epoch_deadline` overflows when epoch > 0. Use `epoch_deadline_max()` which computes `u64::MAX - epoch - 1`.
- [ ] **WasmTrap handling**: After a WASM trap, **drop** the controller — do **not** call `release_controller`. Trapped instances may have inconsistent state; returning them to the pool risks cross-request leaks.

---

## Protocol stability

- [ ] **WIT changes require ABI version bump**: Modifying `surrealism.wit` or `sdk.wit` changes the component model ABI. Bump version strings and ensure host/guest compatibility.
- [ ] **Backwards compatibility**: Avoid breaking changes to `.surli` layout (`surrealism/mod.wasm`, `surrealism/surrealism.toml`, `surrealism/exports.toml`, `surrealism/fs/`). If breaking, document migration and version clearly.
