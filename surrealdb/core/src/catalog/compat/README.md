# Catalog Backwards Compatibility Tests

This module ensures that serialized catalog data from previous SurrealDB versions can still be correctly deserialized by the current code. This is critical for database upgrades and data integrity.

## Purpose

1. **Detect breaking serialization changes** - If we accidentally change how a type is serialized, old databases won't load
2. **Detect parsing behavior changes** - Even if deserialization succeeds, subtle changes in how data is interpreted can cause bugs
3. **Enforce migration paths** - When intentional changes are made, tests fail loudly to ensure proper migrations are implemented

## Architecture

```
compat/
├── README.md           # This file
├── mod.rs              # Module entry point
├── fixtures.rs         # Source of truth: expected values for each test case
├── generator.rs        # Tool to serialize fixtures into byte arrays
├── v3_0_0.rs           # FROZEN: Serialized bytes from version 3.0.0
└── tests.rs            # Tests that decode bytes and assert equality
```

### Data Flow

```
fixtures.rs ──┬──> generator.rs ──> v3_0_0.rs (frozen bytes)
              │
              └──> tests.rs <────── v3_0_0.rs
                       │
                       └──> assert_eq!(decoded, expected)
```

## File Descriptions

### `fixtures.rs`
Contains functions that return expected values for each catalog type. This is the **single source of truth** used both:
- By the generator to create serialized bytes
- By tests to assert decoded values match expectations

Example:
```rust
pub fn namespace_basic() -> NamespaceDefinition {
    NamespaceDefinition {
        namespace_id: NamespaceId(1),
        name: "test".to_string(),
        comment: None,
    }
}
```

### `v3_0_0.rs`
Contains pre-serialized byte arrays representing the exact format from version 3.0.0. 

**⚠️ NEVER modify this file after it's committed.** It represents a snapshot of the serialization format that must remain unchanged. A hash check test ensures this file hasn't been tampered with.

### `generator.rs`
Tool to generate the byte arrays by serializing the fixtures. Outputs Rust code to stdout that can be copied to a version file.

### `tests.rs`
Contains test cases that:
1. Deserialize bytes from `v3_0_0.rs`
2. Compare against expected values from `fixtures.rs`
3. Fail loudly if either step fails

## How to Generate `v3_0_0.rs`

**Note:** You should only need to regenerate this file if you're setting up the initial fixtures or fixing a bug in the generator. Once committed, this file is frozen.

### Step 1: Run the generator

```bash
cargo test -p surrealdb-core --lib catalog::compat::generator::generator -- --ignored --nocapture
```

This runs the `generator` test (which is marked `#[ignore]`) and prints the generated Rust code to stdout.

### Step 2: Copy the output

The output will look like:
```rust
// v3_0_0.rs - Generated file, DO NOT EDIT
//! Catalog compatibility fixtures for SurrealDB 3.0.0
// ...
pub const NAMESPACE_BASIC: &[u8] = &[
    0x00, 0x01, ...
];
```

Copy everything from `// v3_0_0.rs` to the end and paste it into `v3_0_0.rs`.

### Step 3: Update the hash check

After modifying `v3_0_0.rs`, the hash check test will fail. Update the expected hash in `generator.rs`:

```bash
# Run the hash check test to see the new hash
cargo test -p surrealdb-core --lib catalog::compat::generator::test_v3_0_0_remains_unchanged -- --nocapture
```

The error message will show the actual hash - update the assertion in `generator.rs`.

### Step 4: Verify tests pass

```bash
cargo test -p surrealdb-core --lib catalog::compat::tests
```

All tests should pass.

## How to Add a New Version (e.g., v3_1_0)

When releasing a new major or minor version, create a new frozen snapshot of the serialization format.

### Step 1: Update the fixtures used in the generator (if needed)

Add a new generator test in `generator.rs`:

```rust
#[test]
#[ignore]
fn generator_v3_1_0() {
    let output = generate_all_fixtures();
    // Update the header comment in generate_all_fixtures() or create a parameterized version
    println!("{}", output);
}
```

### Step 2: Generate and save the bytes

```bash
cargo test -p surrealdb-core --lib catalog::compat::generator::generator -- --ignored --nocapture > /tmp/v3_1_0.rs
```

Copy the relevant output to `surrealdb/core/src/catalog/compat/v3_1_0.rs`.

### Step 3: Add the module

In `mod.rs`, add:
```rust
mod v3_1_0;
```

### Step 4: Add tests

In `tests.rs`, add test cases for the new version:
```rust
use super::v3_1_0 as bytes_v3_1_0;

// ... add compat_test! calls for v3_1_0 fixtures
```

### Step 5: Add hash protection

Add a hash check test for the new version in `generator.rs` to prevent accidental modifications.

## Maintenance Guidelines

### When types evolve

- **`v3_0_0.rs`**: NEVER modify. The bytes represent exactly what was serialized in 3.0.0.
- **`fixtures.rs`**: MAY be updated to reflect how old data should be interpreted by current code.

For example, if a field is added with a default value:
1. The old bytes remain unchanged (they don't contain the new field)
2. Update `fixtures.rs` to include the default value that `revisioned` will deserialize to

### When adding new catalog types

1. Add fixture functions to `fixtures.rs`
2. Add entries to the appropriate `*_fixtures()` function in `generator.rs`
3. Add test cases to `tests.rs`
4. Regenerate the version file (only for new unreleased versions)

### When tests fail

A failing test indicates one of:
1. **Accidental serialization change** - Fix the code to maintain compatibility
2. **Intentional breaking change** - Implement a proper migration path and update fixtures
3. **Bug in test setup** - Verify fixtures match what was actually serialized

## Running Tests

```bash
# Run all compatibility tests
cargo test -p surrealdb-core --lib catalog::compat::tests

# Run with output
cargo test -p surrealdb-core --lib catalog::compat::tests -- --nocapture

# Run a specific test
cargo test -p surrealdb-core --lib catalog::compat::tests::v3_0_0_namespace_basic
```

