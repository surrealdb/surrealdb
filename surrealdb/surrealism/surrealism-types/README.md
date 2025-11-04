# surrealism-types

A language-agnostic serialization framework for WebAssembly (WASM) guest-host communication in SurrealDB's Surrealism ecosystem.

## Overview

`surrealism-types` provides a custom binary serialization protocol designed to enable SurrealDB client modules to be built in any language that compiles to WebAssembly. By defining a well-specified wire format and memory transfer protocol, this crate allows different programming languages to communicate seamlessly across WASM boundaries.

## Why Custom Serialization?

Instead of using standard serialization frameworks like Serde with JSON or bincode, `surrealism-types` implements a custom protocol for several key reasons:

1. **Language Agnostic**: Any language (Rust, Go, C, AssemblyScript, etc.) can implement the same binary protocol
2. **WASM-Optimized**: Designed specifically for WebAssembly's linear memory model
3. **Zero-Copy Operations**: Uses `bytes::Bytes` to minimize memory allocations
4. **Dual-Mode Support**: Works both synchronously (guest) and asynchronously (host)
5. **Fine-Grained Control**: Complete control over wire format ensures stability across versions

## Architecture

### Core Components

#### 1. Memory Transfer Layer (`transfer.rs`)

The foundation of cross-boundary communication:

```rust
// Guest side (sync)
pub trait Transfer {
    fn transfer(self, controller: &mut dyn MemoryController) -> Result<Ptr>;
    fn receive(ptr: Ptr, controller: &mut dyn MemoryController) -> Result<Self>;
}

// Host side (async)
#[async_trait]
pub trait AsyncTransfer {
    async fn transfer(self, controller: &mut dyn AsyncMemoryController) -> Result<Ptr>;
    async fn receive(ptr: Ptr, controller: &mut dyn AsyncMemoryController) -> Result<Self>;
}
```

- **`Ptr`**: Type-safe wrapper around `u32` pointers for WASM linear memory
- **`Transfer`**: Synchronous trait for WASM guest-side operations
- **`AsyncTransfer`**: Asynchronous trait for host-side (Wasmtime runtime) operations

#### 2. Memory Management (`controller.rs`)

Abstracts memory allocation across guest and host:

```rust
// Guest side (sync)
pub trait MemoryController {
    fn alloc(&mut self, len: u32, align: u32) -> Result<u32>;
    fn free(&mut self, ptr: u32, len: u32) -> Result<()>;
    fn mut_mem(&mut self, ptr: u32, len: u32) -> &mut [u8];
}

// Host side (async)
#[async_trait]
pub trait AsyncMemoryController {
    async fn alloc(&mut self, len: u32, align: u32) -> Result<u32>;
    async fn free(&mut self, ptr: u32, len: u32) -> Result<()>;
    fn mut_mem(&mut self, ptr: u32, len: u32) -> &mut [u8];
}
```

#### 3. Serialization Layer (`serialize.rs`)

The core binary protocol:

```rust
pub trait Serializable: Sized {
    fn serialize(self) -> Result<Serialized>;
    fn deserialize(serialized: Serialized) -> Result<Self>;
}
```

## Wire Format Specification

### Basic Layout

All serialized data follows a length-prefixed pattern when transferred:
```
[4-byte length (u32, little-endian)][data bytes]
```

### Primitive Types

| Type | Format | Size |
|------|--------|------|
| `String` | Raw UTF-8 bytes | Variable |
| `f64` | Little-endian IEEE 754 | 8 bytes |
| `u64` | Little-endian unsigned | 8 bytes |
| `i64` | Little-endian signed | 8 bytes |
| `bool` | Single byte (0=false, 1=true) | 1 byte |
| `()` | Empty | 0 bytes |

### Enum Types

All enums use a tag byte followed by optional payload:

#### `Option<T>`
```
Some(T): [0x00][serialized T]
None:    [0x01]
```

#### `Result<T, E>`
```
Ok(T):  [0x00][serialized T]
Err(E): [0x01][serialized E]
```

#### `Bound<T>`
```
Unbounded:       [0x00]
Included(T):     [0x01][serialized T]
Excluded(T):     [0x02][serialized T]
```

### Collection Types

#### `Vec<T>`
```
[4-byte count (u32)]
[4-byte item1_len][item1_data]
[4-byte item2_len][item2_data]
...
```

#### Tuples (1-10 elements)
```
[4-byte elem1_len][elem1_data]
[4-byte elem2_len][elem2_data]
...
```

#### `SerializableRange<T>`
```
[4-byte beg_len][4-byte end_len][beg_data][end_data]
```

### Complex Types

#### `surrealdb_types::Value` and `Kind`
These types use **FlatBuffers** serialization via the `surrealdb-protocol` crate:
```
[FlatBuffers-encoded data]
```

This leverages the existing SurrealDB binary protocol for complex database types like records, geometries, and durations.

## Feature Flags

### `host`

Enable this feature for host-side (runtime) code:
```toml
[dependencies]
surrealism-types = { version = "*", features = ["host"] }
```

When enabled:
- Makes traits async (`AsyncTransfer`, `AsyncMemoryController`)
- Adds `Send` bounds for thread-safe async operations
- Required for Wasmtime/runtime implementations

When disabled (default):
- All traits are synchronous
- No async/await overhead
- Suitable for WASM guest modules

## Usage Examples

### Guest Side (WASM Module)

```rust
use surrealism_types::{Serializable, Transfer, MemoryController};

// Serialize and transfer a value across WASM boundary
fn export_value(value: String, controller: &mut dyn MemoryController) -> Result<u32> {
    let ptr = value.transfer(controller)?;
    Ok(*ptr)
}

// Receive and deserialize a value from host
fn import_value(ptr: u32, controller: &mut dyn MemoryController) -> Result<String> {
    String::receive(ptr.into(), controller)
}
```

### Host Side (Runtime)

```rust
use surrealism_types::{Serializable, AsyncTransfer, AsyncMemoryController};

// Transfer a value to WASM module
async fn send_to_guest(
    value: String,
    controller: &mut dyn AsyncMemoryController
) -> Result<u32> {
    let ptr = value.transfer(controller).await?;
    Ok(*ptr)
}

// Receive a value from WASM module
async fn receive_from_guest(
    ptr: u32,
    controller: &mut dyn AsyncMemoryController
) -> Result<String> {
    String::receive(ptr.into(), controller).await
}
```

### Function Arguments

```rust
use surrealism_types::{Args, args::Args};
use surrealdb_types::SurrealValue;

// Define function with typed arguments
fn my_function<T, U>(args: (T, U)) -> Result<()>
where
    T: SurrealValue,
    U: SurrealValue,
{
    // Convert args to SurrealDB Values
    let values = args.to_values();
    
    // ... use values ...
    
    Ok(())
}

// Reconstruct typed arguments from Values
let args: (String, i64) = Args::from_values(values)?;
```

## Implementation Guide

To implement this protocol in another language:

1. **Implement Memory Management**
   - Provide allocator that returns aligned pointers
   - Track allocations for proper cleanup
   - Implement the memory controller interface

2. **Implement Wire Format**
   - Follow the exact byte layouts specified above
   - Use little-endian for all multi-byte integers
   - Respect alignment requirements (typically 8 bytes)

3. **Implement Type Marshalling**
   - Map your language's types to the wire format
   - Handle length prefixes correctly
   - Implement tag-based enum discrimination

4. **Test Cross-Language Compatibility**
   - Serialize data in one language
   - Deserialize in another
   - Verify round-trip equality

## Memory Safety

The protocol includes several safety mechanisms:

- **Bounds Checking**: All memory accesses are bounds-checked
- **Alignment**: Allocations respect alignment requirements
- **Cleanup**: Proper deallocation via `free()` prevents leaks
- **Type Safety**: `Ptr` wrapper prevents raw pointer confusion

## Performance Considerations

- **Zero-Copy**: `bytes::Bytes` enables efficient memory sharing
- **Minimal Allocations**: Length-prefixed format reduces reallocation
- **Batch Transfers**: Multiple values can be transferred in one allocation
- **FlatBuffers**: Complex types use efficient binary format

## Contributing

When adding new `Serializable` implementations:

1. Document the wire format in comments
2. Include serialization tests
3. Verify round-trip correctness
4. Consider endianness (always use little-endian)
5. Update this README with new type specifications

## Related Crates

- **surrealism-runtime**: Host-side WASM runtime implementation
- **surrealism**: Main API surface for building WASM modules
- **surrealism-macros**: Procedural macros for deriving traits
- **surrealdb-protocol**: FlatBuffers schema for SurrealDB types
- **surrealdb-types**: Core SurrealDB type system

## License

See the main SurrealDB repository for license information.

