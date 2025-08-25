# SurrealDB Types

This crate provides the shared public value type system for SurrealDB. It serves as a foundational layer that defines all the data types that can be stored and manipulated in SurrealDB.

## Purpose

The `surrealdb-types` crate acts as a shared public value type system that:

- **Provides type safety**: Offers strongly-typed representations of all SurrealDB data types
- **Enables serialization**: Supports serialization/deserialization of values
- **Facilitates type conversion**: Provides traits and methods for converting between Rust types and SurrealDB values

## Core Types

### Value Types

The main `Value` enum represents all possible data types in SurrealDB:

```rust
use surrealdb_types::Value;

// Basic types
let bool_val = Value::Bool(true);
let string_val = Value::String("hello".to_string());
let number_val = Value::Number(Number::Int(42));

// Complex types
let array_val = Value::Array(Array(vec![Value::String("item".to_string())]));
let object_val = Value::Object(Object(BTreeMap::new()));
let datetime_val = Value::Datetime(Datetime(chrono::Utc::now()));
```

### Type System

The `Kind` enum represents the type system used for schema validation and type checking:

```rust
use surrealdb_types::Kind;

// Basic kinds
let string_kind = Kind::String;
let number_kind = Kind::Number;
let array_kind = Kind::Array(Box::new(Kind::String), Some(10)); // Array of strings, max 10 items
```

## Key Features

### Type Conversion

The `SurrealValue` trait provides type-safe conversion between Rust types and SurrealDB values:

```rust
use surrealdb_types::{SurrealValue, Value};

// Convert from Rust type to SurrealDB value
let value: Value = "hello".into_value();

// Check if a value is of a specific type
if value.is::<String>() {
    println!("Value is a string");
}

// Convert from SurrealDB value to Rust type
if let Some(string) = value.into::<String>() {
    println!("Extracted string: {}", string);
}
```

### Geometric Types

Support for spatial data types using the `geo` crate:

```rust
use surrealdb_types::{Geometry, Value};
use geo::Point;

let point = Point::new(1.0, 2.0);
let geometry_val = Value::Geometry(Geometry::Point(point));
```

### Record Identifiers

Type-safe representation of SurrealDB record identifiers:

```rust
use surrealdb_types::{RecordId, RecordIdKey, Value};

let record_id = RecordId {
    table: "person".to_string(),
    key: RecordIdKey::String("john".to_string()),
};
let record_val = Value::RecordId(record_id);
```

## Architecture

This crate is designed with the following principles:

- **Comprehensive type coverage**: Supports all SurrealDB data types
- **Serialization support**: Works with serde for data exchange
- **Extensible**: Easy to add new types or modify existing ones
- **Type safe**: Compile-time guarantees for type conversions

## Usage

### Basic Usage

```rust
use surrealdb_types::{Value, Number, Array, Object};
use std::collections::BTreeMap;

// Create values
let values = vec![
    Value::Bool(true),
    Value::Number(Number::Int(42)),
    Value::String("hello".to_string()),
    Value::Array(Array(vec![Value::String("item".to_string())])),
];

// Work with objects
let mut map = BTreeMap::new();
map.insert("key".to_string(), Value::String("value".to_string()));
let object = Value::Object(Object(map));
```

### object & array macros

```rust
use surrealdb_types::{object, array};

let values = array![
    true,
    42,
    "hello".to_string(),
    array!["item1".to_string()],
];

let map = object! {
    key: "value".to_string(),
};
```

### Type Checking

```rust
use surrealdb_types::{Value, SurrealValue};

fn process_value(value: &Value) {
    match value {
        Value::String(s) => println!("String: {}", s),
        Value::Number(n) => println!("Number: {:?}", n),
        Value::Array(arr) => println!("Array with {} items", arr.0.len()),
        _ => println!("Other type"),
    }
}
```

## Dependencies

This crate has minimal external dependencies:

- `serde`: For serialization/deserialization
- `chrono`: For datetime handling
- `uuid`: For UUID support
- `rust_decimal`: For decimal number support
- `regex`: For regular expression support
- `geo`: For geometric types

## License

This crate is part of SurrealDB and follows the same licensing terms.
