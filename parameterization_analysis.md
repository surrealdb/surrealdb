# DEFINE and REMOVE Statement Parameterization Analysis

## Overview

This document analyzes which clauses in DEFINE and REMOVE statements should be parameterized (converted from static values to `Expr` types for runtime evaluation) based on the pattern established in `DEFINE ACCESS`.

## Parameterization Pattern from DEFINE ACCESS

### Key Changes Made:
1. **Field Type Changes**: Static fields → `Expr` types
   - `name: Ident` → `name: Expr`
   - `comment: Option<Strand>` → `comment: Option<Expr>`
   - Duration fields: `Option<val::Duration>` → `Option<Expr>`

2. **Method Signature Changes**: Added `stk: &mut Stk` parameter to `compute` methods

3. **Runtime Evaluation**: Using macros like `process_definition_ident!` and `map_opt!`

4. **Default Implementation**: Added `Default` trait with literal expressions

## Analysis by Statement Type

### DEFINE Statements

#### 1. DEFINE NAMESPACE
**Current Structure:**
```rust
pub struct DefineNamespaceStatement {
    pub kind: DefineKind,           // Static - should stay
    pub id: Option<u32>,           // Static - should stay  
    pub name: Ident,               // SHOULD BE PARAMETERIZED
    pub comment: Option<Strand>,   // SHOULD BE PARAMETERIZED
}
```

**Parameterization Plan:**
- ✅ `name: Ident` → `name: Expr` - Names can be dynamic
- ✅ `comment: Option<Strand>` → `comment: Option<Expr>` - Comments can be dynamic
- ❌ `kind: DefineKind` - Static operation type
- ❌ `id: Option<u32>` - Internal ID, not user-facing

#### 2. DEFINE DATABASE
**Current Structure:**
```rust
pub struct DefineDatabaseStatement {
    pub kind: DefineKind,           // Static - should stay
    pub id: Option<u32>,           // Static - should stay
    pub name: Ident,               // SHOULD BE PARAMETERIZED
    pub comment: Option<Strand>,   // SHOULD BE PARAMETERIZED
    pub changefeed: Option<ChangeFeed>, // Static - should stay
}
```

**Parameterization Plan:**
- ✅ `name: Ident` → `name: Expr` - Database names can be dynamic
- ✅ `comment: Option<Strand>` → `comment: Option<Expr>` - Comments can be dynamic
- ❌ `kind: DefineKind` - Static operation type
- ❌ `id: Option<u32>` - Internal ID
- ❌ `changefeed: Option<ChangeFeed>` - Complex structure, likely static

#### 3. DEFINE TABLE
**Current Structure:**
```rust
pub struct DefineTableStatement {
    pub kind: DefineKind,           // Static - should stay
    pub id: Option<u32>,           // Static - should stay
    pub name: Ident,               // SHOULD BE PARAMETERIZED
    pub drop: bool,                // Static - should stay
    pub full: bool,                // Static - should stay
    pub view: Option<View>,        // Static - should stay
    pub permissions: Permissions,  // Static - should stay
    pub changefeed: Option<ChangeFeed>, // Static - should stay
    pub comment: Option<Strand>,   // SHOULD BE PARAMETERIZED
    pub table_type: TableType,     // Static - should stay
}
```

**Parameterization Plan:**
- ✅ `name: Ident` → `name: Expr` - Table names can be dynamic
- ✅ `comment: Option<Strand>` → `comment: Option<Expr>` - Comments can be dynamic
- ❌ All other fields - Complex structures or static configuration

#### 4. DEFINE FIELD
**Current Structure:**
```rust
pub struct DefineFieldStatement {
    pub kind: DefineKind,           // Static - should stay
    pub name: Idiom,               // SHOULD BE PARAMETERIZED
    pub what: Ident,               // SHOULD BE PARAMETERIZED
    pub flex: bool,                // Static - should stay
    pub field_kind: Option<Kind>,  // Static - should stay
    pub readonly: bool,            // Static - should stay
    pub value: Option<Expr>,       // Already parameterized
    pub assert: Option<Expr>,      // Already parameterized
    pub computed: Option<Expr>,    // Already parameterized
    pub default: DefineDefault,    // Already parameterized (contains Expr)
    pub permissions: Permissions,  // Static - should stay
    pub comment: Option<Strand>,   // SHOULD BE PARAMETERIZED
    pub reference: Option<Reference>, // Static - should stay
}
```

**Parameterization Plan:**
- ✅ `name: Idiom` → `name: Expr` - Field names can be dynamic
- ✅ `what: Ident` → `what: Expr` - Table names can be dynamic
- ✅ `comment: Option<Strand>` → `comment: Option<Expr>` - Comments can be dynamic
- ❌ Other fields - Already parameterized or static

#### 5. DEFINE FUNCTION
**Current Structure:**
```rust
pub struct DefineFunctionStatement {
    pub kind: DefineKind,           // Static - should stay
    pub name: Ident,               // SHOULD BE PARAMETERIZED
    pub args: Vec<(Ident, Kind)>,  // Static - should stay
    pub block: Block,              // Static - should stay
    pub comment: Option<Strand>,   // SHOULD BE PARAMETERIZED
    pub permissions: Permission,   // Static - should stay
    pub returns: Option<Kind>,     // Static - should stay
}
```

**Parameterization Plan:**
- ✅ `name: Ident` → `name: Expr` - Function names can be dynamic
- ✅ `comment: Option<Strand>` → `comment: Option<Expr>` - Comments can be dynamic
- ❌ Other fields - Complex structures or static configuration

#### 6. DEFINE USER
**Current Structure:**
```rust
pub struct DefineUserStatement {
    pub kind: DefineKind,           // Static - should stay
    pub name: Ident,               // SHOULD BE PARAMETERIZED
    pub base: Base,                // Static - should stay
    pub hash: String,              // Static - should stay
    pub code: String,              // Static - should stay
    pub roles: Vec<Ident>,         // SHOULD BE PARAMETERIZED
    pub duration: UserDuration,    // SHOULD BE PARAMETERIZED
    pub comment: Option<Strand>,   // SHOULD BE PARAMETERIZED
}
```

**Parameterization Plan:**
- ✅ `name: Ident` → `name: Expr` - User names can be dynamic
- ✅ `roles: Vec<Ident>` → `roles: Vec<Expr>` - Roles can be dynamic
- ✅ `duration: UserDuration` → Similar to AccessDuration pattern
- ✅ `comment: Option<Strand>` → `comment: Option<Expr>` - Comments can be dynamic
- ❌ Other fields - Security-sensitive or static

#### 7. DEFINE INDEX
**Current Structure:**
```rust
pub struct DefineIndexStatement {
    pub kind: DefineKind,           // Static - should stay
    pub name: Ident,               // SHOULD BE PARAMETERIZED
    pub what: Ident,               // SHOULD BE PARAMETERIZED
    pub cols: Vec<Idiom>,          // SHOULD BE PARAMETERIZED
    pub index: Index,              // Static - should stay
    pub comment: Option<Strand>,   // SHOULD BE PARAMETERIZED
    pub concurrently: bool,        // Static - should stay
}
```

**Parameterization Plan:**
- ✅ `name: Ident` → `name: Expr` - Index names can be dynamic
- ✅ `what: Ident` → `what: Expr` - Table names can be dynamic
- ✅ `cols: Vec<Idiom>` → `cols: Vec<Expr>` - Column references can be dynamic
- ✅ `comment: Option<Strand>` → `comment: Option<Expr>` - Comments can be dynamic
- ❌ Other fields - Static configuration

#### 8. DEFINE ANALYZER
**Current Structure:**
```rust
pub struct DefineAnalyzerStatement {
    pub kind: DefineKind,           // Static - should stay
    pub name: Ident,               // SHOULD BE PARAMETERIZED
    pub tokenizers: Vec<Tokenizer>, // Static - should stay
    pub filters: Vec<Filter>,      // Static - should stay
    pub comment: Option<Strand>,   // SHOULD BE PARAMETERIZED
}
```

**Parameterization Plan:**
- ✅ `name: Ident` → `name: Expr` - Analyzer names can be dynamic
- ✅ `comment: Option<Strand>` → `comment: Option<Expr>` - Comments can be dynamic
- ❌ Other fields - Complex static configuration

#### 9. DEFINE PARAM
**Current Structure:**
```rust
pub struct DefineParamStatement {
    pub kind: DefineKind,           // Static - should stay
    pub name: Ident,               // CANNOT BE PARAMETERIZED
    pub value: Expr,               // Already parameterized
    pub comment: Option<Strand>,   // CANNOT BE PARAMETERIZED
}
```

**Parameterization Plan:**
- ❌ `name: Ident` - **CANNOT be parameterized** - Meta-parameter, defines the parameter system
- ❌ `comment: Option<Strand>` - **CANNOT be parameterized** - Meta-parameter
- ❌ Other fields - Already parameterized or static

**Reason**: DEFINE PARAM statements are meta-parameters that define the parameter system itself. Making them dynamic would create circular dependencies.

#### 10. DEFINE EVENT
**Current Structure:**
```rust
pub struct DefineEventStatement {
    pub kind: DefineKind,           // Static - should stay
    pub name: Ident,               // SHOULD BE PARAMETERIZED
    pub what: Ident,               // SHOULD BE PARAMETERIZED
    pub when: Expr,                // Already parameterized
    pub then: Expr,                // Already parameterized
    pub comment: Option<Strand>,   // SHOULD BE PARAMETERIZED
}
```

**Parameterization Plan:**
- ✅ `name: Ident` → `name: Expr` - Event names can be dynamic
- ✅ `what: Ident` → `what: Expr` - Table names can be dynamic
- ✅ `comment: Option<Strand>` → `comment: Option<Expr>` - Comments can be dynamic
- ❌ Other fields - Already parameterized or static

#### 11. DEFINE MODEL
**Current Structure:**
```rust
pub struct DefineModelStatement {
    pub kind: DefineKind,           // Static - should stay
    pub name: Ident,               // SHOULD BE PARAMETERIZED
    pub version: String,           // SHOULD BE PARAMETERIZED
    pub comment: Option<Strand>,   // SHOULD BE PARAMETERIZED
}
```

**Parameterization Plan:**
- ✅ `name: Ident` → `name: Expr` - Model names can be dynamic
- ✅ `version: String` → `version: Expr` - Versions can be dynamic
- ✅ `comment: Option<Strand>` → `comment: Option<Expr>` - Comments can be dynamic
- ❌ Other fields - Static

#### 12. DEFINE CONFIG
**Current Structure:**
```rust
pub struct DefineConfigStatement {
    pub kind: DefineKind,           // Static - should stay
    pub name: Ident,               // SHOULD BE PARAMETERIZED
    pub value: Expr,               // Already parameterized
    pub comment: Option<Strand>,   // SHOULD BE PARAMETERIZED
}
```

**Parameterization Plan:**
- ✅ `name: Ident` → `name: Expr` - Config names can be dynamic
- ✅ `comment: Option<Strand>` → `comment: Option<Expr>` - Comments can be dynamic
- ❌ Other fields - Already parameterized or static

#### 13. DEFINE API
**Current Structure:**
```rust
pub struct DefineApiStatement {
    pub kind: DefineKind,           // Static - should stay
    pub name: Ident,               // SHOULD BE PARAMETERIZED
    pub base: Base,                // Static - should stay
    pub comment: Option<Strand>,   // SHOULD BE PARAMETERIZED
}
```

**Parameterization Plan:**
- ✅ `name: Ident` → `name: Expr` - API names can be dynamic
- ✅ `comment: Option<Strand>` → `comment: Option<Expr>` - Comments can be dynamic
- ❌ Other fields - Static

#### 14. DEFINE BUCKET
**Current Structure:**
```rust
pub struct DefineBucketStatement {
    pub kind: DefineKind,           // Static - should stay
    pub name: Ident,               // SHOULD BE PARAMETERIZED
    pub comment: Option<Strand>,   // SHOULD BE PARAMETERIZED
}
```

**Parameterization Plan:**
- ✅ `name: Ident` → `name: Expr` - Bucket names can be dynamic
- ✅ `comment: Option<Strand>` → `comment: Option<Expr>` - Comments can be dynamic
- ❌ Other fields - Static

#### 15. DEFINE SEQUENCE
**Current Structure:**
```rust
pub struct DefineSequenceStatement {
    pub kind: DefineKind,           // Static - should stay
    pub name: Ident,               // SHOULD BE PARAMETERIZED
    pub comment: Option<Strand>,   // SHOULD BE PARAMETERIZED
}
```

**Parameterization Plan:**
- ✅ `name: Ident` → `name: Expr` - Sequence names can be dynamic
- ✅ `comment: Option<Strand>` → `comment: Option<Expr>` - Comments can be dynamic
- ❌ Other fields - Static

### REMOVE Statements

#### 1. REMOVE ACCESS
**Current Structure:**
```rust
pub struct RemoveAccessStatement {
    pub name: Ident,               // SHOULD BE PARAMETERIZED
    pub base: Base,                // Static - should stay
    pub if_exists: bool,           // Static - should stay
}
```

**Parameterization Plan:**
- ✅ `name: Ident` → `name: Expr` - Access names can be dynamic
- ❌ Other fields - Static configuration

#### 2. REMOVE TABLE
**Current Structure:**
```rust
pub struct RemoveTableStatement {
    pub name: Ident,               // SHOULD BE PARAMETERIZED
    pub if_exists: bool,           // Static - should stay
    pub expunge: bool,             // Static - should stay
}
```

**Parameterization Plan:**
- ✅ `name: Ident` → `name: Expr` - Table names can be dynamic
- ❌ Other fields - Static configuration

#### 3. REMOVE FIELD
**Current Structure:**
```rust
pub struct RemoveFieldStatement {
    pub name: Idiom,               // SHOULD BE PARAMETERIZED
    pub table_name: Ident,         // SHOULD BE PARAMETERIZED
    pub if_exists: bool,           // Static - should stay
}
```

**Parameterization Plan:**
- ✅ `name: Idiom` → `name: Expr` - Field names can be dynamic
- ✅ `table_name: Ident` → `table_name: Expr` - Table names can be dynamic
- ❌ Other fields - Static configuration

#### 4. REMOVE INDEX
**Current Structure:**
```rust
pub struct RemoveIndexStatement {
    pub name: Ident,               // SHOULD BE PARAMETERIZED
    pub what: Ident,               // SHOULD BE PARAMETERIZED
    pub if_exists: bool,           // Static - should stay
}
```

**Parameterization Plan:**
- ✅ `name: Ident` → `name: Expr` - Index names can be dynamic
- ✅ `what: Ident` → `what: Expr` - Table names can be dynamic
- ❌ Other fields - Static configuration

#### 5. REMOVE USER
**Current Structure:**
```rust
pub struct RemoveUserStatement {
    pub name: Ident,               // SHOULD BE PARAMETERIZED
    pub base: Base,                // Static - should stay
    pub if_exists: bool,           // Static - should stay
}
```

**Parameterization Plan:**
- ✅ `name: Ident` → `name: Expr` - User names can be dynamic
- ❌ Other fields - Static configuration

#### 6. REMOVE FUNCTION
**Current Structure:**
```rust
pub struct RemoveFunctionStatement {
    pub name: Ident,               // SHOULD BE PARAMETERIZED
    pub if_exists: bool,           // Static - should stay
}
```

**Parameterization Plan:**
- ✅ `name: Ident` → `name: Expr` - Function names can be dynamic
- ❌ Other fields - Static configuration

#### 7. REMOVE ANALYZER
**Current Structure:**
```rust
pub struct RemoveAnalyzerStatement {
    pub name: Ident,               // SHOULD BE PARAMETERIZED
    pub if_exists: bool,           // Static - should stay
}
```

**Parameterization Plan:**
- ✅ `name: Ident` → `name: Expr` - Analyzer names can be dynamic
- ❌ Other fields - Static configuration

#### 8. REMOVE PARAM
**Current Structure:**
```rust
pub struct RemoveParamStatement {
    pub name: Ident,               // CANNOT BE PARAMETERIZED
    pub if_exists: bool,           // Static - should stay
}
```

**Parameterization Plan:**
- ❌ `name: Ident` - **CANNOT be parameterized** - Meta-parameter, defines the parameter system
- ❌ Other fields - Static configuration

**Reason**: REMOVE PARAM statements are meta-parameters that define the parameter system itself. Making them dynamic would create circular dependencies.

#### 9. REMOVE EVENT
**Current Structure:**
```rust
pub struct RemoveEventStatement {
    pub name: Ident,               // SHOULD BE PARAMETERIZED
    pub what: Ident,               // SHOULD BE PARAMETERIZED
    pub if_exists: bool,           // Static - should stay
}
```

**Parameterization Plan:**
- ✅ `name: Ident` → `name: Expr` - Event names can be dynamic
- ✅ `what: Ident` → `what: Expr` - Table names can be dynamic
- ❌ Other fields - Static configuration

#### 10. REMOVE MODEL
**Current Structure:**
```rust
pub struct RemoveModelStatement {
    pub name: Ident,               // SHOULD BE PARAMETERIZED
    pub version: String,           // SHOULD BE PARAMETERIZED
    pub if_exists: bool,           // Static - should stay
}
```

**Parameterization Plan:**
- ✅ `name: Ident` → `name: Expr` - Model names can be dynamic
- ✅ `version: String` → `version: Expr` - Versions can be dynamic
- ❌ Other fields - Static configuration

#### 11. REMOVE NAMESPACE
**Current Structure:**
```rust
pub struct RemoveNamespaceStatement {
    pub name: Ident,               // SHOULD BE PARAMETERIZED
    pub if_exists: bool,           // Static - should stay
    pub expunge: bool,             // Static - should stay
}
```

**Parameterization Plan:**
- ✅ `name: Ident` → `name: Expr` - Namespace names can be dynamic
- ❌ Other fields - Static configuration

#### 12. REMOVE DATABASE
**Current Structure:**
```rust
pub struct RemoveDatabaseStatement {
    pub name: Ident,               // SHOULD BE PARAMETERIZED
    pub if_exists: bool,           // Static - should stay
    pub expunge: bool,             // Static - should stay
}
```

**Parameterization Plan:**
- ✅ `name: Ident` → `name: Expr` - Database names can be dynamic
- ❌ Other fields - Static configuration

#### 13. REMOVE BUCKET
**Current Structure:**
```rust
pub struct RemoveBucketStatement {
    pub name: Ident,               // SHOULD BE PARAMETERIZED
    pub if_exists: bool,           // Static - should stay
}
```

**Parameterization Plan:**
- ✅ `name: Ident` → `name: Expr` - Bucket names can be dynamic
- ❌ Other fields - Static configuration

#### 14. REMOVE SEQUENCE
**Current Structure:**
```rust
pub struct RemoveSequenceStatement {
    pub name: Ident,               // SHOULD BE PARAMETERIZED
    pub if_exists: bool,           // Static - should stay
}
```

**Parameterization Plan:**
- ✅ `name: Ident` → `name: Expr` - Sequence names can be dynamic
- ❌ Other fields - Static configuration

## Implementation Strategy

### Macro Usage:
1. **For `Ident` fields**: Use existing `process_definition_ident!` macro
2. **For `Idiom` fields**: Create new `process_definition_idiom!` macro
3. **For `Vec<Ident/Idiom>` fields**: Convert to `Vec<Expr>`, iterate and use appropriate macros
4. **For `Vec<Expr>` with array support**: Consider accepting array values with strings for convenience

### Special Cases:

#### Column References (`cols` field):
- Accept either `Expr::Idiom` directly
- Or use `type::field` and `type::fields` functions (like in SELECT statements)
- Analyze the expressions, extract string arguments, and parse them
- This allows dynamic column specification while maintaining type safety

#### Parameter Statements (DEFINE/REMOVE PARAM):
- **CANNOT be parameterized** - The syntax itself is already a parameter
- These are meta-parameters that define the parameter system itself
- Must remain static to avoid circular dependency

### Updated Field Classification:

#### Fields That SHOULD Be Parameterized:
1. **Names/Identifiers**: `name`, `what`, `table_name` - Use `process_definition_ident!`
2. **Comments**: `comment` - Use `process_definition_ident!` 
3. **Versions**: `version` - Use `process_definition_ident!`
4. **Roles**: `roles: Vec<Ident>` → `Vec<Expr>` - Iterate and use `process_definition_ident!`
5. **Durations**: `duration` fields - Use `map_opt!` pattern from ACCESS
6. **Column References**: `cols: Vec<Idiom>` → `Vec<Expr>` - Special handling for field extraction
7. **Field Names**: `name: Idiom` → `Expr` - Use new `process_definition_idiom!` macro

#### Fields That Should STAY Static:
1. **Operation Types**: `kind`, `if_exists`, `expunge` - Control flow
2. **Internal IDs**: `id` - System-generated
3. **Complex Structures**: `permissions`, `view`, `changefeed` - Complex configuration
4. **Boolean Flags**: `drop`, `full`, `flex`, `readonly` - Simple configuration
5. **Security Fields**: `hash`, `code` - Security-sensitive
6. **Already Parameterized**: Fields that are already `Expr` types
7. **Parameter Statements**: DEFINE/REMOVE PARAM - Meta-parameters, cannot be dynamic

### Implementation Priority:
1. **High Priority**: Names, comments, versions, roles, durations
2. **Medium Priority**: Column references, field names (Idiom → Expr)
3. **Low Priority**: Complex nested structures
4. **Excluded**: Parameter statements (DEFINE/REMOVE PARAM)

### New Macros Needed:
```rust
// For processing Idiom fields
macro_rules! process_definition_idiom {
    ($stk:ident, $ctx:ident, $opt:ident, $doc:ident, $x:expr, $into:expr) => {
        match $x {
            crate::expr::Expr::Idiom(x) => x.to_raw_string(),
            x => match $stk
                .run(|stk| x.compute(stk, $ctx, $opt, $doc))
                .await
                .catch_return()?
                .coerce_to::<String>()
            {
                Err(crate::val::value::CoerceError::InvalidKind {
                    from,
                    ..
                }) => Err(crate::val::value::CoerceError::InvalidKind {
                    from,
                    into: $into.to_string(),
                }),
                x => x,
            }?,
        }
    };
}
```

This updated strategy provides a more nuanced approach that handles the special cases while maintaining the flexibility of dynamic parameterization where appropriate.
