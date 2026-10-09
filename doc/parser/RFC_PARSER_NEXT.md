# RFC: a size-first SurrealQL front end

Status: accepted direction. The new parser replaces both `surrealdb-syn`
(`surrealdb/syn`) and the v3 parser (`surrealdb-parser`, `surrealdb-ast`,
`surrealdb-token`) once it reaches parity; until then `syn` is the
differential oracle. Milestones M1 (full grammar) and M3 (side by side behind
a switch) are met (§8). M2's error-message differential is at parity on the
corpus (§7.8); its fuzz target and recovery remain.

## 1. Summary

The SurrealQL front end costs about 1.1 MB of the ~13 MB `-Oz` wasm engine
before planning starts: the `surrealdb-syn` parser (584K), `reblessive`, its
heap stack machine (224K), and the `surrealdb-sql` statement AST (303K, most of
it derived `Clone`, `PartialEq`, `Drop` and `ToSql` code). The edge target is a
whole engine under 5 MB with the front end under 1.5 MB including planning
(`doc/executor/EDGE_PLAN.md`, W9).

This RFC proposes a new front end built on four decisions:

1. **A hand-written byte lexer with no token buffer.** The lexer is a pure
   function of `(source, offset)`. Context-sensitive tokens (numbers,
   durations, record-id keys, regexes) are re-lexed on demand from a token's
   start. Keywords are identifiers tagged through a `const`-built hash table.
2. **Recursive descent for statements, and a Pratt parser on an explicit heap
   stack for expressions.** Every production an input can nest cheaply
   (operators, parentheses, arrays, objects, sets, blocks, closures, arguments,
   idiom parts, record-id keys, kinds) uses zero native stack per level.
   Native recursion happens only for a statement inside an expression and for
   the few statement-like idiom parts (lookups with clauses, destructuring),
   and every level of it is charged against `syn`'s depth budgets. No
   `reblessive`, no `async`.
3. **A flat syntax tree.** One `Vec` of 20-byte nodes plus one `Vec<u32>` of
   child lists, Zig-style. Leaves hold source spans, not copies. A reused tree
   parses with zero heap allocations. Dropping a tree never recurses.
4. **Lowering, not a second AST.** The flat tree is lowered straight into the
   logical plan and `expr` types. Value decoding, builtin-name resolution and
   semantic checks move from the parser into lowering. A canonical printer over
   the flat tree produces stored catalog text byte-for-byte as `ToSql` does
   today.

The parser covers the whole grammar: every statement kind, every expression
form and every literal, with no fallback to `syn`. On the 20,006 statements of
the language-test corpus it agrees with `syn` on every one: 19,910 produce an
equal `surrealdb_sql` tree (`==`, after lowering), and the other 96 are
rejected by both. It also agrees on v3's snapshot inputs, on mutations of
corpus statements, on the nesting depth at which 48 kinds of deep input stop
being accepted, and through 17 of `syn`'s other entry points and settings on
a quarter of a million inputs and twice as many mutants (§7.2). Behind a
switch it runs the engine's whole front end, and the language and upgrade
suites pass with every parse checked against `syn` (§7.7).

On the 20,981 corpus statements all three parsers accept, it measures:

- **Speed:** parses 5.0× faster than `syn` (2.0× when also lowering into the
  old AST).
- **Allocations:** zero heap allocations per statement with a reused arena,
  against 15.3 for `syn`.
- **Size:** builds to a 134 KB standalone `-Oz` wasm module (66 KB gzipped), streaming
  and `syn`'s error wording included, against 1.9 MB for `syn` built the same way.
- **Streaming:** parses a 1 GiB import from 64 KiB chunks at 213 MB/s in
  10.5 MB of resident memory (§6.9).

That makes the front-end saving about 0.85 MB, before deleting the
`sql`↔`expr` conversion layer.

## 2. Goals and constraints

| Goal | Measure |
|---|---|
| Small wasm code | Standalone `-Oz` + `wasm-opt -Oz` module size, and reachability attribution in the engine build |
| Few allocations | Heap allocations per statement, counted by a counting global allocator |
| Bounded stack, no `reblessive` | Native stack needed as a function of nesting depth |
| Speed | MB/s and statements/s on the corpus |
| Good error recovery | Errors reported per parse; valid statements after an error still parse |
| Streaming | Statements yielded as their `;` arrives; memory bounded by the largest statement, not the input (§6.9) |

Constraints from the repository:

- **No new third-party dependencies.** `logos`, `phf`, `bumpalo` and `rowan`
  are excluded unless agreed. The parser has no dependencies at all.
- **Security invariants (`SECURITY_GUIDE.md` §15).** These cover four things:
  - Every recursive production has a depth limit with a bounded-cost error.
  - Parser memory is bounded relative to input size.
  - No evaluation happens during parsing.
  - No panics occur on untrusted input.
- **Stored text is the only stable format.** Catalog definitions are SurrealQL
  text (`SurqlText<T>` in `surrealdb/catalog/src/text.rs`), produced by `ToSql`
  and re-parsed by the `*_for_definition` entry points. The upgrade tests
  depend on this text round-tripping through every release.
- **Language tests pin parse errors.** `parsing-error = """…"""` blocks fix the
  rendered message, line and column.

## 3. The current front end

### 3.1 Crates and data flow

| Crate | Role | LOC |
|---|---|---|
| `surrealdb/syn` (`surrealdb-syn`) | Lexer and parser in production | 23.4k (16k grammar) |
| `surrealdb/sql` (`surrealdb-sql`) | Statement AST, `ToSql` | 12.6k |
| `surrealdb/expr` | `expr` types, `sql`↔`expr` conversion (47 files, 5.8k LOC in `convert/`), logical lowering | — |
| `surrealdb/parser`, `ast`, `token`, `parse-common` (`surrealdb-parser`, "v3") | Arena-based successor, not wired in (§4) | 21.3k |
| `surrealdb/gql`, `pgsql`, `mysql` | Other front ends | — |

The flow from text to plan:

1. `ds.rs` calls `syn::parse_with_capabilities`.
2. The parser produces `sql::Ast { expressions: Vec<TopLevelExpr> }`.
3. `expr::logical::lower(sql::Expr)` (`expr/src/expr/logical/lower.rs`)
   produces the logical plan.
4. Permissions, grants and some catalog paths still go through
   `From<sql::Expr> for expr::Expr`.

Catalog definitions are stored as `ToSql` text and decoded through
`expr_for_definition`, `kind_for_definition` and related entry points under
`ParserSettings::STORED_TEXT`.

GQL reuses `syn`'s `SyntaxError`, `syntax_error!` and `token::Span`. The pgsql
and mysql front ends have their own parsers.

### 3.2 Lexer

**Tokens and keywords.**
- `Token { kind: TokenKind, span }` is 12 bytes and carries no payload.
- `TokenKind` is 4 bytes and includes `Keyword(Keyword)` with 270 keywords.
- Keywords are looked up through a 363-entry `phf::Map<UniCase<&str>, _>`.
- Almost every keyword is also a valid identifier: `kind_is_identifier` accepts
  all `Keyword(_)` tokens.

**Context sensitivity.** SurrealQL cannot be tokenised ahead of parsing, so
`syn` lexes a small base token and re-lexes "compound" tokens on request
(`Lexer::lex_compound`):
- numbers: int, float, `f` and `dec` suffixes, and durations such as `1h30m`
  or `5µs`;
- regexes, where `/` is ambiguous with division;
- JavaScript function bodies;
- prefixed strings (`r"…"`, `d'…'`, `u'…'`, `b'…'`, `f'…'`, `s'…'`), whose
  contents are re-lexed as a record id, datetime, uuid, bytes or file.

Record ids, ranges and graph arrows are whitespace sensitive. `syn` emits
whitespace tokens and peeks with `peek_whitespace*`.

**Lookahead.** A `TokenBuffer<4>` gives three tokens of lookahead. `speculate`
backtracks at four sites: geometry points, object-vs-block, and
`INSERT (…) VALUES`.

### 3.3 Parser

**Structure.**
- Hand-written recursive descent with a Pratt loop for operators.
- Every production is an `async fn` stepped on a `reblessive` stack, with 236
  `stk.run` sites.

**Binding powers, lowest first:**
1. Nullish (`??`, `?:`)
2. Or
3. And
4. Equality (`=`, `IS`, `==`, `!=`, `*=`, `?=`, `@@`)
5. Relation (`<`, `>`, `<=`, `>=`, `CONTAINS*`, `INSIDE*`, `OUTSIDE`,
   `INTERSECTS`, `IN`, `NOT IN`, KNN)
6. AddSub
7. MulDiv
8. Power
9. Prefix
10. Range
11. Call

Equality, relation and range operators are non-associative. A chain of them
without parentheses is an error.

**Table vs field.** A `table_as_field` flag decides whether a bare identifier
is a field of the current document or a table.

**Depth budgets.** Three `ParserSettings` budgets apply:

| Budget | Default | Under `STORED_TEXT` |
|---|---|---|
| `object_recursion_limit` | 100 | 400 |
| `query_recursion_limit` | 20 | 400 |
| `expr_recursion_limit` | 128 | 512 |

The expression budget bounds tree depth, not stack depth. The sql AST is later
walked recursively by `Drop`, `ToSql` and lowering, so a flat chain such as
`1 + 1 + …` must be bounded even though the parser itself could handle it.

**Errors.** `SyntaxError` is a boxed list of diagnostics. It is rendered
rustc-style (`--> [line:col]`, snippet, carets). There is no recovery: the
first error aborts the parse.

**Semantic work inside the parser.**
- Builtin names are validated against a `phf` table of paths.
- `GROUP BY` and `SPLIT` idioms are checked against the projection.
- GeoJSON-shaped objects are turned into geometry literals.
- Datetimes, uuids, decimals and regexes are decoded or compiled, which pulls
  the `regex` crate into the parser.

### 3.4 Where the bytes go

Reachability attribution of the `-Oz` engine build
(`coremin_v0.csv`), by owning crate:

| Item | Bytes |
|---|---|
| `surrealdb_syn` | 583,615 |
| `reblessive` | 224,122 |
| `surrealdb_sql` | 303,261 |
| …of which `Clone` impls | 186,611 |
| …of which `syn`→`sql` construction | 146,955 |
| …of which `From`/`Into` | 91,560 |
| …of which `ToSql`/`fmt` | 48,642 |
| …of which `PartialEq` and `Drop` | 94,631 |

Inside `syn`, `Parser::parse_prime_expr` (115K), `parse_define_stmt` (102K),
`parse_alter_stmt` (97K) and `parse_remove_stmt` (40K) dominate. Several
of these appear twice in the attribution, as two instantiations of the same
`async fn`. Each one
compiles to a state machine that holds every live local of every arm, and
inlines the construction and drop glue of the boxed AST it builds.

`syn` built alone into a wasm module (§7.4) is 1.9 MB:
- 426K of parser code;
- 476K of data segments;
- 325K of `regex`, `regex-automata`, `regex-syntax` and `aho-corasick`, because
  regex literals are compiled at parse time;
- 95K of sql AST code.

`reblessive` is also used by the legacy executor and `idx` (`TreeStack`).
Removing it from the parser is necessary but not sufficient to drop the crate;
EDGE_PLAN's "no `reblessive`" covers both.

## 4. The in-tree successor: `surrealdb-parser` ("v3")

`syn`'s crate docs say it "is scheduled for deletion once the greenfield parser
(`surrealdb-ast` + `surrealdb-parser`) reaches parity". This RFC has to say
how it relates to that effort.

**What v3 is.**
- Mees Delzenne's arena-based parser, kept in step with the grammar by several
  engineers since.
- It covers nearly the full statement surface; LIVE SELECT is missing.
- It has 161 snapshot tests, a streaming-consistency test, a criterion bench
  and a fuzz target.
- Nothing lowers its AST into `sql`, `expr` or the planner, and it has no
  printer.

**Design and measured cost** (§7, same corpus and build):

| | v3 | Consequence for the edge goals |
|---|---|---|
| Lexer | `logos`, 386 token kinds, every keyword its own case-insensitive regex, five morphing side lexers | `surrealdb_token` is **1.36 MB** of a 2.26 MB standalone module. The generated DFA is the largest single item of any parser measured |
| Recursion | Every production is an `async fn` on `reblessive` (146 enter sites) | Same structural cost as `syn` |
| AST | `library!` generates ~190 typed `Vec` arenas, `TypeId`-dispatched, with linked lists for sequences | Code per node type; O(n) list length and span |
| Allocations | Interned strings, reused arenas | 3.2 per statement reused, 21.3 fresh |
| Throughput | | 1.75× `syn` |
| Nested input | Heap stack | 33 MB of heap at 10,000-deep parentheses with limits lifted |
| Recovery | None | First error only |

v3's grammar coverage, snapshot files and streaming test are worth keeping.
Its lexer and recursion strategy are what this RFC replaces. §8 proposes
porting v3's grammar knowledge and test files onto the engine below, rather
than finishing two parsers. That is a decision for v3's owners (§10).

## 5. Survey of approaches

### 5.1 Parsing strategy

**Hand-written recursive descent with Pratt / precedence climbing**
(Pratt, "Top Down Operator Precedence", POPL 1973; Kladov, "Simple but
Powerful Pratt Parsing", 2020,
<https://matklad.github.io/2020/04/13/simple-but-powerful-pratt-parsing.html>).

Hand-written recursive descent is what `syn`, v3, rustc, rust-analyzer,
Roslyn, Go and Zig all use.
- **Size:** code grows with the grammar, but each production is a plain
  function with no state machine.
- **Speed:** the fastest option in practice.
- **Recovery:** possible anywhere, by hand.

The only problem is the native stack under deep nesting. Kladov, "From Pratt to
Dijkstra" (2020), shows that the Pratt loop converts mechanically to
Dijkstra's shunting-yard algorithm (1961) with an explicit operator stack.
That conversion is what removes native recursion for expressions here.

**Resilient LL parsing** (Kladov, "Resilient LL Parsing Tutorial", 2023,
<https://matklad.github.io/2023/05/21/resilient-ll-parsing-tutorial.html>).

A recursive-descent parser that never aborts:
- every loop has a *recovery set* of tokens that end it;
- an unexpected token is wrapped in an error node and skipped;
- each production either advances or returns, which guarantees progress.

It produces a tree for any input with several diagnostics, at the cost of a
few branches per loop. It suits a hand-written parser and is what the
recovery design in §6.5 follows.

**Table-driven LR / LALR with automatic repair** (Diekmann & Tratt, "Don't
Panic! Better, Fewer, Syntax Errors for LR Parsers", ECOOP 2020; the CPCT+
algorithm in `grmtools`/`lrpar`).
- **Recovery:** the best automatic recovery published. CPCT+ searches for
  minimal repair sequences and reports all minimal-cost repairs, with no
  per-grammar effort.
- **Size:** tables are data, which is good, but SurrealQL needs:
  - context-sensitive lexing (record ids, durations, regexes, prefixed
    strings) that LR lexer interfaces do not model;
  - keyword-as-identifier ambiguity everywhere, which inflates the state
    count;
  - whitespace-sensitive productions.

  LALR tables for a grammar of this size, with hundreds of states and around
  300 terminals, compress to an estimated tens to a few hundred KB; this was
  not measured.
- **Dependencies:** a generator would be a new dependency.
- **Migration:** the risk is too high. The grammar is defined by `syn`'s
  behaviour, not by a grammar file, and an LR port would have to rediscover
  every quirk the differential test pins.

Rejected as the primary strategy. CPCT+'s idea of ranking repairs by cost
informs the "expected …" suggestions.

**Parser combinators / PEG.** Rejected:
- generic combinators monomorphise into large code;
- packrat memoisation allocates;
- error recovery is weak.

### 5.2 Tree representation

**Lossless green/red trees** (Roslyn; Lippert, "Persistence, Facades and
Roslyn's Red-Green Trees", 2012; `rowan` in rust-analyzer).

They keep trivia and support incremental reparse and IDE refactoring. They pay
for it with an interned green node per token and tree, a typed red facade, and
an extra layer before any semantic tree. The engine never edits text in place,
and the IDE tooling is out of process, so a lossless tree buys nothing here
and costs size and allocations.

**Flat, data-oriented AST in arrays with `u32` indices.**
- Zig's `std.zig.Ast`: a `MultiArrayList` of `{tag, main_token, lhs, rhs}`
  plus `extra_data` (Kelley, "Practical Data-Oriented Design", Handmade
  Seattle 2021).
- Carbon's toolchain: a token buffer and a post-order parse tree in arrays
  (Carruth, "Modernizing Compiler Design for Carbon Toolchain", CppNow 2023).

Benefits:
- Nodes are fixed size and allocated in bulk.
- Children are indices.
- There is no per-node `Box`, `Clone`, `Drop` or `PartialEq` code.
- Dropping a tree is two frees.
- Memory is roughly 20 bytes per node.

The cost is that consumers read through accessors rather than matching
`enum`s. This is the representation recommended here.

**Typed per-node-type arenas** (v3). These sit between the two. They are typed,
but code is generated per node type, and the `TypeId` dispatch and linked
lists cost code and time (§4).

### 5.3 Lexing

**Generated DFA lexers** (`logos`). Fast, but on this grammar the generated
code is 1.36 MB of wasm (§4), and adding one would be a dependency proposal.
Rejected for size.

**SIMD / branch-light lexing** (Langdale & Lemire, "Parsing Gigabytes of JSON
per Second", VLDB Journal 2019; Mytkowicz, Musuvathi & Schulte, "Data-Parallel
Finite-State Machines", ASPLOS 2014).

The two key ideas are classifying bytes in bulk (strings, structural
characters) with vector instructions, and running a DFA speculatively over
chunks. They pay off on megabyte inputs with few token kinds. Queries average
42 bytes, the lexer is not the bottleneck (parse alone runs 2.4× faster than parse plus lowering, §7.3), and wasm SIMD is not
available on every host. Adopted in spirit only: a 256-entry byte dispatch and
straight-line scanning loops. Revisit for the import path, which lexes
megabytes.

**Hand-written byte lexer with on-demand compound re-lexing.** What `syn`
already does, minus the token buffer and whitespace tokens. Adopted.

### 5.4 Incremental parsing

tree-sitter (Brunsfeld) gives incremental GLR parsing with error recovery, for
editors. The engine parses each query once, and the language server can use
tree-sitter's own grammar. Not relevant to the engine's parser. The flat tree
does not preclude statement-level reuse for the import stream (§6.5).

### 5.5 Allocation and interning

**Arena reuse.** A tree and scratch stacks kept per connection or executor stop
allocating once warm. Adopted.

**String interning** (as v3 does). It allocates one `String` per unique string
and needs a hash table, about 180K of `hashbrown` and `ahash` in the engine
already (W9). Spans into the source avoid both, provided the source outlives
the tree, which holds for a parse followed immediately by lowering. Escaped
strings are decoded once, at lowering. Interning is not adopted.

### 5.6 Related work and what we adopt

This section sets the design against published work from 2019 to 2026. Each
item is judged on six goals: wasm code size, speed, zero allocation per
statement, streaming, bounded native stack, and printing that matches `ToSql`
byte for byte. Citations marked *(unverified)* were taken from memory or a
secondary source and not checked against a primary one.

**Zig's AST and ZIR.** `std.zig.Ast` stores nodes as a struct of arrays
`{tag, main_token, lhs, rhs}` with an `extra_data` side array. AstGen lowers
that tree straight into ZIR, an untyped IR with interned strings, with no
second AST in between (Kelley, "Practical Data-Oriented Design", Handmade
Seattle 2021 *(unverified)*; Hashimoto, "Zig AstGen: AST => ZIR",
<https://mitchellh.com/zig/astgen>). Our tree follows the same pattern, with
two differences. There is no token buffer, so spans live in the node, which is
why a node is 20 bytes. There is no interning, because lowering runs while the
source is still held. **Adopt** (the basis of §6.3): bulk allocation, no
per-node `Drop` or `Clone` code, and teardown in two frees.

**Carbon's postorder parse tree.** Carbon's parse tree is a postorder array of
8-byte nodes `{kind, has_error, token_index}` with no child pointers. Each kind
has a fixed child count or opens with a bracketing node, and the parser runs
from an explicit state stack (Carruth, "Modernizing Compiler Design for
Carbon Toolchain", C++Now 2023; `toolchain/parse/tree.h` and `parse.md` in
<https://github.com/carbon-language/carbon-lang>).

Our nodes are already appended in postorder, so adopting the layout would mean
dropping `a`, `b`, `end` and `depth`. Without a token buffer the node would be
`{tag, flags, start}`; `end` would be re-lexed from `start` and depth enforced
on the frame stack.
- **Gains:** 2–2.5× less tree memory; lowering in one pass with a value
  stack; and a parser driven only by a state stack, which could suspend at a
  chunk boundary instead of re-parsing.
- **Costs:** the printer and clause access need random access to children,
  either through a subtree-size pass or slow scans. The `ToSql`-exact printer
  (operator parenthesisation, clause order) is the consumer that most needs
  it.
- **At our sizes:** statements average 42 bytes, so the tree sits in L1
  whatever its layout. Re-parsing a statement costs microseconds (§7.5), so
  suspension buys nothing measurable either.

**Adapt only after Experiment B (below).** We already follow Carbon's
error-bit contract: structural invariants hold except under an error node.

**Rust arena ASTs (oxc, swc, rowan/Biome).** oxc keeps typed nodes in one bump
arena with strings as views into the source, and reports about 3× swc's parse
speed (<https://oxc.rs/docs/guide/benchmarks>). swc has an open arena-AST
effort *(unverified)*. rust-analyzer's `rowan`, which Biome forks, is a
lossless green/red tree. oxc confirms that the speed comes from arenas plus
source spans. It still pays for per-type node code and depends on `bumpalo`,
and a `Vec<Node>` gets the same effect without a dependency. **Adopt the
principle** (in place); **reject `rowan`** for size and allocations (§5.2).

**flap: fused lexing.** Yallop, Xie and Krishnaswami, "flap: A Deterministic
Parser with Fused Lexing", PACMPL 7 (PLDI 2023), Art. 155,
<https://doi.org/10.1145/3591269>. flap normalises typed grammar expressions
into a deterministic Greibach-like form and uses MetaOCaml staging to fuse a
regex-derivative lexer into each nonterminal, so no token stream exists. It
reports code 2–6× faster than ocamlyacc with ocamllex. Its limits, from the
paper:
- LL(1) grammars only, with no left recursion;
- it needs a staging code generator;
- parser state, several entry points and richer lexer actions are listed as
  future work.

Its baseline lexers allocate and buffer tokens; ours does neither, and returns
a 12-byte token in registers. Specialising the lexer per parser state, with
about 300 keywords that are also identifiers, pushes wasm size the wrong way.
**Reject the mechanism; adapt the idea:** a few hand-written scanners
specialised to the hottest parser states. The compound-token entry points
already work this way. Experiment A tests one more.

**Typed algebraic and staged selective combinators.** Krishnaswami and Yallop,
"A Typed, Algebraic Approach to Parsing", PLDI 2019,
<https://doi.org/10.1145/3314221.3314625>; Willis, Wu and Pickering, "Staged
Selective Parser Combinators", ICFP 2020, <https://doi.org/10.1145/3409002>.
Both use types and staging to make combinators linear-time and as fast as
hand-written code. Rust can stage only through macros or a build step, and
neither paper addresses recovery, streaming or context-sensitive lexing.
**Reject:** they approximate the hand-written parser we already have.

**DuckDB's runtime-extensible PEG parser.** Mühleisen and Raasveldt,
"Runtime-Extensible Parsers", CIDR 2025,
<https://vldb.org/cidrdb/papers/2025/p18-muhleisen.pdf>. They replace YACC
with a PEG grammar loaded at runtime, for extensibility and better messages.
They measure about 10× slower parsing (0.03 ms against 0.3 ms on TPC-H Q1).
SurrealQL keeps growing syntax, but it grows at compile time, and the speed
and dependency costs contradict two of our goals. **Reject.** Their clause-order
hints ("GROUP BY must precede ORDER BY") are worth copying into clause-level
recovery (§6.5) as static messages.

**simdjson On-Demand.** Keiser and Lemire, "On-Demand JSON: A Better Way to
Parse Documents?", Software: Practice and Experience 54(6), 2024,
<https://doi.org/10.1002/spe.3313>. A SIMD structural index feeds a forward
iterator that decodes a value only when it is read. Leaves that stay spans
until lowering (§6.3) are the same idea: **adopt** (in place). The
document-stream mode, which finds document boundaries before parsing each
document, is the closest prior art for §6.9. It also names our streaming risk:
a large statement full of `;` inside blocks or strings would be re-parsed each
time a `;` arrives. §6.9 bounds that by re-parsing large statements
geometrically. A resumable boundary pre-scan, tracking strings, comments and
brackets over new bytes only, is the **adapt** option if imports show the
need.

**SIMD UTF-8 validation and scanning.** Keiser and Lemire, "Validating UTF-8
in less than one instruction per byte", Software: Practice and Experience
51(5), 2021, <https://doi.org/10.1002/spe.2920>. On wasm32, simd128 is
available in V8 (and so on Cloudflare Workers) and in Wasmtime. Two things
stop us using it:
- wasm has no runtime feature detection, so a simd128 module fails validation
  on a host without it;
- rustc does not enable simd128 by default on `wasm32-unknown-unknown`.

Tokens are short and varied, and the parse costs less than lowering. The
stream validates UTF-8 incrementally with `core::str::from_utf8` and
`valid_up_to`/`error_len`, which handles a character split across chunks with
no dependency. **Reject SIMD in the lexer. Adapt it for imports only**, as a
word-at-a-time scan for `;`, quotes and brackets in a boundary pre-scan, if
measurement asks for one.

**Incremental parsers: tree-sitter and Lezer.** tree-sitter (Brunsfeld,
<https://tree-sitter.github.io>) is incremental GLR, with a lexer chosen by
parse state, external scanners and error nodes. Lezer (Haverbeke,
<https://lezer.codemirror.net/docs/guide/>) is an incremental LR parser that
packs subtrees at 64 bits per node. Both reuse subtrees after an edit, which
pays only when a document changes in place; the engine never edits. **Reject
for the engine.** tree-sitter's lexer chosen by parse state is the same idea
as our compound entry points.

**Error recovery.** Kladov, "Resilient LL Parsing Tutorial", 2023,
<https://matklad.github.io/2023/05/21/resilient-ll-parsing-tutorial.html>,
uses recovery sets, an advance-or-return rule per loop, and a fuel counter
that catches a parser making no progress. Diekmann and Tratt, "Don't Panic!
Better, Fewer, Syntax Errors for LR Parsers", ECOOP 2020,
<https://doi.org/10.4230/LIPIcs.ECOOP.2020.6>, use CPCT+ to search for all
minimum-cost repairs.
- **Resilient LL: adopt** the recovery sets and the progress rule, with fuel
  as a debug assertion.
- **CPCT+: reject the algorithm.** It is specific to LR and searches at parse
  time. Its minimal repairs shape only the "expected …" text.

**Would fused lexing or a postorder tree materially beat the design?**
Probably not on our goals. Parse alone runs at 167 MB/s with no allocations,
and parse plus lowering at 70 MB/s, so lowering sets end-to-end speed until
Phase B. Two cheap experiments on the expression subset would settle it; both
are gated on zero differential mismatches:

- **Experiment A, specialised operator lexing.**
  - *Change:* in the "have an operand" state, replace `next_token` with a
    `lex_infix` that dispatches on the first byte to an operator and probes
    only the operator keywords.
  - *Adopt if:* expression-heavy throughput rises at least 15% for at most
    2 KB of wasm.
- **Experiment B, Carbon-style 8-byte postorder nodes.**
  - *Change:* nodes become `{tag, flags, start}` for expressions, with lowering
    on a value stack and a printer that computes subtree sizes in one backward
    pass.
  - *Adopt if:* parse plus lowering improves at least 10%, or lowering and
    printer code shrinks. Memory savings alone do not count at our statement
    sizes.

Neither is on the M1 critical path. The grammar keeps the clause-record
layout, which a postorder tree would make harder to read.

| Item | Verdict | Reason |
|---|---|---|
| Zig AST / ZIR | Adopt (in place) | Flat fixed-size nodes, `extra`, direct lowering |
| Carbon postorder tree | Adapt after Experiment B | Already postorder; random access for the printer is the cost |
| oxc / swc arenas | Adopt the principle | Arena plus spans; a `Vec` replaces `bumpalo` |
| rowan / Biome | Reject | Lossless trees cost size and allocations |
| flap (PLDI 2023) | Reject mechanism, adapt idea | Generator and LL(1) only; per-state lexers inflate wasm |
| Typed/staged combinators (2019, 2020) | Reject | Recover hand-written speed we already have |
| DuckDB PEG (CIDR 2025) | Reject | About 10× slower and a dependency; borrow clause-order hints |
| simdjson On-Demand (2024) | Adopt lazy leaves; adapt boundary pre-scan | Spans decoded at lowering; pre-scan if re-parse cost shows |
| SIMD UTF-8 and scanning | Reject in lexer; adapt for imports | No runtime feature detection in wasm; `core` validates chunks |
| tree-sitter, Lezer | Reject for the engine | Edit-incremental reuse does not apply |
| Resilient LL (2023) | Adopt | Recovery sets, advance-or-return, fuel as a debug check |
| CPCT+ (ECOOP 2020) | Reject algorithm | LR-specific search at parse time |

## 6. Recommendation

### 6.1 Lexer

**Interface.** `Lexer { src: &[u8], pos: u32 }` is `Copy`. `next_token()`
returns `Tok { kind: u8, ws: bool, kw: Kw, aux: u8, start: u32, end: u32 }`,
12 bytes:
- `ws` records whether trivia preceded the token, replacing whitespace tokens;
- `kw` is the keyword an identifier spells;
- `aux` holds per-kind data, such as a string's prefix.

**Lookahead.** The parser holds one current token. `peek1` and `peek2` lex from
a copy of the lexer, so there is no ring buffer, no `backup_after`, and no
token-buffer state to keep consistent. Re-lexing is cheap: two to three tokens
are re-lexed only at the few ambiguous sites.

**Keywords.** Keywords are identifiers tagged by a case-insensitive FNV probe
into a 512-entry `u8` table built by a `const fn`. That is about 0.5 KB of
data and one loop, against `phf`'s map plus `unicase`.

**Compound tokens.** These are explicit lexer entry points the parser calls
from a token's start:
- `numeric(start)`: int, float, decimal or duration, with validation;
- `regex(start)`;
- `record_key_ident(start)`: flexible keys such as `person:1abc`;
- later, `js_body(start)` and the prefixed-string sub-grammars.

Each validates syntax and returns a span. Values are decoded at lowering.

**Unicode.** Multi-byte operators (`×`, `÷`, `∋`, `⊇`, `…`, `⟨…⟩`, `µs`) are
matched on the lead byte.

### 6.2 Parser

**Statements** use plain recursive descent: one function per statement and
clause, with the current parser's clause order. As in `syn`, every statement
other than the top-level-only ones (`BEGIN`, `COMMIT`, `CANCEL`, `USE`,
`OPTION`, `KILL`, `LIVE`, `SHOW`, `ACCESS`) is a prime expression, so an
operator or idiom part may follow one.

**Definitions** (`DEFINE`, `ALTER`) are a fixed record (name, `ON` target,
kind-specific slots) plus a list of clause nodes in source order. The parser
decides which clauses a definition accepts; lowering replays the list, so a
later clause overrides an earlier one, and `syn`'s checks on combinations of
clauses (`PASSWORD` with `PASSHASH`, `RETRY` before `ASYNC`, index column
counts, DiskANN distance and type) run there.

**Expressions** use a Pratt engine that keeps its pending work on
`Vec<Frame>`.

Frame kinds:
- `Infix` (with an optional payload record for `@n,OP@` and `<|k,…|>`),
  `Prefix`, `Cast`, `Paren`;
- `Array`, `Object`, `Set`, `Block`, `BlockOrSet` and `Args`, which collect
  elements on a value stack from a mark;
- `Idiom` and `Bracket`, for idiom parts;
- `Closure`, for a closure body;
- record-id key wrappers, for array, object and range keys;
- `KindAlts`, `KindSized`, `KindObject` and `KindTuple`, for kinds;
- three bottom frames: `Base` (one expression), `Atom` (one primary and
  nothing after it: `RELATE` ends, record ids, required blocks) and `Path` (a
  plain idiom: parts, no operators).

The engine alternates between two states:
- *want an operand*: prefix operators push frames, and a primary pushes a
  value;
- *have an operand*: postfix, idiom and infix operators either push a frame or
  reduce the top frame.

Binding powers live in a `const` table indexed by operator. Associativity
checks read a per-node "covered" bit, set when the node was written in
parentheses, so they reproduce `syn`'s "chained relational operators" rules
exactly, including `syn`'s checking of `@@` at relation power though it
parses at equality power. Idiom parts continue only a prime: a postfix call or
range ends the idiom, as in `syn`.

**Native recursion** happens only for a statement inside an expression
(`(SELECT …)`, `RETURN (UPDATE …)`), a lookup with clauses
(`->(SELECT … WHERE …)`) and destructuring (`.{ a: …, b.{ … } }`). Each level
charges at least one depth budget (§6.7), so the budgets bound the recursion.
The parser needs about 0.7 KB of native stack per nested statement (§7.3).
Every other construct uses constant native stack at any depth.

**Lookahead and backtracking.** Lookahead is at most two tokens. Two sites
speculate, as in `syn`: `INSERT (fields) VALUES …` against a parenthesised
expression, and a geometry point `(x, y)` against a parenthesised
expression. The arena only grows, so undoing a speculative parse is a few
truncations (`Parser::checkpoint`/`restore`). Object against block is decided
by lookahead (a key, then `:`), and set against block by the token after the
first element.

### 6.3 Tree

**Layout.**
- `Ast { nodes: Vec<Node>, extra: Vec<u32>, stmts, spans }`.
- `Node { tag: u8, op: u8, depth: u16, a: u32, b: u32, start: u32, end: u32 }`
  is 20 bytes.
- `a` and `b` are child ids, inline payload (an `i64` or `f64` split across
  both), or an index into `extra` where lists are stored as `[len, item…]` and
  statement clause records as fixed slots.

**Depth tracking.** Every node records its subtree depth, so a recursive
consumer (lowering, printing) knows its recursion bound. The depth budgets
(§6.7) keep it bounded; a backstop rejects a tree deeper than the three
budgets combined.

**Leaves** are spans:
- identifiers, strings and parameters are unescaped at lowering;
- integers and floats are decoded inline;
- decimals, durations, datetimes, uuids, bytes, files, regexes, `r"…"` record
  ids and builtin paths are decoded or resolved at lowering, where
  `rust_decimal`, `chrono`, `uuid`, `regex` and the function registry already
  live in the engine. The harness's lowering decodes each with `syn`'s rules
  (`examples/support/lower_lit.rs`); nothing is handed to `syn`.
- kinds are parsed into nodes, not left as spans.

### 6.4 Lowering instead of a second AST

The flat tree reaches the plan in two phases.

**Phase A (migration).** An adapter lowers the flat tree into today's
`surrealdb_sql` types. The differential harness *is* this adapter
(`examples/support/lower*.rs`). Cutting over behind it
changes nothing downstream.

**Phase B (target).**
- `expr::logical::lower` reads the flat tree directly.
- The paths that use `From<sql::Expr> for expr::Expr` (permissions, grants,
  catalog compile) lower from the flat tree.
- `INFO`'s use of the sql AST is replaced by the printer (§6.6).

`surrealdb-sql`, its 186K of `Clone` code and the sql↔expr conversion layer
are then deleted.

**Semantic checks** the current parser makes move to lowering:
- builtin-name validation, including deprecation and typo hints;
- the `GROUP BY` and `SPLIT` projection checks;
- GeoJSON recognition;
- letless-`LET` deprecation stays in the parser, because it is purely
  syntactic.

The differential harness already runs the moved checks in its lowering and
matches `syn`'s accept/reject decisions on the whole corpus.

### 6.5 Errors and recovery

**Diagnostics** are `{start, end, msg: &'static str}`. Recording
one formats nothing, so the error path costs no `fmt` machinery in the parser.
A renderer at the API boundary produces `syn`'s current format (`--> [l:c]`,
snippet, carets, labels).

The "expected …" text comes from the token kind and a static expectation
string. Where `syn` interpolates values (`did you maybe mean string::slice`),
lowering produces the message, because it holds the registry.

**Recovery** works at three levels, all following Kladov's recovery-set
scheme:

1. **Statement** (implemented). The first error in a statement makes it
   an `Error` node. Parsing resumes at the next `;` outside brackets, matched
   by kind, so an unbalanced `[` does not swallow the script.
2. **List** (planned). In arrays, objects, arguments and projection lists, an
   error skips to the next `,` or the closing delimiter at the same depth,
   leaving an `Error` element.
3. **Clause** (planned). In statements, an error skips to the next clause
   keyword in the statement's clause order (`WHERE`, `GROUP`, `ORDER`, `LIMIT`,
   …).

Query semantics do not change: a query with any diagnostic still fails as a
whole. Recovery changes how many errors are reported in one round trip, and
lets the import stream and the language server keep going.

### 6.6 Stored text and `ToSql`

Catalog text is produced by `ToSql` on the parsed definition, not copied from
the source. Changing a single byte of it changes what released binaries read
back. The new front end therefore ships a **canonical printer over the flat
tree** that reproduces `ToSql` output byte-for-byte for every construct that
can be persisted.

That guarantee is enforced three ways:
1. **A differential printer test.** For every corpus statement and every fuzz
   input `x` that `syn` accepts, `print(parse_next(x))` must equal
   `syn::parse(x).to_sql()`. The adapter gives this today by
   construction, by printing the lowered `sql` tree.
2. **A round-trip test.** `parse(print(t)) == t` holds on the flat tree, which
   is the property `fuzz_format` checks today.
3. **The upgrade suite** (`language-tests/tests/upgrade/`). It runs unchanged
   under the new parser, both as the reader of old text and as the writer of
   new text.

Until Phase B, stored text keeps coming from `sql::ToSql` via the adapter. The
printer replaces it only once test 1 passes on the whole corpus.

### 6.7 Depth limits and the stack bound

The parser enforces `syn`'s three recursion budgets, charged where `syn`
charges them, so that the same deep inputs are rejected:

| Budget | Default (`STORED_TEXT`) | One level per |
|---|---|---|
| `expr_depth` | 128 (512) | expression entered, operator appended to an expression's spine, postfix operator, idiom part, destructuring level |
| `query_depth` | 20 (400) | nested expression context `syn` enters through `parse_expr_field/table/inherit`: statement clause, parenthesis, argument list, index part, block, closure body |
| `object_depth` | 100 (400) | array, object or set literal, and nested kind |

Each frame records the budgets in use when it was pushed and restores them
when popped, which reproduces `syn`'s "charge on entry, restore on exit"
without native recursion. A probe over 32 nesting templates (parentheses,
arrays, sets, blocks, calls, operator chains, idiom parts, subqueries,
closures, kinds, destructuring, …) finds the same deepest accepted input for
`syn` and the new parser in every case (§7.2).

**Worst-case native stack.** Every native recursion level charges a budget,
so the deepest recursion is bounded by the budgets; a separate counter caps
it at their sum as a backstop, and the tree's depth at twice that. Measured
at the deepest input the budgets accept (`bench stack`, §7.3), the worst case
is 51 KiB at the default budgets (127 nested lookup aliases) and 307 KiB under
`STORED_TEXT` (400 nested definitions with blocks). Worst-case heap is linear
in input size: at most one node per token, plus frames bounded by the
budgets.

### 6.8 Size and allocation budget

**Code.** The complete parser builds to a 134 KB standalone module after
`wasm-opt -Oz` (66 KB gzipped), inside the 120–200 KB estimated from the
prototype. That includes the lexer, the keyword table (269 keywords and
aliases), the expression engine, every statement, kinds, `core`'s float
parser, the streaming layer's entry point and the diagnostics in `syn`'s
words, but not the printer (M4). The diagnostics cost 10.7 KB: `syn`'s message
templates and a port of its compound-literal errors, which stay in the parser
because streaming needs to know where an error ends. Filling a template
against the source is the renderer's job, in the lowering crate.

**Budget against today's cost** (engine attribution):

| | Today | Proposed |
|---|---|---|
| Parser | 584K (`syn`) | 120K measured standalone |
| Stack machine | 224K (`reblessive`, parser share) | 0 |
| Statement AST | 303K (`sql`) | ≈ 20K (flat tree accessors) + ≈ 40K printer |
| Lowering into plan | `expr::logical::lower` from `sql` | Same, reading the flat tree |
| **Total** | **≈ 1.11 MB** | **≈ 0.15–0.26 MB** |

Deleting `sql`↔`expr` conversion (part of `expr`'s 769K) is additional upside
and is not counted.

**Allocations.** None per statement in steady state. A fresh arena costs about
8 allocations as the vectors grow. Lowering into the target types allocates
what those types need, and Phase B removes the intermediate `sql` tree's
allocations.

### 6.9 Streaming

Imports and the RPC stream feed query text in chunks. The parser yields each
top-level statement as soon as the input confirms where it ends, so parsing,
compilation and execution can be pipelined, and an import of any size parses
in memory bounded by its largest statement.

**Interface** (`src/stream.rs`). `StreamParser::feed(bytes)` appends input,
and `finish()` marks its end. `next_statement()` returns one of:
- `Statement` — the statement's source window, its offset in the whole input,
  its tree and its diagnostics;
- `NeedMore` — the input ends inside a statement;
- `End`;
- `StreamError` — invalid UTF-8, or a statement longer than the configured
  maximum.

A statement borrows the parser until the next call to `next_statement()`, which
releases the statement's bytes and arena. Input ending mid-statement is never
an error before `finish()`.

**One statement, one window, one arena.** Leaves point into the source, so a
statement's source must live as long as its tree. Each statement is parsed by
`parse_statement` into a tree cleared per statement, over a window starting at
the statement's first byte. Released bytes are dropped from the buffer once
they are at least half of it, so each byte is copied at most once. Memory is
therefore at most twice the largest statement plus a chunk, with no
allocation per statement once warm.

**Re-parse, do not resume.** When a statement is incomplete, nothing about it
is kept. When more input arrives, it is parsed again from its first byte. The
lexer's re-lex-from-a-position design makes this trivial, and it means a
streamed statement parses exactly as it does in a whole-input parse: there is
no suspended parser state to get wrong.

**When a statement is final.** A parsed statement is final when the next
token is `;`. No byte after that `;` can change it, because the parser never
looks past a statement's terminator. A statement that failed to parse is
final when two things hold:
- its recovery stopped at a `;`;
- the parser saw no token within four bytes of the end of the window before
  failing. Four bytes is the longest lookahead past a token that any lexer
  entry point takes.

The second condition matters because recovery lexes without the parser's
context: a regex can hold a `;` that recovery sees as a terminator. Chunk
boundaries inside strings, comments, escaped identifiers, regexes,
brackets, blocks and multi-byte characters all reduce to these rules, with no
case-by-case handling.

**Bounding the re-parse work.** A statement can only become final when a `;`
byte arrives. A `;` already buffered was inside a string, comment or bracket
the last time the statement was parsed, and appending bytes cannot move it
out. So a feed without `;` costs no parse, and statements up to 4 KiB are
re-parsed only on a feed that carries a `;`. Longer statements are re-parsed
only once they have grown by half since the last attempt, so the total work on
one statement stays linear in its length. Yielding is delayed by at most that
growth.

**Bounded memory and errors.**
- `max_statement` (default 64 MiB at the API boundary, at most 4 GiB because
  spans are `u32`) turns unbounded buffering into `StatementTooLarge` at the
  statement's offset.
- Invalid UTF-8 is reported at its offset. A character split across chunks
  waits for its remaining bytes.

**Effect on the grammar work.** Every new production keeps two properties
that streaming relies on:
- it never looks past a statement's terminating `;`;
- it never looks more than four bytes past a token's end.

The chunk-boundary tests below enforce both on the whole corpus.

**Testing.**
- `differential --stream` splits every corpus file at every byte (files up to
  2 KiB) or at 2,048 evenly spaced points (larger files), feeds the two halves,
  and requires the streamed statements to equal the whole-input parse
  node for node.
- `differential --stream-fuzz` does the same with random chunk sizes over the
  corpus and 20 three-byte mutations of each file, so error statements and
  recovery are split too.
- Both modes first check that statement-at-a-time parsing over the whole input
  splits and tags statements exactly as `parse_script` does.

**Prior art.** `syn`'s `StatementStream` re-parses the buffered prefix on
every call. v3's partial mode (`surrealdb-parser`, `ParserSettings::PARTIAL`)
turns every end-of-input read into a "missing data" error, and its stream test
re-parses at every character boundary. Both treat any statement that touches
the end of the buffer as incomplete. §6.9 differs in three ways:
- the `;`-arrival rule, which skips parses that cannot succeed;
- geometric re-parse, which bounds the work on a large statement;
- recovery-aware finality, which lets a failed statement be yielded without
  waiting for the end of input.

## 7. Implementation and measurements

### 7.1 What exists

`surrealdb/parser-next` (crate `surrealdb-parser-next`) has no dependencies.
`surrealdb/parser-next/lower` (crate `surrealdb-parser-next-lower`) is the
migration adapter of §6.4: it lowers the flat tree into `surrealdb_sql` and
parses `syn`'s literal-value grammars, depending only on workspace crates.
`surrealdb-syn` routes its entry points through both behind a switch (§7.7).

| File | Contents |
|---|---|
| `src/kw.rs` | Keyword table: `syn`'s 269 keywords and their aliases |
| `src/lex.rs` | Lexer, with compound entry points for numbers, regexes, record-id keys and JavaScript bodies |
| `src/ast.rs` | Flat tree: tags, clause codes, statement records |
| `src/parse/mod.rs` | Parser state, depth budgets, statement dispatch, recovery, `parse_statement` |
| `src/parse/expr.rs` | Expression engine |
| `src/parse/idiom.rs`, `record.rs`, `kind.rs`, `lit.rs` | Idiom parts and lookups, record ids, kinds, single-kind literals |
| `src/parse/stmt/*.rs` | Statements by family: `select`, `write` (CREATE, UPDATE, UPSERT, DELETE, RELATE, INSERT, RETURN, LET), `define`, `alter`, `remove`, `misc` |
| `src/stream.rs` | Streaming (§6.9) |
| `src/decode.rs` | Escape decoding |

The examples:
- `examples/differential.rs` is the differential harness (§7.2).
- `examples/bench.rs` reports throughput, allocations, the stack probe and the
  streamed import.
- `examples/entries.rs` is the differential harness for `syn`'s other entry
  points and settings (§7.2).
- `lower/src/{lib,define,lit}.rs` lower the tree into `surrealdb_sql`,
  including literal decoding, the semantic checks moved out of the parser and
  the parser settings (`legacy_strands`, the files and Surrealism gates);
  `lower/src/entry.rs` mirrors `syn`'s entry points; `lower/src/value.rs`
  parses `syn`'s value, JSON and record-id grammars into public values.

`surrealdb/parser-next/size-probe` builds `next`, `syn` or `v3` alone as a wasm
module.

**Modelled:** the whole grammar `syn::parse` accepts with every optional
grammar enabled: all statements, all expression forms (closures, mocks,
sets, blocks, KNN and match operators, lookups with clauses and ranges,
recursion and destructuring parts, JavaScript functions, `mod::`, `silo::`
and `ml::` calls, geometry points) and all literals.

**Entry points:** `parse_entry` parses an expression in field context, a
block, a plain idiom, a kind or a field list alone, with or without
requiring the whole input (`syn`'s fresh and `*_for_definition` forms);
`parse_statement` serves `StatementStream`. Every `syn` entry point the
engine calls to parse a query, a stored definition, an expression or a
client-supplied value has a parser-next counterpart. Still on `syn` alone:
the lexical helpers `duration`, `datetime`, `table` and
`function_with_capabilities`, and error rendering (M2).

**Settings:** `Limits` carries the three budgets and strict record ids
(`flexible_record_id = false`); the lowering applies `legacy_strands` and the
files and Surrealism gates, since parser-next accepts the whole grammar.

### 7.2 Differential coverage

`cargo run --locked --release -p surrealdb-parser-next --example differential`
runs every mode below. Results compare the lowered tree with `syn`'s by `==`
(masking the random keys `syn` mints for access methods).

| Mode | Inputs | Result |
|---|---|---|
| default: every corpus statement | 20,006 statements in 2,687 files | 19,910 equal, 96 rejected by both, 0 mismatches, 0 false accepts or rejects |
| `--v3`: v3's snapshot inputs as whole scripts | 160 files | 118 equal, 42 rejected by both |
| `--fuzz-diff`: one- or two-edit mutants of every corpus statement, 10 per statement | 200,060 scripts | all equal or rejected by both |
| `--depth-probe`: deepest accepted nesting, 48 templates | 48 | all equal to `syn` |
| `--stream`: every split point (2,048 per large file) | 2,818,556 streamed parses | 0 differ from the whole-input parse |
| `--stream-fuzz`: random chunk sizes over mutated files | 451,416 | 0 differ |
| `--fuzz`: truncations and byte mutations, invariants only | 558,000 | no panics, no broken invariants, also with debug assertions and overflow checks |
| `entries`: every corpus statement and every suffix of it starting at a token, through 17 entry points and settings | 248,192 inputs × 17 | 0 differences |
| `entries --fuzz 2`: two mutants of each of those | 496,384 × 17 | 0 differences, except JSON (below) |

`entries` covers `expr`, `expr_for_definition`, `expr_legacy_strand`, `block`,
`block_for_definition`, `idiom`, `idiom_for_definition`, `kind`,
`kind_for_definition`, `fields_for_definition`, `value`,
`value_legacy_strand`, JSON with and without legacy strands, `record_id`,
and whole queries with the files and Surrealism gates off and with strict
record ids. Where an entry point does not require the whole input, both sides
must also stop at the same place, which is what most of its findings were
about: a lone `?` or `+?` ends no expression in `syn`, a number ends at a
letter but not at a digit after its suffix, and so on.

JSON mutants are not at parity, and cannot be checked fully: `syn`'s JSON
grammar falls back to a table name from wherever a failed record id left the
parser, and on some mutants (an unterminated string inside a record-id key)
`syn` itself panics on an `unwrap` in `parse_ident_str`. Every unmutated input
agrees.

The corpus count is statements as the new parser splits them; `syn` splits the
same way (a worded `IF … THEN …; … END` keeps its inner `;`).

The fuzz differential found 68 disagreements in its first run and 26 more
with other seeds, all in corners the corpus does not reach; §7.6 lists what
they taught.

### 7.3 Throughput, allocations, stack

`cargo run --locked --release -p surrealdb-parser-next --example bench` on an
Apple M5 Pro, over the 20,981 corpus statements (1.68 MB) that all three
parsers accept and that lower equal to `syn`'s tree:

| Throughput | MB/s | Statements/s | vs `syn` |
|---|---|---|---|
| `surrealdb-syn` | 42 | 521k | 1.0× |
| `surrealdb-parser` (v3) | 78 | 968k | 1.9× |
| parser-next, parse | 210 | 2.62M | 5.0× |
| parser-next, parse + lower to `sql` AST | 83 | 1.03M | 2.0× |

| Heap allocations per statement | |
|---|---|
| `syn` (including drop) | 15.3 |
| v3, fresh arena | 22.8 |
| v3, reused arena | 3.9 |
| parser-next, fresh arena | 10.1 |
| parser-next, reused arena | **0** |
| parser-next + lowering to `sql` | 14.3 |

Lowering into the boxed `sql` tree costs more than parsing. That cost is the
`sql` AST itself, and it is what Phase B removes.

Re-measured on the `main` base (19,779 statements all three parsers accept):
the allocation counts are unchanged (`syn` 15.5, parser-next 10.1 fresh, 0
reused, 14.4 with lowering), and so is every stack figure below to within
one probe step. Throughput was not re-measured on an idle machine; under a
load average of about 45 parser-next still parsed 3.9× faster than `syn`.

**Minimum native stack** with the budgets lifted, bisected over thread stack
sizes in KiB. 16 is the platform minimum.

| Input | `syn` | v3 | next |
|---|---|---|---|
| 10,000 nested parentheses | 16 | 16 | 16 |
| 10,000 nested arrays | 1,075 | 16 | 16 |
| 10,000 nested objects | 1,075 | 16 | 16 |
| 10,000 nested `-( )` | 611 | 16 | 16 |
| 10,000 nested calls | 611 | 16 | 16 |
| 1,000 nested subqueries | 51 | 16 | 723 |

The new parser's stack grows only with native recursion (nested statements,
lookups with clauses, destructuring), at 0.2–0.7 KB per level. At the deepest
input the budgets accept:

| Input | Default budgets | KiB | `STORED_TEXT` | KiB |
|---|---|---|---|---|
| nested subqueries | 10 | 16 | 200 | 131 |
| nested lookups with `WHERE` | 20 | 16 | — | — |
| nested lookup aliases | 127 | 51 | 511 | 275 |
| nested destructuring | 126 | 16 | 510 | 67 |
| nested `IF … { }` | 20 | 16 | 400 | 243 |
| nested `DEFINE FUNCTION … { }` | 20 | 16 | 400 | 307 |

| Peak heap at nested parentheses | 1,000 | 10,000 |
|---|---|---|
| `syn` | 4.2 MB | 67 MB |
| v3 | 4.2 MB | 34 MB |
| next | 39 KB | 0.61 MB |

The unit test `rejects_deep_nesting_with_bounded_cost` parses 100,000-deep
nesting of nine kinds and gets one depth error each, and with the budgets
lifted parses 30,000-deep nesting on a 64 KiB thread.

### 7.4 Wasm code size

Each parser was built alone with `size-probe`: `wasm32-unknown-unknown`, the
`wasm-release` profile (`opt-level = "z"`, fat LTO, one codegen unit,
`panic = "abort"`), then `wasm-opt -Oz --enable-bulk-memory
--enable-nontrapping-float-to-int --enable-sign-ext` (current rustc emits
bulk-memory operations for this target).

| Module | Bytes | gzip |
|---|---|---|
| `syn` | 1,911,574 | 665,725 |
| v3 | 2,264,337 | 490,006 |
| parser-next, full grammar with streaming and `syn`'s error wording | 133,790 | 65,948 |
| parser-next, prototype (51% of the corpus) | 59,281 | 33,723 |

**Inside the engine.** `surrealdb-wasm` with `kv-mem`, built the same way
(`wasm-release`, `wasm-opt -Oz`), on the branch's base and with the switch
compiled in. `syn` stays linked, so this is the cost of running side by side,
not the end state:

| Engine module | Bytes | gzip |
|---|---|---|
| base (`syn` only) | 14,504,520 | 5,241,408 |
| with parser-next, its lowering and the switch | 14,821,809 | 5,353,109 |
| difference | +317,289 | +111,701 |

The difference is the parser (about 120 KB, as standalone) plus the lowering
into `surrealdb_sql` and the value grammars. The lowering builds the boxed
`sql` tree whose construction code §3.4 attributes to `syn`; both go when
Phase B lowers the flat tree directly (M5) and `syn` is deleted (M6).

The standalone `syn` and v3 numbers include everything their parsers decode or
compile at parse time, such as the `regex` crate for `syn`. The new parser
defers that work to lowering, where the engine already carries those crates.
The engine attribution in §3.4 (≈1.11 MB) is the like-for-like baseline for
the saving.

### 7.5 Streaming

`cargo run --locked --release -p surrealdb-parser-next --example bench -- import 1024 --stream-only`
streams a synthetic 1 GiB import. The import repeats every corpus statement
the parser accepts and is generated on the fly in 64 KiB chunks, so the input
is never held whole. Peak resident set size comes from `/usr/bin/time -l`.

| Run | Throughput | Statements/s | Peak live heap | Peak RSS |
|---|---|---|---|---|
| Streamed, 1 GiB | 213 MB/s | 2.6M | 450 KiB | 10.5 MB |
| Whole input, 257 MiB in memory | 210 MB/s | — | 1.6 GB (tree for every statement) | — |

Memory does not grow with input size; the peak heap is set by the largest
statement in the material (a few large `DEFINE` and `INSERT` statements).
Streaming is no slower than a whole parse: its per-statement arena stays in
cache, while the whole-input tree does not.

### 7.6 What resisted the design

The grammar is defined by `syn`'s behaviour, and reaching full agreement meant
reproducing a number of behaviours no grammar file would state. Each is
mirrored deliberately and covered by a differential mode:

- **Depth budgets.** `syn` charges three budgets at specific call sites
  (parentheses and arguments cost a query level; arrays an object level and an
  expression level; each operator on a spine and each idiom part an expression
  level). A single tree-depth limit accepted deeper input than `syn` in 28 of
  32 probe templates. The explicit stack now saves and restores the three
  budgets per frame (§6.7), which costs nothing per token and keeps native
  recursion out.
- **Idiom continuation.** `syn` looks for idiom parts only right after a
  prime, so `f:x(1).y` is an error and `RETURN f:x(1).y` applies `.y` to the
  whole `RETURN`. The engine tracks whether the current operand is a prime.
  For `<`, `syn` decides an idiom continues when a `-` follows even after
  whitespace, but parses the lookup only when it is adjacent, so `1 < -2`
  makes `1` an idiom with no parts.
- **Associativity.** `==` and the binary range operators chain; `@@` parses at
  equality power but is checked as a relation.
- **Record ids.** In expressions a range key is allowed and no space may
  precede the `:`; in `RELATE` ends and `r"…"` strings the opposite holds. A
  `>` directly after a key demands `..`. `syn` never consumes the digits of a
  `+`-prefixed key, so `a:+1` is always an error; a letter directly after
  negative digits is dropped (`a:-10n` is the string key `-10`).
- **Statement boundaries.** The worded `IF … THEN …; ELSE …; END` consumes
  the `;` inside it, so statement splitting depends on parsing, not just
  bracket matching. Streaming handles this because finality is decided by the
  parser, not by a scan (§6.9).
- **Speculation.** `INSERT (a, b) VALUES …` and `(x, y)` points need the
  parse of a prefix before the choice is known. The append-only arena makes
  backtracking cheap, so these two sites speculate rather than contort the
  grammar.
- **Clause order inside definitions.** `syn`'s checks on combinations of
  clauses depend on their order. Keeping definitions as ordered clause lists
  moves those checks to lowering without losing that order.
- **Literal corners.** Strings take only `syn`'s escapes (no `\/`), and
  `\u{}` is U+0000. Digits joined to an escaped identifier form one record
  key spelled as written (``a:1`x` ``). `<-1>` is a cast, not a lookup. `..`
  on its own is a prefix operator, not a prime, so an idiom after it applies
  to the enclosing statement.
- **Random keys.** `syn` mints random signing keys for access methods defined
  without one, so equal definitions do not compare equal. The harness masks
  them.
- **Where a partial parse stops.** The entry points that do not require the
  whole input made the token after an expression significant: `syn` binds a
  lone `?` as the start of `??` and lexes `+?` as one invalid token, so
  neither ends an expression; a number ends at a letter but not at a digit
  after its suffix (`2f0` is `2f`, then `0`); `s"…"` is a plain string; `??`
  after `.` or `->` is an optional part and then `?`.
- **Failure positions in JSON.** `syn`'s JSON grammar reads a table name from
  wherever a failed record id left the parser, so matching it means
  modelling which failures consume their token (`expected!` and
  `parse_ident` do, closing delimiters do not).

### 7.7 Side by side

`surrealdb_syn::next` holds a process-wide mode, read from
`SURREAL_PARSER_NEXT` on first use or set by `Datastore::with_parser_next`:

- `off` (default): `syn` alone.
- `on`: every routed entry point parses with parser-next and lowers into the
  same `surrealdb_sql` tree. A rejected input reports parser-next's own
  diagnostic, rendered by `syn`'s renderer (§7.8).
- `check`: both parsers on every input; any difference in outcome, tree
  (masking random access keys) or rendered error text is an error. Test
  lanes use this.

Routed: `parse` and `query_with_settings` (core's `parse_with_capabilities`,
so every query a datastore executes), `expr`, `expr_legacy_strand`, `block`,
`idiom`, `kind`, every `*_for_definition` decoder (stored catalog text),
`value`, `value_legacy_strand`, `record_id`, JSON (core's `json` and
`json_legacy_strand`), and `StatementStream` (imports). The mode is
process-wide because these are free functions that callers reach without a
datastore.

Both lanes are cargo-make tasks: `ci-lang-test-parser-next` (the language
suite with `SURREAL_PARSER_NEXT=check`) and
`ci-lang-upgrade-test-parser-next` (the upgrade suite, where only the current
checkout reads the variable, so every definition a released binary stored is
re-parsed through parser-next and checked).

| Run (memory backend unless noted) | `off` | `on` | `check` |
|---|---|---|---|
| Language suite, 3 planner strategies | 6,681 passed | 6,685 passed, 0 divergences | 6,691 passed, 0 divergences in outcome, tree or error text |
| Upgrade suite, every seed to the current checkout (SurrealKV) | 533 passed | — | 533 passed, 0 divergences |

The few runs the totals differ by are tests that hit the suite's timeout
under load (reported as warnings, not failures). The `syn` integration test
`next_switch` parses the same queries, definitions, values and a split
statement stream in all three modes and requires identical trees and error
text.

### 7.8 Error parity

A rejection is a diagnostic: one or two spans, labels, and a message template
in `syn`'s words. The renderer fills the template against the source and
hands the result to `syn`'s renderer, so the text matches character for
character. Checks `syn` makes while parsing but parser-next makes in the
lowering carry `syn`'s spans; the order `syn` meets failures in is kept by
reporting an earlier invalid escape or undecodable prefixed string, or an
earlier `SPLIT`, `GROUP` or `FLEXIBLE` check, before a later syntax error.

The entries harness compares the outcome, tree and rendered error text of
the 17 entry points and settings `syn` exposes, on every statement of the
language-test corpus, the parser crate's test corpora and `syn`'s sample
script, and on every token-start suffix of each (252,317 inputs):

```text
cargo run --locked --release -p surrealdb-parser-next --example entries -- --errors
```

| Inputs × entry points | Count |
|---|---|
| Both accept, equal trees | 992,394 |
| Both reject, identical text | 3,296,991 |
| Both reject, text differs | 4 |
| Tree or outcome differs | 0 |

The four are two suffixes of one corpus statement under the two definition
entry points that read a whole expression. After a negative record-id key whose digits overflow
`u64`, `syn`'s lexer leaves the start of the next token at those digits, so
the error at the following `;` spans the digits too; parser-next reports the
`;` alone. The text is otherwise identical.

`--fuzz 1` replaces each input with one mutant. On the 12 entry points it
reaches before `syn` itself panics on an unterminated string in a JSON
record-id key (pinned in `syn`'s `json_record_id_unterminated_string` test),
2,484,796 of 2,486,003 rejections (99.95%) render identically.

## 8. Migration plan

The new parser runs side by side with the old one behind a switch, and is
proven by differential testing until parity, then cut over. Timeline estimates
assume one engineer.

**M1. Full grammar** — met.
- Every statement and expression form is modelled; no leaf is delegated to
  `syn`; v3's snapshot inputs, a mutation differential and a nesting probe
  agree with `syn` (§7.2).
- **Exit (met):** 100% of corpus statements agree with `syn` (equal or both
  rejecting), with zero false accepts or rejects.
- `syn`'s other entry points, settings and unsigned integers above
  `i64::MAX` followed in M3.

**M2. Errors and fuzz parity** (2–3 weeks).
- Port the renderer.
- Run the differential on rendered error text for every `parsing-error` test.
  Each difference either gets fixed or is listed for an explicit
  message-change review.
- Turn `--fuzz-diff` into a cargo-fuzz target that runs both parsers on the
  same input and compares outcomes, seeded from the corpus and
  `fuzz_sql_parser`'s dictionary.
- Add list- and clause-level recovery.

**M3. Side by side** — met (§7.7).
- Entry points for expressions, blocks, idioms, kinds, field lists, values,
  JSON and record ids; `legacy_strands`, strict record ids and the files and
  Surrealism gates (§7.1, §7.2).
- A process-wide switch (`SURREAL_PARSER_NEXT`, `Datastore::with_parser_next`)
  routes `syn`'s entry points and `StatementStream` through parser-next.
- The language suite passes in `off`, `on` and `check`; the upgrade suite's
  seed axis passes in `check`. Both have cargo-make lanes.
- Still on `syn` alone: `duration`, `datetime`, `table`,
  `function_with_capabilities`, and every error message (M2).

**M4. Canonical printer** (2 weeks).
- Write a printer over the flat tree.
- Prove byte-for-byte equality with `sql::ToSql` across the corpus and the
  fuzz corpus (§6.6), plus the round-trip property and the upgrade suite.

**M5. Phase B lowering** (4–6 weeks).
- `expr::logical::lower`, permissions, grants, catalog compile and `INFO` read
  the flat tree.
- Delete the adapter.
- This is the step that overlaps the planner and VM work most, and needs
  coordinating with those owners.

**M6. Delete** (1 week).
- Delete `surrealdb-syn`, `surrealdb-sql` and the `sql`↔`expr` conversions.
- Retire v3, or keep its useful pieces (`parse-common`, the snapshot harness,
  the stream-consistency test) under the new crate.
- Put a size budget for the front end in CI.

**Remaining: about 3 months for one engineer.** M4 and M5 can overlap once
M3 has landed.

Until M3 ships behind the switch, no persisted format and no query behaviour
changes. The upgrade tests need no new pair until M4 makes the new printer the
writer of stored text. At that point the existing pairs are the guard,
because the text must be byte-identical.

## 9. Risks

**The grammar is defined by `syn`'s behaviour.** Whitespace-sensitive
record-id and range rules, keyword-as-identifier positions and table-vs-field
contexts are subtle.
- Mitigation: the differential harness compares full trees by `==`, not text,
  and requires zero false accepts, on the corpus, on v3's snapshots, on
  mutants of every corpus statement and at nesting limits (§7.2). §7.6 lists
  the behaviours this surfaced. Inputs none of these reach may still differ;
  M2's cargo-fuzz target widens the search.

**Error-message churn.** Language tests pin rendered errors.
- Mitigation: M2's message differential. Any intended wording change is
  reviewed as a product change.

**Stored-text drift.** A printer difference would change catalog bytes.
- Mitigation: §6.6. The printer is not the writer until byte equality holds on
  every corpus and fuzz input.

**Semantic checks moved to lowering.** A check could be lost in the move (for
example builtin names or GROUP BY).
- Mitigation: the harness runs the moved checks and matches `syn`'s
  accept/reject on the whole modelled corpus. M3 runs the full suite.

**Spans outlive their source.** Leaves borrow the query text.
- Mitigation: lowering runs immediately after parsing, while the text is held.
  Anything kept longer (prepared statements, catalog) holds lowered types or
  printed text, never the tree.

**Size creep.** The full grammar with `syn`'s error wording measures 134 KB
standalone; the printer is still to come.
- Mitigation: the size probe runs at each milestone, with a CI budget from M6.

**Phase B scope.** Lowering from the flat tree touches the planner,
permissions and catalog compile. It can slip without blocking M1–M4, which
already remove `reblessive` from the parser and most of `syn`'s size. The
adapter keeps `surrealdb-sql` alive until Phase B lands.

## 10. Open questions

Decided: the new parser replaces both `syn` and v3 (§4); v3's snapshot
inputs run in the differential harness. The three depth budgets are kept,
because matching `syn`'s acceptance at depth requires charging them where
`syn` does (§6.7).

1. **Recovery output.** Should the RPC error payload carry every diagnostic
   from a recovered parse, or keep reporting only the first?
2. **Fans.** EDGE_PLAN puts "the front end on fans" after launch. A 134 KB
   parser makes a fan-side parse for `eval`, `INFO` and stored-text decode
   affordable. Is that worth pulling earlier?
