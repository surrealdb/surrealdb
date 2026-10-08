# Resumable Streaming Bytecode VM & SIMD Execution Engine

**PAX Frames, Fused Synchronous Kernels, Zero-Alloc Arena, and Linear Prefetching**

Status: DESIGN SPECIFICATION (Unification Target)  
Prior art: the logical plan layer, the `surrealedge` experiments and the `shaledb` architecture  
Target Substrates: In-Process Engine, Surrealkv, and ShaleDB Distributed Cloudflare/R2

---

## 1. Executive Summary & Prior Art

Modern distributed and multi-tenant databases require:
1. **Predictable multi-tenant fairness**: Ability to pause/resume queries cooperatively with zero async runtime overhead.
2. **Extreme scan throughput**: Reaching $100\,\text{GB/s}$ across NVMe/R2 storage hierarchies.
3. **Zero per-row memory churn**: Operating entirely within pre-allocated query arenas.

### Empirical Laws from Surrealedge & ShaleDB

- **The De-Async Thesis (E-reblessive-cost)**: Coroutine/async heap recursion incurs a **$+257\%$ throughput regression** ($\approx 10.3\times$ over budget). The core execution loop MUST be pure synchronous Rust code; async wait points live strictly at the host driver / segment seam.
- **Custom Fused Kernels vs Arrow/DataFusion (E-foreign-kernel)**: Fused synchronous zero-allocation kernels achieve **$39.97\,\text{GB/s}$**, outperforming Apache Arrow ($15.52\,\text{GB/s}$, $2.58\times$ slower) and DataFusion ($20.17\,\text{ms}$, $3.00\times$ slower), while compiling to **$16\,\text{KB}$** vs Arrow's **$1.6\,\text{MB}$** wasm binary ($97\times$ code bloat).
- **The "Blob is the Batch" Law (FRAME.md)**: Stored bytes on disk/R2 ARE the in-memory scan format. Zero transformation between page cache and kernel SIMD registers.
- **Frame Sizing Law (AMENDMENT 03)**: Slicing ranged column reads only pays off when frame size exceeds the OS page size ($16\,\text{KiB}$ on Apple Silicon / modern kernels). Frames must be sized $\ge 64\,\text{KiB}$ (typically $256\,\text{KiB}$ – $1\,\text{MiB}$) to unlock $4.8\times$ – $20.7\times$ ranged-read bandwidth gains.

---

## 2. PAX / Packed Frame Memory Layout

```text
┌────────────────────────────────────────────────────────────────────────┐
│                        CANONICAL PAX FRAME LAYOUT                      │
├────────────────────────────────────────────────────────────────────────┤
│ Header: Magic (4B) | Version (2B) | RowCount (4B) | ColCount (2B)      │
├────────────────────────────────────────────────────────────────────────┤
│ Column Directory: [ColId (2B) | TypeId (2B) | Offset (4B) | Len (4B)]   │
├────────────────────────────────────────────────────────────────────────┤
│ Zone Maps & Mini-Runs: [MinVal | MaxVal | NullCount] per column        │
├────────────────────────────────────────────────────────────────────────┤
│ Validity Bitmaps: 1 bit per row per nullable column (64-bit aligned)  │
├────────────────────────────────────────────────────────────────────────┤
│ Fixed-Width Runs: Dense scalar arrays (u64, i64, f64, u128, bool)      │
├────────────────────────────────────────────────────────────────────────┤
│ Var-Width Offsets: uint32 offset array [0, len0, len0+len1, ...]       │
├────────────────────────────────────────────────────────────────────────┤
│ Var-Width Data: Contiguous UTF-8 / byte buffer                         │
└────────────────────────────────────────────────────────────────────────┘
```

### Layout Properties (F1 – F4)
1. **Borrowed Batches**: `ValueBatch` is a zero-copy borrowed slice over a resident frame:
   $$\text{lifetime}(\text{Batch}) \le \text{lifetime}(\text{Segment}) \le \text{lifetime}(\text{Arena})$$
2. **Co-Access Column Families**: Columns frequently queried together (e.g. `WHERE a > 1 AND b < 10`) are clustered contiguously in the frame. Ranged GET requests coalesce adjacent columns into a single storage read.
3. **SIMD Alignment**: All fixed-width scalar runs and validity bitmaps are 64-byte aligned for AVX-512 and ARM NEON / `std::simd` vectorization.

---

## 3. Resumable Cooperative Execution Model

### 3.1 Synchronous Interpreter Core

```rust
pub trait HostDriver {
    /// Provide next batch of data from storage or upstream segment.
    fn fetch_next_batch(&mut self) -> Result<Option<FrameView>, ExecError>;
    /// Issue batched asynchronous prefetch for upcoming keys/pages.
    fn prefetch(&mut self, keys: &[Key]);
}

pub struct BytecodeVM<'arena> {
    /// Arena allocator for temporary scratch, slot windows, and accumulators.
    arena: &'arena BumpArena,
    /// Linear program bytecode.
    program: &'arena [Instruction],
    /// Current program counter.
    pc: usize,
    /// Stack frame slots.
    slots: &'arena mut [SlotValue],
    /// Execution budget metering.
    budget: Budgets,
}
```

### 3.2 Segment Cuts & Effect Boundaries (AMENDMENT 04)

Instead of suspending inside deeply nested async Rust futures (`Box<dyn Future>`), effectful operations (I/O, multi-gets, mutations, timeouts) force a **compile-time segment cut**:

```text
[Pipeline Segment 0 (Sync Scan + Filter + Project)]
                      │
                      ▼ (Yields batch at Segment Boundary)
   [Host Driver: Batched Async Multi-Get / I/O Prefetch]
                      │
                      ▼ (Hands next batch & prefetch data)
[Pipeline Segment 1 (Sync Join Probe + Aggregate)]
```

- Query execution within a segment is **100% synchronous**.
- Pausing for multi-tenant fairness charges a `fuel` budget per loop back-edge and batch boundary. When fuel expires, the VM saves `pc` and returns `ExecutionSignal::Yield(pc)`.
- Resumption cost: $\approx 0\,\text{ns}$ (reading instruction at `pc`, passing active slot slab).

---

## 4. Bytecode Instruction Set Architecture (ISA)

### 4.1 Dense Linear Instruction Format

Instructions are packed into 8-byte or 16-byte fixed structures:

```rust
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Instruction {
    pub opcode: OpCode,
    pub flags: InstFlags,
    pub dst: u16,
    pub src1: u16,
    pub src2: u16,
    pub imm: u32,
}
```

### 4.2 Core OpCode Categories

1. **Memory & Slot Access**: `LOAD_COL_SLOT`, `LOAD_CONST`, `STORE_SLOT`, `EXTRACT_PATH_SLOT`.
2. **SIMD & Arithmetic**: `ADD_I64`, `SUB_I64`, `MUL_I64`, `DIV_I64`, `ADD_F64`, `MUL_F64`.
3. **Comparison & Predicates**: `CMP_EQ`, `CMP_NEQ`, `CMP_LT`, `CMP_LTE`, `CMP_GT`, `CMP_GTE`, `CMP_CONTAINS`.
4. **Vector Mask Operations**: `MASK_AND`, `MASK_OR`, `MASK_NOT`, `COMPRESS_LANES`.
5. **Flow Control & Aggregates**: `JMP_IF_ZERO`, `JMP_IF_NONZERO`, `ACC_COUNT`, `ACC_SUM`, `ACC_MIN`, `ACC_MAX`, `YIELD_BATCH`.

### 4.3 Promotion Lattice & Homogeneous Kernels

To prevent $N^2$ combinatoric explosion of typed kernels across mixed variants (e.g. `Int + Float`, `Decimal + Int`):
- Mixed types normalize at lowering via an explicit promotion lattice (`Int -> Float -> Decimal`).
- The VM executes only homogeneous kernels ($T \times T \to T$) or tagged fallback adapters for `Unknown`/`Any`.

```text
      Decimal
      ▲     ▲
     Float  │
      ▲     │
     Int ───┘
```

### 4.4 Superinstruction Fusion

The bytecode optimizer identifies common operator chains and emits fused superinstructions at query schedule time:

$$\text{Filter}(a > 10) \land \text{Sum}(b) \implies \text{FUSED\_FILTER\_GT\_I64\_ACC\_SUM\_I64}$$

Fused kernels execute in a single unrolled loop with vector registers without spilling intermediate booleans to RAM.

---

## 5. Masked Evaluation Totality Law (CONDITION 01)

### 5.1 The Safety Hazard

When evaluating a SIMD vector under a selection mask (e.g. mask `[1, 1, 0]`), evaluating lane 2 for fallible or state-advancing operations can cause:
1. Spurious runtime panic or error on a row excluded by `WHERE`.
2. Unauthorized side-channel permission evaluation on inaccessible rows.
3. State side-effects (`sequence::nextval` advancing state on rejected rows).

### 5.2 The Totality Contract

```rust
pub trait ScalarFunctionDefinition {
    /// Returns whether this scalar function is total over all domain values.
    /// Total means f(x) never panics, never raises, and has no side-effects.
    fn is_total(&self) -> bool {
        false
    }
}
```

- If an operator or builtin declares `is_total() == true` (e.g. `+` without overflow, string length, comparisons), it is safely executed across ALL SIMD lanes with masked write-back.
- If `is_total() == false` (e.g. `Divide`, `Cast`, `ParamPermissionCheck`, `nextval`), the VM **refuses direct SIMD execution** and executes **Partition-by-Condition**:
  $$\text{Input Batch} \xrightarrow{\text{Compress}} \text{Surviving Lanes} \xrightarrow{\text{Execute}} \text{Result} \xrightarrow{\text{Expand}} \text{Output Batch}$$

---

## 6. Linear Prefetch Frontier Analysis

Because bytecode instructions and basic blocks are linear, the compiler derives the **Read Frontier** ahead of execution:

```rust
pub struct PrefetchFrontier {
    /// Target table and secondary index definitions.
    pub table: String,
    /// Projected key expressions / record IDs needed by subsequent segments.
    pub key_expressions: Vec<PhysicalExprId>,
    /// Range bounds for lookahead scans.
    pub scan_bounds: Option<KeyRange>,
}
```

1. **Ahead-of-Time Multi-Get**: Before entering a `LookupJoin` or record link dereference segment, the host driver inspects the prefetch frontier and issues asynchronous multi-gets against ShaleDB / SurrealKV.
2. **Zero Async Bubble**: When the VM reaches the segment, the requested pages/values are already resident in cache, achieving continuous linear execution.

---

## 7. Budgets, Metering, and Verification

### 7.1 Bounded Execution Budgets

```rust
pub struct Budgets {
    /// Instruction fuel remaining before forced cooperative yield.
    pub fuel_remaining: u64,
    /// Call frame recursion depth limit (bounds stack memory).
    pub max_call_depth: u16,
    /// Memory watermark (bytes allocated in arena).
    pub memory_watermark_bytes: usize,
    /// Cooperative deadline timestamp.
    pub deadline: web_time::Instant,
}
```

Exhaustion of any budget results in a typed `ExecutionSignal::BudgetExhausted(Kind)`, never silent truncation or undefined behavior.

### 7.2 Zero-Allocation Harness Verification

All bytecode VM loops are gated in CI with an allocation-counting global allocator:
$$\text{Allocations}_{\text{steady\_state}}(\text{Row}) = 0.00$$
$$\text{Bytes}_{\text{steady\_state}}(\text{Row}) = 0.0$$

---

## 8. Multi-Representation Data Model (Row, Slotted, PAX & Dynamic)

The Bytecode VM is **data-representation agnostic** at the slot level. It is not coupled exclusively to PAX frames.

```rust
pub enum SlotValue<'arena> {
    /// 1. Typed Primitive: Direct scalar (i64, f64, bool, u128, etc.) for SIMD / PAX columns.
    Scalar(PrimitiveVal),
    /// 2. Borrowed Raw Byte Slice: Direct slice of stored bytes for positional extraction.
    RawSlice(&'arena [u8]),
    /// 3. Dynamic / Schemaless Value: Full SurrealDB Value (Object, Array, Geometry, etc.).
    Dynamic(&'arena Value),
    /// 4. Tagged Variant: Value with runtime type tag for mixed/changing types across rows.
    Tagged { type_id: u16, data: u64 },
    /// 5. Memoized Lookup Table: Pre-evaluated hash sets / vectors from partial hoisting.
    MemoRef(&'arena PrecomputedSet),
}
```

### 8.1 Handling Changing & Heterogeneous Types
When data types vary per row on open/schemaless tables (e.g. `age: 20` on row 1, `age: "unknown"` on row 2):
1. **Homogeneous Run Fast Path**: While consecutive rows match the anticipated primitive type, the VM executes fast unboxed SIMD instructions.
2. **Dynamic Fallback**: When a type mismatch is detected via tag inspection, the VM routes that specific row through the single shared **Tagged Fallback Adapter**, preserving 100% SurrealQL semantics without abortion or panic.

---

## 9. Two-Segment Execution & Serialized Schedule Distribution

### 9.1 Segment Cuts for Hoisted Computation

```text
[Coordinator]
      │
      ├─► [Segment 0: Initialize Memos] ── Evaluates hoisted subqueries ONCE into Memo Slots
      │
      └─► [Segment 1: Streaming Fan-Out] ─ Sends (Schedule + Memo Tables + Extent) to Fan Workers
```

1. **Segment 0 (Initialization)**: Evaluates all hoisted, invariant subqueries and pre-gates before any storage scan begins.
2. **Segment 1 (Leaf Execution)**: Shipped to fan workers as a pre-compiled, self-contained `Schedule`. Fan workers read pre-computed memo tables via `OP_CONTAINS_MEMO` with zero subquery overhead.

---

## 10. Bytecode Optimization: CSE, Subroutines & Loop Unrolling

### 10.1 Common Subexpression Elimination (CSE) & Bytecode Deduplication
During bytecode schedule compilation:
- The compiler maintains a Value Numbering Table mapping canonical expression signatures to slot registers.
- If an expression (e.g. `math::abs(score) + 1.0`) appears in both `WHERE` and `SELECT`, the compiler emits the calculation ONCE into slot register $r_k$, and reuses $r_k$ across both sites.

### 10.2 Subroutine Call Convention
For large or recursive functions where inlining would blow up bytecode size:
```rust
// Subroutine Call Instruction
OP_CALL_SUBROUTINE { func_idx: u16, arg_base_slot: u16, return_slot: u16 }
// Return from Subroutine
OP_RET { return_slot: u16 }
```
- Subroutine frames execute within the existing arena slot slab by bumping the frame window index, requiring zero heap allocation.

### 10.3 Batch Vectorization & Unrolling
- For homogeneous scalar runs (e.g. fixed-width `i64` PAX columns), the VM unrolls inner loop steps in 4x or 8x lanes using SIMD vector registers, amortizing the instruction fetch/decode overhead to $< 0.5\,\text{ns}$ per element.

---

## 11. Flat Register Window & Zero-Overhead Gosub/Return Subroutines

Following production VM architecture (SQLite VDBE, LuaJIT):
1. **Pre-Allocated Register Slab**: The query planner pre-calculates maximum register requirements ($N_{\text{regs}}$) and allocates a flat `SlotValue` array inside the `QueryArena`.
2. **Zero-Overhead `Gosub` / `Return`**:
   ```rust
   pub enum Op {
       /// Store pc + 1 into registers[return_reg] and jump to target_pc.
       Gosub { target_pc: u32, return_reg: u16 },
       /// Restore pc = registers[return_reg].as_u32().
       Return { return_reg: u16 },
   }
   ```
   - Eliminates dynamic call stack frame allocation and heap pointer chasing.
   - Used for factoring out nested path lookups, field permission guards, and cold error stubs.
3. **Suffix-Tree Bytecode Outliner**: Scans emitted bytecode sequences and outlines $\ge 3$ repeated opcodes into subroutines, shrinking bytecode schedules for network transmission to edge workers.

---

## 12. Cooperative Fuel Metering & Host Driver Asynchrony

Execution separates synchronous computation from asynchronous host scheduling:

```rust
pub trait Charge {
    fn enter(&mut self, fuel: u32) -> Result<(), Exhausted>;
    fn frame_push(&mut self) -> Result<(), Exhausted>;
    fn frame_pop(&mut self);
    fn kernel(&mut self, bytes_in: u64, bytes_out: u64, rows: u32, fuel: u64) -> Result<(), Exhausted>;
}
```

1. **Amortized Metering**: Fuel is charged at loop back-edges, batch boundaries, and kernel entry points rather than per scalar instruction, preserving CPU branch prediction and SIMD vectorization.
2. **Sync Core + Async Host Seam**: The Bytecode VM loop is 100% synchronous. At batch boundaries, it yields control (`SegmentOut::More(Batch)`) to the asynchronous host driver to handle network RPCs, credit refills, and timer deadlines (`now >= deadline`).
3. **Non-Blocking Storage Residency**: Un-cached storage segments return typed `NotResident` signals, triggering asynchronous lookahead prefetching without blocking the single-threaded isolate event loop.

---

## 13. Hierarchical Arena Memory Lifecycles

```text
┌────────────────────────────────────────────────────────┐
│ QueryArena (Query Lifetime)                            │
│  - Query Plan, Physical Expressions, Symbol Tables     │
│                                                        │
│   ┌────────────────────────────────────────────────┐   │
│   │ SegmentArena (Batch / Stage Lifetime)          │   │
│   │  - Temporary hash buckets, sort buffers        │   │
│   │  - Reset and reused between batches (0 mmap)   │   │
│   │                                                │   │
│   │   ┌────────────────────────────────────────┐   │   │
│   │   │ BatchView (Zero-Copy PAX Slices)       │   │   │
│   │   │  - Column arrays: &[T], null bitmasks  │   │   │
│   │   │  - Borrowed directly from block cache  │   │   │
│   │   └────────────────────────────────────────┘   │   │
│   └────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────┘
```

1. **`QueryArena`**: Allocates query-lifetime structures, freed in a single bulk operation at query termination.
2. **`SegmentArena`**: Allocates 64 KB – 2 MB blocks via bump pointer for intermediate stage buffers. `arena.reset()` clears the pointer to zero while retaining allocated blocks, avoiding OS memory churn.
3. **`BatchView`**: Zero-copy borrowed slices over resident PAX frame bytes.
4. **Hard Memory Caps**: Enforces reservation grants against `ExecutionBudgets::memory_watermark`. If memory exceeds the hard cap and cannot spill, execution halts with `E_BUDGET_EXHAUSTED` and detailed diagnostics.

---

## 14. Bytecode Multi-Tier Prefetch & Ranged Offset Opcodes

```rust
pub enum Op {
    /// Issue multi-tier prefetch hint for a target key or record ID.
    PrefetchKey { key_slot: u16, tier: PrefetchTier },
    /// Issue range prefetch hint across [start_key, end_key).
    PrefetchRange { start_slot: u16, end_slot: u16, max_keys: u32, tier: PrefetchTier },
    /// Read a specific byte range of a stored value directly into an arena slice.
    ReadRangeSlot { key_slot: u16, offset: u32, len: u32, dst_slot: u16 },
}
```

### 14.1 Non-Blocking Execution Flow
1. The Bytecode VM dispatches `PrefetchKey` / `PrefetchRange` opcodes at segment entry points.
2. The asynchronous host driver forwards hints to ShaleDB's `Authority::prefetch` / `Authority::prefetch_range` in the background.
3. When the execution cursor reaches `ReadRangeSlot`, the requested byte slice is served directly from resident memory with zero-wait page cache hits.

---

## 15. The Prefetch Boundary: Host Driver vs. In-Loop VM

```text
[Bytecode VM (Segment 0)] ── Emits extracted keys to Slot Slab ──► [Yield at Segment Boundary]
                                                                          │
                                                                          ▼
                                                         [Host Driver Lookahead]
                                                         - Gathers batch keys from Slot Slab
                                                         - Dispatches `shale.prefetch(keys, Hot)`
                                                         - Awaits I/O without blocking VM loop
                                                                          │
                                                                          ▼
[Bytecode VM (Segment 1)] ◄── Takes resident cache hits ───────── [Resume Segment 1]
```

### 15.1 Why the Host Driver Prefetches Instead of Scalar Opcodes
1. **Preserving Synchronous ILP**: Scalar instructions must remain synchronous and tightly packed in registers. Injecting async future polling or storage syscalls into every instruction destroys CPU pipelining and SIMD vectorization.
2. **Batch Coalescing**: The Host Driver batches hundreds of extracted foreign keys from a batch and issues a single coalesced multi-get against ShaleDB, amortizing round trips.

---

## 16. Edge Sandboxing (W^X) & Two-Tier Engine Strategy

```text
┌────────────────────────────────────────────────────────────────────────┐
│                        TWO-TIER EXECUTION STRATEGY                     │
├──────────────────────────────────────────┬─────────────────────────────┤
│ Tier 0: Universal Synchronous Bytecode   │ Tier 1: Adaptive JIT        │
│ VM + Pre-Compiled SIMD Kernels           │ (Cranelift, Server-Only)    │
├──────────────────────────────────────────┼─────────────────────────────┤
│ • Universal (WASM, Cloudflare Workers)   │ • Gated behind `feature=jit`│
│ • Zero startup latency (< 1 µs)          │ • Cranelift fast backend    │
│ • 100% W^X & Sandbox compliant           │ • Fuses hot batch loops     │
│ • Amortized chunk dispatch (< 1 cycle)   │ • Triggered on large scans  │
│ • 50 – 100 GB/s throughput via AOT SIMD  │   (> 100k rows / > 10 ms)   │
└──────────────────────────────────────────┴─────────────────────────────┘
```

### 16.1 The Edge $W \oplus X$ Law
In Cloudflare Workers (V8 Isolates), WebAssembly runtimes, and mobile sandboxes, dynamic native code generation (`mprotect(PROT_EXEC)`) is **strictly forbidden**.

### 16.2 Why the Vectorized Bytecode VM is the Universal Engine
1. **100% AOT & Sandbox Safe**: All kernels are compiled Ahead-Of-Time into the static binary or `.wasm` artifact by `rustc`.
2. **SIMD-Saturated Scan Speed**: By operating over contiguous vector chunks (1024 values) using explicit SIMD (`std::simd`, AVX-512, ARM NEON, WASM SIMD128), inner loops run with optimal instruction scheduling and zero interpreter dispatch overhead, hitting physical memory bandwidth limits ($50 - 100+\,\text{GB/s}$).
3. **Flying-Start Server Acceleration**: On native servers (where JIT is permitted), long-running analytical queries start on Tier 0 instantly ($< 1\,\mu\text{s}$) and seamlessly hand off subsequent morsels to Cranelift JIT in the background.

---

## 17. TIN-PAX Vectorized Full-Text Opcodes & BM25 Scoring

```rust
pub enum Op {
    /// Probe chunk Minimal Perfect Hash / FST for term offset and max score.
    FtsProbeDict { dict_slot: u16, term_str_idx: u16, dst_meta_slot: u16 },
    /// Zero-copy borrow of Elias-Fano or Roaring posting slice from resident PAX frame.
    FtsLoadPostings { frame_slot: u16, meta_slot: u16, dst_post_slot: u16 },
    /// Bitwise SIMD intersection of posting bitsets across 64-bit vector registers.
    FtsSimdAnd { post_slot_a: u16, post_slot_b: u16, dst_mask_slot: u16 },
    /// Vectorized 16-lane float BM25 scoring kernel over packed doc lengths and term frequencies.
    FtsEvalBm25Simd { mask_slot: u16, doc_len_slot: u16, tf_slot: u16, dst_scores_slot: u16 },
}
```

### 17.1 Zero-Deserialization Vectorized Matching
1. The Bytecode VM executes `FtsSimdAnd` over contiguous 64-bit words via `_mm512_and_si512` / Neon vector registers.
2. Zero words immediately short-circuit downstream scoring and attribute extraction for those 64 rows.
3. Matching lanes are scored in sub-nanosecond time using fused multiply-add (`_mm512_fmadd_ps`).

---

## 18. GPU Bytecode Execution & Heterogeneous Compute Shaders

```rust
pub enum Op {
    /// Dispatch data-parallel vector and arithmetic subtrees to GPU compute queue.
    GpuDispatchKernel {
        pipeline_id: u16,
        input_buffer_slot: u16,
        output_buffer_slot: u16,
        workgroup_count: u32,
    },
    /// Await GPU execution fence and synchronize shared memory buffer.
    GpuSyncBuffer { buffer_slot: u16 },
}
```

1. **Cross-Platform WGSL Shaders**: Compute shaders compiled via Rust `wgpu` into Vulkan, Metal, DX12, and WebGPU targets.
2. **In-Kernel Bitonic Top-K**: The GPU shader performs parallel candidate reduction in shared memory, transferring only the final top-$K$ indices and distances back to the host query arena.

---

## 19. Morton Index Opcodes & Multi-Range Parallel Slicing

```rust
pub enum Op {
    /// Calculate BIGMIN jump target for active Z-order scan cursor.
    MortonBigminJump {
        curr_z_slot: u16,
        box_meta_slot: u16,
        next_z_slot: u16,
    },
    /// Issue coalesced multi-range prefetch across disjoint Z-intervals.
    PrefetchMultiRange {
        intervals_slice_slot: u16,
        tier: PrefetchTier,
    },
}
```

### 19.1 Non-Blocking Multi-Range Execution
1. The physical planner emits the list of coalesced Z-intervals into `intervals_slice_slot`.
2. The Host Driver dispatches parallel `io_uring` / HTTP `Range` fetches in the background.
3. The synchronous Bytecode VM scans resident intervals and executes `MortonBigminJump` to step between disjoint intervals with zero stall time.

---

## 20. Bloom Digest Opcodes & Pre-Fan-In Sealing Fences

```rust
pub enum Op {
    /// Record an accessed or modified storage key into the active Bloom filter digest.
    RecordKeyDigest { key_slot: u16, digest_slot: u16 },
    /// Execute fast bitwise conflict verification of the Bloom digest against log deltas.
    VerifyDigestConflict {
        leg_digest_slot: u16,
        delta_digest_slot: u16,
        dst_conflict_flag_slot: u16,
    },
    /// Seal and admit certified partial frame into the coordinator fold accumulator.
    SealCertifiedLeg { leg_idx: u16, partial_frame_slot: u16 },
}
```

### 20.1 In-Loop Digest Recording
- During execution of storage reads and writes, the Bytecode VM updates the local 512-byte blocked Bloom filter in register memory with zero memory allocations.
- Pre-fan-in sealing uses SIMD bitwise tests to verify freshness before merging results into the coordinator's `FoldFrame`.
