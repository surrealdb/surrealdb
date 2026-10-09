# Memory: a shared pool, and a ceiling on what one query pins

A query's spillable state never fails it. When the pool is short a query holds
bounded memory by spilling, or goes slower by waiting, and as a last resort it
is granted memory past the node's limit, which is accounted rather than raised.
What a query cannot spill (a gathered value, a value its program binds or
builds, its transaction's buffers) is its own to bound: it fails with
`QueryBeyondMemoryThreshold` when its own pinned bytes would pass its ceiling,
`query_memory_max`, and never because another query used memory. Within its
ceiling a query's answer is the one it gives with memory to spare.

The code is `exec/memory` (the governor, the lease, the temporary store) and its
callers in `exec/vm`.

## The pool

- **One governor per node** (`MemoryGovernor`), sized by the host or by
  `memory_limit`, shared by every query the node runs. A host may supply its
  own; the datastore otherwise holds its queries to a `FairGovernor`.
- **One lease per query** (`Lease`) that holds state across its input. A lease
  has a *minimum* the governor never takes back, and a *grant* above it that
  the query borrows from the node's free memory. The minimum includes the
  *headroom*: the part buffer a spilled run is written through and the read
  buffers a merge holds, so a query can always spill. A lease's *pinned* bytes
  are those it cannot spill; the rest of what it holds is *spillable*.
- **Fair share.** The limit divided among the open leases, never below a
  lease's minimum. Growth up to the fair share is always granted and never
  waits.
- **Two reclaim floors.** Spillable growth lowers another lease no further than
  the bytes it cannot spill plus its headroom, so it creates no overcommit and a
  refused grower spills. A new lease's minimum, and pinned growth within the
  asker's fair share, lower every lease above its fair share to it, pinned bytes
  included: a borrower gives back what it borrowed.
- **Overcommit is derived.** The pinned bytes a lease holds above its usable
  grant (the grant less its headroom) are its overcommit. The governor stores no
  counter: `FairGovernor::overcommitted` is the sum over the open leases, read
  under the governor's lock, and `FairGovernor::overcommits` counts each run of
  overcommit once, however many requests extend it, with one `tracing` warning
  per run. The grants' sum stays within the greater of the limit and the sum of
  the minimums.
- **Grants are lowered at flush.** A query observes its grant where it hands
  rows on. A breaker holding more than its grant spills there, so what a lowered
  grant revokes comes back within one batch. Pinned bytes above a lowered grant
  stay held until the lease releases them.

Operators charge a lease through a `Reservation`, in chunks
(`memory_reserve_chunk`), never a row at a time.

## What a query does when its lease is short

In order:

1. **Spill** what can be spilled. A sort writes sorted runs, an aggregate
   grouped by a key writes its groups' saved states in key order, and a
   distinct and a hash join's build write theirs. Runs are written and read
   through one part buffer each, so the memory a spill takes is bounded
   whatever it spills, and are merged no more than the fan-in at once, in
   several passes otherwise. A breaker spills at its next flush. Spillable
   state counts toward the lease and never toward the ceiling.
2. **Hold through `QueryMemory::insist`** what cannot be spilled. The bytes
   have first been counted against the ceiling, from the query's own pinned
   bytes only. The governor then decides under its lock, with `s` the shortfall of the lease's usable grant
   and `f` its fair share:
   1. *Covered.* The grant already holds the bytes.
   2. *Within the fair share.* If the grant can reach the bytes by growing to
      `max(f, minimum)`, it grows from free memory, or from what borrowers above
      their fair share give back. This never waits. Where the minimums alone pass
      the limit, the excess is the asker's overcommit.
   3. *Borrow.* Past its fair share the grant grows from free memory that no
      waiting lease has claimed. Nothing is reclaimed from others for a
      borrower.
   4. *Displace.* The bytes are taken onto what the lease itself can spill.
      The lease then holds more than its grant, and its own breakers spill at
      their next flush.
   5. *Wait.* A lease waits while an older lease waits, or while the leases that
      are not waiting hold memory above their guarantee that, with the
      unclaimed free memory, covers `s`: that memory comes back when they spill,
      when their statement ends, or when their query does. A waiting lease
      records `s` as its claim, which younger leases leave free for it, and
      leases are ordered by age.
   6. *Proceed past the limit.* When the query's wait budget is spent, or the
      lease is the oldest that is waiting or asking and nothing that is running
      can give back what it needs, the bytes are granted past the limit and
      counted as the lease's overcommit.
3. **The wait is bounded.** A query waits at most `memory_wait` (default 1 s)
   in all, across every wait of its life, and at most half the time its context
   has left. When the bound runs out it proceeds past the limit. A wait is
   cancellation safe, and ends with the context's own error if the query is
   cancelled or past its deadline.
4. **Overcommit is given back.** A borrower's pinned bytes above its grant are
   released as the lease releases pinned bytes, and a lease whose spillable
   bytes its grant covers does not spill because of them. What a query charged
   for a transaction's buffers is given back when the transaction ends,
   committed or cancelled, so a request of many transactions holds one
   transaction's charge at a time.

## The ceiling

`query_memory_max` (default: the node's `memory_limit`) bounds what one query
pins. The single error, in `QueryMemory::within_ceiling`, is raised from the
query's own pinned bytes only: a request that would take them past the ceiling
fails with `QueryBeyondMemoryThreshold`, and nothing another query holds can
cause that. Every path that raises the pinned bytes counts them first: a pinned
account adds each charge to the query's count in one atomic step, and a
transaction's buffers are checked before they are counted. Tasks of one query
running at once (region workers) may pass the ceiling by the bytes each adds
between its check and its count.

The ceiling counts the bytes the query holds, exactly, and not the chunks its
lease reserves for them: a query is never refused while the bytes it holds are
within the ceiling, however many pinned accounts it keeps (a state's ledger, a
gather's account, the unspillable buffer) and however each rounds up to whole
reservation chunks.

Spillable state is never pinned, so a sort, a group, a distinct or a hash join
with a temporary store never meets the ceiling. A statement streamed to its
client holds only one batch of rows, so a plain `SELECT ... ORDER BY` of any
size never meets it either. These do: a gathered value (`LET $x = (SELECT ...)`,
`RETURN (SELECT ...)`), a value a program binds or builds, a write transaction
that buffers more than the ceiling on a backend that does not bound its write
buffer, and a breaker that has no store.

## Values a program builds

A program holds values across instructions in a fixed set of places, and each is
charged to the query's lease as pinned bytes, at what `size::value_bytes` says
the value weighs, while the program holds it:

- a bound parameter (`LET`, `FOR` name), charged as the difference from what the
  slot held before, and again for the copy the block's frame holds. A frame
  derives from the one before it, so the copies of every binding made in a block
  stay held, and are charged, until the block that bound them ends;
- a `FOR` loop's source array, from the loop's start until it ends;
- the value a nested statement run once keeps, until the group that owns it
  starts again;
- the arguments of a function call and the arguments and captures of a closure
  call, at call entry, until the call ends;
- a region worker's copy of its statement's slots, the slots' bytes again
  (the shared frames are not), until the worker's batch ends.

A program's charges settle against one account. While an account has reserved
nothing and they total no more than one reservation chunk, they are counted
exactly toward the ceiling and reserve nothing from the lease, so a query that
holds only small values opens no lease, and the frames of many small calls do
not each pin a chunk. Past one chunk the account reserves whole chunks from the
lease. An account gives back reserved chunks only when it holds more than one
whole chunk beyond what its total needs, so a total that moves across a chunk
boundary, above or below one chunk, does not release and reserve a chunk on
every change.

One chunk is the governor's resolution of what a program holds. The bytes of an
account that has reserved nothing, at most one chunk, are not in the lease, so
the governor's grants, overcommit and wait decisions do not see them; only the
ceiling does. A node running many queries, or deeply recursive calls, that each
hold just under a chunk of bound values holds more than the governor accounts
for, by at most a chunk for each state. The cost is the other side of the
choice: a call whose arguments are small neither opens a lease nor takes the
governor's lock.

The rows a `LET`'s statement gathers are charged while they are gathered, and the
binding that takes the value takes over that charge: the bytes stay reserved as
the value changes hands, so no other query can take them in between.

A value is charged where it is kept and not where it is built, so every builtin,
operator, function and closure that builds one is covered. What is not charged:

- an expression's intermediate values and a builtin's result that is not bound,
  which live within one statement step. Generators are capped per call by
  `generation_allocation_limit`, and combinators over bound values are bounded by
  a small multiple of what is already charged;
- a function's or closure's result, which is charged where its caller binds it;
- the rows a statement streams to its client, which the transport owns, and the
  one batch of encoded rows a streamed statement holds;
- the shared parameters a name resolution rebuilds a frame from;
- a sort's slab of stored rows, which is spillable state charged to the sort;
- the script runtime's heap, bounded by `scripting_max_memory_limit`.

A closure's weight includes the values it captures.

## No deadlock

- **Termination.** Every call to `insist` returns within the wait bound, one
  wait slice and one decision, or returns the context's cancellation or timeout
  error. This does not depend on what other queries do, including queries
  blocked on resources the governor cannot see, such as KV row locks, a channel
  or client backpressure. The ceiling turns nothing into a longer wait.
- **The wait-for relation is acyclic.** A lease `X` waits on `Y` only when `Y`
  is not waiting and holds memory it can give back, or when `Y` is waiting and
  older than `X`. A lease that is not waiting waits on nothing, and every edge
  between waiting leases goes to a strictly older one, so a cycle would need an
  age smaller than itself.
- **The oldest waiter makes progress.** It waits only on running leases. When
  none can give back what it needs its next decision proceeds past the limit,
  and it is woken at once, because every event that can change the answer moves
  the governor's epoch: a release, a close, an open, a waiter joining or
  leaving, or a change of a waiter's claim. A waiter registers for the epoch
  before it compares it, so no wake-up is lost, and a release with no lease
  waiting is one atomic load. A running lease that holds the memory either
  releases it, which moves the epoch, or is blocked outside the governor, which
  the bound covers.
- **No starvation.** Waiters are served oldest first, and a younger lease cannot
  take memory an older waiter has claimed. Growth within the fair share never
  waits, so small queries are served first in effect. Oldest-first is also the
  order that keeps the most work already done, and a smallest-first order would
  starve a large query while small ones keep arriving. The bound turns any
  remaining wait into progress.
- **What bounds the node.** The memory the leases hold is at most the grants'
  sum, plus their overcommit, plus one batch of spillable bytes over a lowered
  grant. Each query's pinned bytes are at most its ceiling. Nothing else bounds
  overcommit across the node by default.

## What a query can fail for

| Failure | Where | Why |
|---|---|---|
| The pool: the governor, a lease, a reservation | nowhere | A query spills, waits or proceeds past the limit. |
| `QueryBeyondMemoryThreshold` | `QueryMemory::within_ceiling` | The query's own pinned bytes would pass `query_memory_max`. |
| `QueryBeyondMemoryThreshold` | the `SURREAL_MEMORY_THRESHOLD` guard (`ALLOC.is_beyond_threshold`) | The process guard, opt-in. It reads the process, not a query: overcommit can push the process past the threshold, and the guard then fails whichever query checks next. Where a threshold is set, keep `query_memory_max` well below the threshold less `memory_limit`. |
| Row-count limits | `gql_max_join_build_rows` and the distinct's | The limits the host's join build and distinct operators raise. |
| A spill past the temporary store's quota | `QueryMemory::spilled` | A disk quota of the store, not the memory budget. The default file store has none. |
| Cancellation and timeout | the query's context | A wait ends with them. |

## Storage owns its buffers

A transaction's write buffer is the backend's. `Transactable::bounds_write_buffer`
is true where the backend stages writes outside memory or flushes them before
commit, and the VM then does not count what the transaction has written against
the query's memory. Otherwise it counts them through `insist`: the charge is
pinned: the governor sees the memory, and it makes the query wait or proceed
past the limit. It fails the query only where its pinned bytes would pass its
ceiling. A speculating transaction holds its writes in memory itself and
answers false.

The change-feed and live-event buffers are part of the transaction's write set.
Commit writes one encoded value per table, so they are counted through `insist`
and bounded by the backend's write buffer rather than spilled earlier.

`TempStore` is generic: a store holds a run's parts and reports an optional
quota. A host or backend supplies the implementation that suits where it runs.

## Membership

`x IN (SELECT ...)` and `x NOT IN (SELECT ...)` over a statement that runs once
is answered from an index of the statement's rows, kept from the first
evaluation and charged to the query's lease as bytes it cannot spill for as long
as it is kept. The physical plan decides this (`plan::Nested::membership`, with
`Strategy::Once`), and lowering emits `BlockInstr::Member` one-to-one.

The answer is the one `Value::contains` gives, which tests each row with
`Value::equal`. That is not `Value::cmp`: a string equals a regex that matches
it, and the order of a value holding a geometry, range or closure is not
defined. So the rows whose order agrees with their equality
(`Value::ord_agrees_with_equal`) are kept sorted and deduplicated by
`Value::cmp`, and a probe that agrees too is found by binary search. Every other
row is kept aside in arrival order and tested with `Value::equal`, as is every
probe that does not agree. Numbers agree: `1`, `1.0` and `1dec` are one value.
