# Memory: a shared pool, and strategies instead of failures

**Status:** design. Sorts under `TEMPFILES` spill (`vm::spill`); nothing else
below is built yet.

A query's memory is not a fixed allowance. The node holds one pool, every
running query leases from it, and what a query may hold depends on what else is
running. A query that cannot lease more changes how it computes — it spills,
takes a cheaper strategy, or waits — and does not fail for want of memory.

## The pool

- **One pool per node**, in bytes, shared by every query the node runs, like the
  region workers' budget.
- **Leases.** A query's sinks — sort buffers, group maps, split rows, the images
  a write holds — lease memory in chunks as they grow and give it back as they
  drain. Each query carries an accountant that its sinks charge.
- **Fair and work-conserving.** A query's share is about the pool divided among
  the queries running; while the node is otherwise idle a query may borrow past
  its share. When other queries arrive, a query over its share is asked to give
  memory back at its next checkpoint: a sink boundary, a batch flush.

## When a lease is refused

In order, never failing the query:

1. **Spill** what can be spilled: a sort's rows as sorted runs merged later; a
   group map partitioned by the hash of its key, each partition spilled and
   aggregated on its own; the images a write holds.
2. **Take a cheaper strategy** for what is left: several passes over the input
   instead of one sort that holds it, smaller region batches, fewer workers.
3. **Wait** for memory to be given back, when what is held cannot be spilled —
   one very large value. The oldest waiting query is served first, so waiters do
   not starve one another.

Spill goes to the temporary directory when one is configured; otherwise to a
scratch keyspace in the datastore itself, which every persistent backend has;
on Cloudflare, to a Durable Object's storage or R2 (see the fan-out design). A
statement's `TEMPFILES` becomes a hint, not the condition for spilling.

## Arenas

A query's intermediates are owned values on the process heap today, so what a
sink holds is an estimate. The row format this design is converging on — rows
as borrowed bytes and views over them, intermediates bump-allocated in an arena
the query owns and resets per batch — makes the account exact and allocation
nearly free. Arenas lease their chunks from the same pool.

## Order of work

1. The pool and each query's accountant, charged by the sinks with an estimated
   size per row.
2. Sorts spill when their query is over its share, whether or not the statement
   asked for `TEMPFILES`.
3. Group maps spill by partition.
4. The datastore's scratch keyspace as the spill target when there is no
   temporary directory.
5. Waiting for memory, oldest query first.
6. Arenas, with the row format.
