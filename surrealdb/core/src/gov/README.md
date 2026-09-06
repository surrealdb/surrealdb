# Resource Governance (`gov`)

Schema-defined rate limiting: `RATELIMIT` clauses on tables and fields,
enforced by KV-backed token buckets shared across every node of a cluster.

```surql
DEFINE TABLE post RATELIMIT
    FOR SELECT WHERE $auth != NONE BY $auth.id LIMIT 1000 PER 1h MAX 2000,
    FOR CREATE BY $session.ip LIMIT 10 PER 1m;

DEFINE FIELD comment ON post TYPE string
    RATELIMIT FOR CREATE, UPDATE BY $auth.id LIMIT 20 PER 1h;
```

Grammar: `FOR <actions> [WHERE <condition>] BY <key expr> LIMIT <n> PER
<duration> [MAX <burst>]`, repeatable with commas.

## Semantic invariants

These are the rules the implementation guarantees. Tests pin each one;
changing any of them is a breaking behavioural change.

1. **Continuous refill.** `LIMIT n PER d` is a refill *rate* (n/d tokens per
   unit time), not a fixed window that resets on a boundary. `MAX m` is the
   bucket capacity (burst); it defaults to `n`.
2. **Key `NONE` ⇒ deny.** A `BY` expression that evaluates to `NONE`/`NULL`
   denies the statement (`RateLimitKeyUnavailable`). A silent skip would
   let any key-less code path (logged-out session, IP-less transport) void
   the policy; a shared "NONE bucket" would fuse unrelated principals. The
   intended idiom for nullable keys is a guard:
   `... WHERE $auth != NONE BY $auth.id ..., WHERE $auth = NONE BY $session.ip ...`
3. **Condition falsy ⇒ exempt.** `WHERE` is the exemption mechanism
   (admins, service accounts). It is evaluated once per statement in
   session context — not per row, unlike permission `WHERE` clauses.
4. **Expression error ⇒ deny.** Errors in `WHERE` or `BY` evaluation fail
   the statement. Errors are never exemptions.
5. **Policies AND-compose.** Every policy matching a statement must admit
   it. Charges settle atomically: if one policy denies, none consume.
6. **SELECT limits meter records *delivered*, attributed per table.** A
   record counts against its own table's policy exactly when its data is
   incorporated into the statement's response, after filtering and
   permission checks. `SELECT ->friend->user.* FROM user:drew` charges
   `user` for the source row and each returned friend; the traversed
   `friend` edges are free — unless edge data is delivered
   (`->friend.{kind}`), in which case `friend` is charged. Navigation
   hops, id-only references, filtered rows, permission-hidden records,
   and aggregate-collapsed rows are not deliveries. Both execution
   engines meter identically. (Read amplification and compute burn —
   work performed without delivering data — are cost-budget concerns,
   governed separately.) Create/update/delete limits charge per affected
   record.
7. **Settlement is atomic and pre-response; responses are never
   partial.** All applicable policies — every delivering table's SELECT
   policies, write policies, field policies — settle as one
   all-or-nothing batch in a dedicated transaction before the response
   is returned. If any policy would be exceeded, nothing is charged, the
   whole statement fails with the rate-limit error, and no data is
   delivered: mixed responses (some rows carrying a limited table's data
   while others silently omit it) cannot occur.
8. **Failed statements charge nothing.** A statement that errors, times
   out, or is cancelled delivers no data, so its pending charges are
   discarded. Bounding the resources such a statement consumed is the
   cost-budget concern above, not a data-egress one.
9. **Enforcement covers every execution path.** Bare statements, explicit
   `BEGIN`/`COMMIT` blocks, and the streaming executor all settle charges;
   wrapping a query in a transaction block does not bypass limits.
10. **Denial is a typed, retryable error.** `RateLimitExceeded` carries the
   policy locus and a `retry_after` estimate (absent when the request
   exceeds bucket capacity outright and can never be admitted). It maps to
   the public `QueryError::RateLimited` (wire code −32010) and HTTP 429
   with a `Retry-After` header.

## Persistence formats (frozen)

Two byte formats in this module outlive any single process and must never
change without a versioning plan:

- **Bucket key derivation** (`BucketKeyHasher`): BLAKE3 over the policy
  identity (ns, db, table/field, action, policy index) and the evaluated
  key, truncated to 128 bits. Collision-resistant because the hashed
  material includes user-controlled values and bucket state is shared —
  a constructible collision would let one principal drain another's
  bucket. Integer writes are little-endian regardless of host. A unit
  test pins a known vector.
- **Bucket value encoding**: version byte (`0x01`) + tokens (f64) +
  last-refill ms + expiry ms, big-endian. Unknown versions decode as
  absent (bucket re-initialises); the cleanup sweep removes them.

## Architecture

- `RateLimiter` — per-datastore: the in-memory plan cache and the charge /
  cleanup engine. Plan cache entries are verified against a
  `PlanIdentity` that includes the table's `cache_tables_ts` stamp, so a
  schema change on *any* node invalidates them naturally (no cross-node
  invalidation protocol, and 64-bit cache-key collisions cannot apply the
  wrong table's policies).
- `ChargeSession` — mints the dedicated settlement transactions; carried
  by value into contexts that have no `Datastore` handle.
- `DeliveryMeter` — per-statement accumulator installed on the `Context`;
  the output paths of both execution engines bump per-table delivered
  counts (a synchronous in-memory increment — no bucket work mid-flight).
  SELECT policies are resolved at settlement for whichever tables
  actually delivered records, so traversed tables no FROM clause names
  are covered, and the whole batch settles atomically before the
  response is returned.
- Field-level charges accumulate per statement (`Context::ratelimit_charges`)
  during document processing and settle with the table-level charges.
- Optimistic conflicts on hot buckets are retried (`CHARGE_ATTEMPTS`)
  before surfacing, so concurrent legitimate traffic does not produce
  spurious denials; the batch is all-or-nothing either way.
- Expired bucket state is swept incrementally (rotating cursor, bounded
  batch) in best-effort transactions piggybacked on every 256th charge.

## Scope

This subsystem governs *admitted, authenticated* work: business-logic
fairness and abuse control keyed on session/auth identity. Two adjacent
problems are explicitly out of scope:

- **Pre-auth flood protection** (unauthenticated connection/request
  floods, IP rotation) must run before parsing and authentication, on
  node-local state, and belongs to the transport layer (reverse proxy,
  WAF, or server-level connection limits).
- **Resource consumption without delivery** — read amplification
  (scans and traversals that return little or nothing) and pure compute
  burn (nested `FOR` loops, expensive expressions). Delivered-records
  limits deliberately do not see this work: they protect the *data*.
  Statement `TIMEOUT` and the transaction timeout bound it per statement
  today; budgeting it per-principal is the planned cost source below — a
  new meter feeding the same buckets, not new infrastructure.

## Future directions (non-breaking)

- **Quota leasing**: nodes lease token chunks from hot buckets and enforce
  locally, reconciling asynchronously — same bucket state, cheaper hot
  keys.
- **Cost units**: grammar space is reserved for an explicit unit/COST
  clause (e.g. rows, bytes, evaluation steps) so `LIMIT` can meter
  resources other than admissions without a breaking syntax change. The
  compute cost source (loop iterations / expression evaluations, already
  counted implicitly by the `ctx.done` checks) closes the compute-burn
  gap noted under Scope.
