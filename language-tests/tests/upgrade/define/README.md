
Upgrade tests testing the upgrading schema related structs.

Each test is a pair: `X_import.surql` runs on the *old* binary and writes the
definition, `X.surql` runs on the new one with `upgrade = true` and asserts what
`INFO` reports. A definition whose stored shape changes must have a pair here,
or nothing checks that a database written by the previous release still reads.

## Why there is no `live.surql`

`LIVE` is the one definition that cannot be covered this way, and the reason is
worth stating so the gap is not "fixed" with a test that cannot fail.

Subscriptions do not survive a restart at all. `Datastore::bootstrap` runs
`expire_nodes` then `remove_nodes`, which deletes both the `node::lq` and
`table::lq` rows of every archived node — and the upgrade harness restarts onto
a new node id, so by the time the new binary serves a query the subscriptions
the old one wrote are gone. An `INFO FOR TABLE` assertion would only ever
observe `lives: {}`.

Worse, it would observe that whether or not subscription decoding works.
`remove_nodes` decodes `NodeLiveQuery` for its ns/db/tb and then deletes the
subscription row by constructing its key; it never decodes
`StoredSubscriptionDefinition`. So a decode regression would leave such a test
green.

The case that does matter for subscriptions is a rolling cluster upgrade, where
a new node writes to a table whose subscriptions a still-live old node
registered. That is a cross-version *decode* of stored bytes, which is what the
frozen fixtures in `core/src/catalog/compat/` cover directly, at the byte level
and for every released format.
