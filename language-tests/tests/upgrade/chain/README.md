Cumulative upgrade tests: one datastore carried through every released version
in order, rather than an isolated pair of them.

A test here declares `[test.chain]` and is picked up only by
`surrealql-test upgrade --chain`. The runner walks the version list from front
to back against a single data directory. On the first version satisfying
`since` it runs `setup`; from there on, every version runs `mutate` and then the
test body, graded against the same `[[test.results]]` each time.

The walk starts at the first version satisfying the test's introducing release
and runs to the end of the list. Nothing before that version is ever spawned —
the data directory is not created until the loop begins — so a feature that
arrived in 3.1.0 simply has a shorter walk, not an empty store dragged through
3.0.x.

```
                    3.1.0 ─ 3.1.1 ─ 3.1.2 ─ … ─ 3.2.4 ─ HEAD
                      │        │        │          │       │
                   setup    mutate   mutate     mutate  mutate
                   verify   verify   verify     verify  verify
```

`[test] introduced` is where "add the feature at the version that introduced it"
lives, and `since` overrides it for the walk alone. DISKANN arrived in
3.1.0-beta.3, so `3.01.00_index_diskann.surql` sets
`introduced = ">=3.1.0-beta.3"`, and the walk begins at 3.1.0 — the first
release in `LANG_UPGRADE_CHAIN` that can parse the syntax — running from there
to the current checkout. The filename prefix mirrors that same field.

## Writing the body

The body runs once per version, so it has to be idempotent — put the writes in
`mutate`. Because `mutate` makes the data grow as the walk proceeds, a fixed
`value = "..."` expectation only holds at one hop. Matcher expressions get the
walk position for exactly this reason:

| Binding | Meaning |
|---|---|
| `$hop` | 1-based position among the versions that ran the body |
| `$hops` | how many versions will run it in total |
| `$version` | the resolved version currently running |
| `$chain` | every version being walked, in order |

```toml
[[test.results]]
match = '''count($result) == $hop'''
```

`setup` and `mutate` are not graded, and receive `$hop`, `$hops` and `$version`
as `LET` bindings prepended to the script, so they can write per-hop records.
`$chain` is a matcher variable only and is not bound in that prelude:

```surql
CREATE chain_pts SET point = [0.0, 0.0, 0.0, <float> $hop], wrote_at = $version;
```

## What this proves, and what it does not

It proves a structure created once stays correct while every release in turn
maintains it — each binary writes *through* something an older binary built.
Nothing else in the suite covers that.

It does **not** subsume the pairwise axes, and is the weaker test for the
question "can today's build read what an old release wrote". Every intermediate
binary launders the bytes on the way past: if a mid-chain release migrates or
rebuilds the structure, HEAD reads the migrated form and never sees the original
at all. `LANG_UPGRADE_SEEDS` in `Makefile.ci.toml` exists to cover that
directly, by jumping each seed release straight to the current checkout.

## Repairs do not belong in a walk

An upgrade that legitimately requires a manual step declares it with
`[[test.repair]]` — see `language-tests/CLAUDE.md` — but that is a pairwise
concern, and the runner refuses to schedule a chain test whose repair would fire
mid-walk.

The repair replaces the structure the walk is following, so every version after
it reads what the repair wrote rather than what the original writer left; the
walk would still be green while measuring nothing. Worse, `REBUILD INDEX` on an
index first written before 3.2.1 never returns on any later binary, so a walk
starting at 3.1.0 and crossing a 3.3.0 beta hangs to its timeout instead of
failing.

`LANG_UPGRADE_CHAIN` in `Makefile.ci.toml` is therefore its own list, routed
around such releases, and is not the same as the adjacent-pair chain.
