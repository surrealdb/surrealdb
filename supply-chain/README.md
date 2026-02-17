# Supply Chain Security

## Goal

Our main goal with supply chain security is to mitigate the impact of attackers introducing malicious code into third-party dependencies that SurrealDB relies on. At this stage, our aim is to introduce a basic mechanism by which dependency source and access is at least considered as part of the CI process, to reduce the attack surface of SurrealDB by limiting the number of dependencies completely exposed to supply chain attacks and to raise the bar for the effort needed to perform a successful supply chain attack against many of the current SurrealDB dependencies.

## Mechanism

Currently, supply chain security is implemented through a basic configuration of [`cargo-vet`](https://mozilla.github.io/cargo-vet/index.html) for the main SurrealDB repository. This tool is executed as part of the CI process. Ownership of the configuration files for this tool is assigned to **@surrealdb/security** group in the [`.github/CODEOWNERS`](https://github.com/surrealdb/surrealdb/blob/main/.github/CODEOWNERS) file.

### Security Compromises

At this stage, the following compromises are made due to lack of dedicated resources to audit dependencies:
- Dependencies published by SurrealDB employees are trusted by default when they are the only publisher.
- Dependencies audited directly (i.e. not transitively) by [some trusted organizations](https://raw.githubusercontent.com/bholley/cargo-vet/main/registry.toml) are trusted by default.
- Any dependencies that have not yet been audited are exempt from the vetting process.

In this implementation, it is important to note that `cargo-vet` is only used as an informational tool and that no significant security review will be performed by SurrealDB for third-party dependencies. The `cargo-vet` tool will be used to collect information from third-party audits as well as inventory which dependencies are published by trusted developers.

### Process

The following is a simplified lightweight process to support contributors in passing dependency checking.

Using the dependency tools locally requires installing the following software:

```bash
cargo install --locked cargo-deny
cargo install --locked cargo-vet
```

The following process can be followed whenever the dependency checking action fails:

- If the action fails due to `cargo-deny`:
  - Identify the affected dependency.
  - In a separate branch, run `cargo update <PACKAGE>`.
    - If there is no fix or an update is not possible:
      - Add an [exception to the `deny.toml`](https://github.com/surrealdb/surrealdb/blob/main/deny.toml#L64) file.
      - Add a comment to the exception with its rationale and the conditions for it to be removed.
  - Request the changes on a separate PR. Paste the vulnerability details provided by `cargo-deny`.
  - The PR containing the dependency update will be approved by **@surrealdb/security**.
  - Rebase your original branch so that the dependency is updated.
- If the action fails due to `cargo-vet`:
  - This means that the dependency has not yet been trusted, audited nor exempted.
  - If this is a new dependency, think about whether or not it needs to be introduced to SurrealDB.
  - If the dependency should be introduced:
    - If published by [a SurrealDB employee](https://github.com/orgs/surrealdb/people), it can be trusted as `safe-to-deploy`.
      - Ensure that all publishers of the dependency are SurrealDB employees.
      - `cargo vet trust <PACKAGE>`
    - Otherwise, it can be (for now) exempted from the vetting process.
      - `cargo vet add-exemption <PACKAGE>`
  - Afterwards, prune the list of audits to remove outdated entries.
  	- `cargo vet prune`
  - The changes will be approved by **@surrealdb/security**.

### Workspace crates published to crates.io

All workspace crates (e.g. `surrealdb`, `surrealdb-core`, `surrealdb-server`, `surrealism`, `surrealml-core`, etc.) are part of this repo; some are also published to crates.io. We set `audit-as-crates-io = false` for every workspace crate so they are treated as trusted first-party code regardless of version. That avoids maintaining per-version exemptions or audits when bumping. Downstream consumers of published crates use their own `cargo-vet` configuration.
