# Supply Chain Security

## Goal

Our main goal with supply chain security is to mitigate the impact of attackers introducing malicious code into third-party dependencies that SurrealDB relies on. At this stage, our aim is to introduce a basic mechanism by which dependency source and access is at least considered as part of the CI process, to reduce the attack surface of SurrealDB by limiting the number of dependencies completely exposed to supply chain attacks and to raise the bar for the effort needed to perform a successful supply chain attack against many of the current SurrealDB dependencies.

## Mechanism

Currently, supply chain security is implemented through a basic configuration of [`cargo-vet`](https://mozilla.github.io/cargo-vet/index.html) and [`cargo-acl` (i.e. Cackle)](https://github.com/cackle-rs/cackle) for the main SurrealDB repository. These tools are executed as part of the CI process. Ownership of the configuration files for these tools is assigned to **@surrealdb/security** group in the [`.github/CODEOWNERS`](https://github.com/surrealdb/surrealdb/blob/main/.github/CODEOWNERS) file.

### Security Compromises

At this stage, the following compromises are made due to lack of dedicated resources to audit dependencies:
- Dependencies published by SurrealDB employees are trusted by default when they are the only publisher.
- Dependencies audited directly (i.e. not transitively) by [some trusted organizations](https://raw.githubusercontent.com/bholley/cargo-vet/main/registry.toml) are trusted by default.
- Any dependencies that have not yet been audited are exempt from the vetting process.

In this implementation, it is important to note that `cargo-vet` is only used as an informational tool and that no significant security review will be performed by SurrealDB for third-party dependencies. The `cargo-vet` tool will be used to collect information from third-party audits that can be used to inform the decision of allowing or denying newly required access through `cargo-acl` as well as inventory which dependencies are published by trusted developers.

Using `cargo-acl`, the minimum required permissions for each existing dependency (from a total of 594 dependencies, only 272 required no special permissions) at the time of implementation have been granted without any significant review. This limits the exposure to supply chain attacks that require additional access (e.g. a dependency only granted `net` would not be able to suddenly read files and exfiltrate them over the network), but would still allow for dependencies that have been granted some level of access (specially `unsafe`, `fs` and procedural macros) to leverage this access to conduct significant supply chain attacks. Ideally, dependencies granted higher level of access should be reviewed in the future by leveraging `cargo-vet`. As [acknowledged by the Cackle project](https://github.com/cackle-rs/cackle/blob/main/SECURITY.md), we recognize that access limitations can be overcome by determined attackers.

### Process

The following is a simplified lightweight process to support contributors in passing dependency checking.

Using the dependency tools locally requires installing the following software:

```bash
cargo install --locked cargo-deny
cargo install --locked cargo-vet

# Linux
cargo install --locked cargo-acl
sudo apt install -y bubblewrap # Adapt as required

# Other Systems (Docker)
# You will need to build the following image at least once:
docker build -t surrealdb-local/builder --target builder -f docker/Dockerfile .
# Disable the sandboxing configuration in favor of Docker.
sed -i 's/kind = "Bubblewrap"/kind = "Disabled"/g' cackle.toml
# Run Cackle interactively inside the Docker image.
docker run --entrypoint /bin/bash -it --rm -v $(pwd):/app -w /app surrealdb-local/builder \
  -c "cargo install cargo-acl && cargo acl"
# Revert the sandboxing configuration before committing your changes.
sed -i 's/kind = "Disabled"/kind = "Bubblewrap"/g' cackle.toml
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
- If the action fails due to `cargo-acl`:
  - The newly required access (e.g. `unsafe`, `fs`, `net`...) should be understood by the author of the PR.
  - If an audit is present in [`supply-chain/audits.toml`](https://github.com/surrealdb/surrealdb/blob/main/supply-chain/audit.toml), you may review it to understand the required access.
  - If the newly required permissions are understood and accepted.
    - Locally run `cargo acl`. When the required access dialog appears, press `f`.
      - Alternatively, you can directly edit the [`cackle.toml`](https://github.com/surrealdb/surrealdb/blob/main/cackle.toml) file to add the necessary permissions.
    - Select the minimum access that you believe the dependency should be granted.
    - Commit and push the changes to the config files to your PR.
    - In your PR add a brief explanation of the granted access.
    - The changes will be approved by **@surrealdb/security**.
