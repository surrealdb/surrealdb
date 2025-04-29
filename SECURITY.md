# <img height="25" src="/img/security.svg">&nbsp;&nbsp;Open Source Security Policy

## Supported Versions

| Version    | Supported                                       |
| ---------- | ----------------------------------------------- |
| < 2.0      | -                                               |
| >= 2.0.0   | <img width="20" src="/img/tick.svg">            |

## Reporting a Vulnerability

We take the security of SurrealDB code, software, and infrastructure very seriously. If you believe you have found a
security vulnerability in SurrealDB, we encourage you to let us know right away. We will investigate all legitimate
reports and do our best to quickly fix the problem.

Please report any issues or vulnerabilities via [Github Security
Advisories](https://github.com/surrealdb/surrealdb/security/advisories) instead of posting a public issue in GitHub.
You can also send security communications to [security@surrealdb.com](mailto:security@surrealdb.com). Please include the
version identifier obtained by running `surreal version` on the command line and details on how the vulnerability
can be exploited.

### Do

- ✅ Privately disclose the details of any potential vulnerability to SurrealDB.
- ✅ Provide enough information to reproduce the vulnerability in your report.
- ✅ Ask permission from SurrealDB to run automated security tools against its infrastructure.

### Do Not

- ❌ Disclose the details of the vulnerability publicly or to third parties. 
- ❌ Exploit a vulnerability beyond what is strictly necessary to verify its existence.
- ❌ Run automated security tools against SurrealDB infrastructure without permission.

### Our Responsibility 

- Acknowledge your report within 3 business days of the date of communication.
- Verify the issue and keep you informed of the progress toward its resolution.
- Handle your report and any data you share with us with strict confidentiality.
- Abstain from legal action against you for any report made following this policy.
- Credit you in any relevant public security advisory, unless you desire otherwise.

## Security Advisories

SurrealDB strives to provide timely and clear communication regarding any security issues that may impact users of its
binaries, libraries and platforms using [Github Security
Advisories](https://docs.github.com/en/code-security/security-advisories/working-with-repository-security-advisories/creating-a-repository-security-advisory)
and other available communication channels.  Generally, vulnerabilities will be discussed and [resolved
privately](https://docs.github.com/en/code-security/security-advisories/working-with-repository-security-advisories/collaborating-in-a-temporary-private-fork-to-resolve-a-repository-security-vulnerability)
to minimize the risk of exploitation. Security advisories will generally be published once a SurrealDB version including
a fix for the relevant vulnerability is available. The goal of publishing security advisories is to notify users of the
risks involved with using a vulnerable version and to provide information for resolving the issue or implementing any
possible workarounds.

Vulnerabilities in third-party dependencies may only be independently published by SurrealDB when they affect a
SurrealDB binary or platform. In those cases, the original CVE identifier will be referenced. Vulnerabilities affecting
SurrealDB libraries will not be published again by SurrealDB when an advisory already exists for the original dependency
as security tooling (e.g. `cargo audit`, or `cargo deny check` or Dependabot) will already be able to track it up the 
dependency tree.

## Security Updates

As with any other update, security updates to SurrealDB are released following [Semantic Versioning (AKA
SemVer)](https://semver.org).

Urgent security patches will be released for the latest SurrealDB minor release (e.g. `2.999.0`) using a patch release
(e.g. `2.999.1`). We commit not to break API backward compatibility in patch releases to ensure that SurrealDB users
have no reservations that may cause delays when applying security patches.

Regular security updates can be released as part of a minor release (e.g.  `2.999.0` to `2.1000.0`). Minor releases
should not break backward compatibility either and we encourage updating whenever possible. However, there are a few
security-sensitive dependencies (e.g. `rustls` or `native_tls`) that are part of the public API of SurrealDB but are
still in an unstable version, meaning that they can break backward compatibility. We do not consider these types (e.g.
the [TLS enumeration](https://docs.rs/surrealdb/2.0.0/surrealdb/opt/enum.Tls.html)) part of the SurrealDB stable API and
as such their backward compatibility may be broken in a minor release. These breaking changes should be rare and will
always be clearly stated in the changelog. Even if not considered part of the stable API, these types of breaking
changes will only be included in major and minor releases; never in patch releases as stated in the previous paragraph.

## Security Automation

### Dependencies

Dependencies used by SurrealDB are [checked for known vulnerabilities in
CI](https://github.com/surrealdb/surrealdb/pull/3386) using `cargo deny check`. Developers are required to either update,
replace or acknowledge vulnerable dependencies found during the approval process of every pull request. Additionally,
SurrealDB makes use of Github's [Dependabot
alerts](https://docs.github.com/en/code-security/dependabot/dependabot-alerts/about-dependabot-alerts) to continuously
monitor its dependencies for security issues.

SurrealDB also [implements basic supply chain security practices](https://github.com/surrealdb/surrealdb/pull/3395)
using [`cargo-vet`](https://mozilla.github.io/cargo-vet/index.html) and [`cargo-acl` (i.e.
Cackle)](https://github.com/cackle-rs/cackle) to mitigate the impact of attackers introducing malicious code into
third-party dependencies. These tools are executed as part of the CI process to ensure that significant changes in
dependencies are considered. More details about these practices can be found [here](supply-chain/README.md).

### Fuzzing

SurrealDB is [integrated](https://github.com/google/oss-fuzz/tree/master/projects/surrealdb) with Google's
[OSS-Fuzz](https://google.github.io/oss-fuzz/) project. As part of this integration, both [the SurrealQL parser and
query executor](https://github.com/surrealdb/surrealdb/tree/main/lib/fuzz/fuzz_targets) are continuously fuzzed to
identify security and performance bugs in SurrealQL. We aim to resolve all [security-relevant
bugs](https://google.github.io/oss-fuzz/advanced-topics/bug-fixing-guidance#security-issues) before their disclosure
deadline. Other bugs (e.g. crashes or performance bugs) that may have some availability impact will be prioritized and
resolved as any other bug regardless of the disclosure deadline.
