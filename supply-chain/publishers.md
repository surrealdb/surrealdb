# Trusted crates.io publishers

Reference for numeric `**user-id**` values in `[audits.toml](./audits.toml)` `**[[trusted.*]]**` blocks. This file is for humans only; `**[cargo vet](https://mozilla.github.io/cargo-vet/)` does not read it** - trust is still expressed only in `audits.toml`.

**Meaning of “Third-party publisher trust”:** we recorded `**safe-to-deploy` publisher trust** with `[cargo vet](https://mozilla.github.io/cargo-vet/)` for crates.io releases in the `start`/`end` window on the matching `[[trusted.<crate>]]` entry, per `[supply-chain/README.md](./README.md)` - not a manual audit of that account.

Some rows are filled from comments in `audits.toml`; for IDs that appear **without** a trailing comment (for example after `cargo vet fmt`), known identities are recorded here manually.

| crates.io user-id | Publisher                        | Reason                                                                                     |
| ----------------- | -------------------------------- | ------------------------------------------------------------------------------------------ |
| 1                 | Alex Crichton (alexcrichton)     | Third-party publisher trust                                                                |
| 5                 | Steven Fackler (sfackler)        | Third-party publisher trust                                                                |
| 10                | Carl Lerche (carllerche)         | Third-party publisher trust                                                                |
| 189               | Andrew Gallant (BurntSushi)      | Third-party publisher trust                                                                |
| 267               | Tony Arcieri (tarcieri)          | Third-party publisher trust                                                                |
| 356               | bluss (bluss)                    | Third-party publisher trust                                                                |
| 359               | Sean McArthur (seanmonstar)      | Third-party publisher trust                                                                |
| 498               | Ruud van Asseldonk (ruuda)       | Third-party publisher trust                                                                |
| 539               | Josh Stone (cuviper)             | Third-party publisher trust                                                                |
| 2751              | Joe Birr-Pixton (ctz)            | Third-party publisher trust                                                                |
| 2915              | Amanieu d'Antras (Amanieu)       | Third-party publisher trust                                                                |
| 3204              | Ashley Mannix (KodrAus)          | Third-party publisher trust                                                                |
| 3618              | David Tolnay (dtolnay)           | Third-party publisher trust                                                                |
| 3987              | Rushmore Mushambi (rushmorem)    | SurrealDB team member; first-party / employee publisher (`[README](./README.md)` process). |
| 4556              | Dirkjan Ochtman (djc)            | Third-party publisher trust                                                                |
| 6741              | Alice Ryhl (Darksonn)            | Third-party publisher trust                                                                |
| 6743              | Ed Page (epage)                  | Third-party publisher trust                                                                |
| 6825              | Dan Gohman (sunfishcode)         | Third-party publisher trust                                                                |
| 17316             | csmoe (csmoe)                    | Third-party publisher trust                                                                |
| 30606             | Michele d'Amico (la10736)        | Third-party publisher trust                                                                |
| 33035             | Taiki Endo (taiki-e)             | Third-party publisher trust                                                                |
| 51017             | Yuki Okushi (JohnTitor)          | Third-party publisher trust                                                                |
| 55123             | Rust project (rust-lang-owner)   | Rust project crates.io organisation account (not an individual maintainer).                |
| 64539             | Kenny Kerr (kennykerr)           | Third-party publisher trust                                                                |
| 66052             | Sunli (sunli829)                 | Third-party publisher trust                                                                |
| 79445             | Mees Delzenne (DelSkayn)         | SurrealDB team member; first-party / employee publisher (`[README](./README.md)` process). |
| 145457            | Tobie Morgan Hitchcock (tobiemh) | SurrealDB team member; first-party / employee publisher (`[README](./README.md)` process). |
| 172786            | Hayden Stainsby (hds)            | Third-party publisher trust                                                                |
| 178443            | Derek Bailey (dbaileychess)      | Third-party publisher trust                                                                |
| 217605            | Yusuke Kuoka (mumoshu)           | SurrealDB team member; first-party / employee publisher (`[README](./README.md)` process). |
| 256011            | Folkert de Vries (folkertdev)    | Third-party publisher trust                                                                |


