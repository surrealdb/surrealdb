# surrealdb-mysql

> **Warning: Unstable! Internal API**
> This crate is an internal implementation detail of SurrealDB and is not intended for public use outside the SurrealDB workspace.

> **Note: Dialect Profiles**
> MySQL 8.0 and 8.4 grammar parsing dialect profiles are implemented within this crate.

> **Important: SurrealQL Disclaimer**
> This crate implements the MySQL language front end for SurrealDB. It is entirely unrelated to `surrealdb-sql`, which provides the SurrealQL parser and AST.

## Overview

`surrealdb-mysql` provides a clean-room, zero-dependency (outside locked utilities) parser, AST, and faithful printer for the MySQL dialect.
