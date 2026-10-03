# surrealdb-pgsql

> **Warning: Unstable! Internal API**
> This crate is an internal implementation detail of SurrealDB and is not intended for public use outside the SurrealDB workspace.

> **Note: Dialect Profiles**
> PostgreSQL 17, 18, and 19 grammar parsing as well as Standard SQL:2016 and SQL:2023 dialect profiles are implemented within this crate.

> **Important: SurrealQL Disclaimer**
> This crate implements the PostgreSQL and Standard SQL language front end for SurrealDB. It is entirely unrelated to `surrealdb-sql`, which provides the SurrealQL parser and AST.

## Overview

`surrealdb-pgsql` provides a clean-room, zero-dependency (outside locked utilities) parser, AST, and faithful printer for the PostgreSQL dialect and ANSI/ISO Standard SQL.
