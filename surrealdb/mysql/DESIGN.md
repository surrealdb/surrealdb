# Architecture and Design: `surrealdb-mysql`

This document details the architectural boundaries, design decisions, and parser pipeline for the MySQL language front end.

## Parser Architecture

1. **Pratt-Parser Hybrid Core**:
   - Pratt precedence climbing for binary and unary expressions.
   - Recursive descent for statements and subqueries, with depth budgets bounding the native stack.

2. **Zero-Allocation Scanner**:
   - Zero-allocation linear-time lexer recognizing MySQL identifier rules (backtick quoting, double-quote in ANSI mode).
   - Strict standard-conforming handling of character escapes and string literals.
