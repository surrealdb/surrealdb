# SurrealDB Driver Guide

## What is a database driver

A database driver, or client library, is a module for a programming language that is implemented to provide access to SurrealDB, and enables access to the wide range of functionality the database offers.

It’s focus is primarily on network protocol correctness, performance, access to distinct database features, error handling, and in due course, transaction handling and retriability.

Drivers are not designed to be a one-size-fits-all, as we are unable to make assumptions about how users will use the drivers. JDBC? Async? ORM? DSL? Due to the many different features and functionalities in each language, we are unable to provide all this functionality in a single library.

We want users to have very clear expectations about how our software works. It’s very important for us that when users move between languages or compare implementations, that the SurrealDB integration is as familiar as possible across all languages.

## Client library architecture

We would recommend following the API of the [Rust driver](https://github.com/surrealdb/surrealdb/tree/main/lib), as the Rust driver is fully utilising our capabilities and is the de-facto reference implementation. In the future it will also be the underlying implementation as we begin to share a common API (either via foreign function interfaces or WASM), with native language specific bindings.

Client libraries connect to SurrealDB using either REST, a text-based WebSocket protocol, or a binary-based WebSocket protocol. Each of the protocols aims to support as many of the SurrealDB features as possible, ensuring that similar functionality, and similar performance are supported regardless of the protocol being used.

Beyond baseline protocol support, error handling is also a key feature. This is tied with both custom SurrealQL protocol status codes included in the response itself, or with HTTP status codes in some cases.

There isn't any specific configuration per driver. We may introduce configuration options in due course, and we will update this guide if we change those configurations.

## Frequently Asked Questions

### My code will be useful for others

We absolutely agree! We intend to have a uniform, long-term approach with regards to support for our drivers. We appreciate community contributions - and we suggest that any changes which do not fit in to the core language drivers themselves, to be held and maintained in community repositories instead, and added to the [awesome-surreal](https://github.com/surrealdb/awesome-surreal) repository.

### We need ORM support

We absolutely agree! We don’t currently have ORM support in the drivers themselves, and aren’t likely to implement this, as there are many ORM libraries and integrations with which to integrate. We are, however, very excited to discover what the community creates. We encourage both newcomers and existing members to explore various projects that address their needs - these can be found in the [awesome-surreal](https://github.com/surrealdb/awesome-surreal) repository. To add more projects to the list, simply open a pull-request!

### Do you support language X?

We are currently focusing on building the core features of SurrealDB, whilst adding core drivers for some of the more popular language integrations. In the meantime, we warmly welcome and wholeheartedly support any contributions you would like to make in developing a driver for a specific language. Your efforts will be greatly valued and recognized, as we showcase these projects on our [awesome-surreal](https://github.com/surrealdb/awesome-surreal) repository.

### What about JDBC drivers and language-specific interface conventions?

We aren’t following that immediately, but we see the future where implementations such as JDBC specification would use the java-driver under the hood and be a wrapping. If you would like to write such an implementation for a language standard then that would be very exciting and people in the community would certainly use this.
