# SurrealDB Driver Guide

## What is a database driver

A database driver is a module for a programming language that is implemented to provide access to a specific database (SurrealDB) and give access to the wide range of functionality the database offers.

It’s focus is primarily on network protocol correctness, performance, access to distinct database features, error handling, and potentially transaction handling and retriability (this functionality may be disputable).

Drivers are not a one-stop-shop for convenience, because we cannot make assumptions about how users will use the drivers.
JDBC?
Async?
ORM?
DSL?
We cannot make these assumptions and we cannot provide all this functionality in a single library.

We would also like users to have very clear expectations about how our software works.
It’s very important for us that when users move between languages or compare implementations, that the SurrealDB part of their implementations is familiar across all languages.

## General Architecture of a Driver

We would recommend following the API of the Rust driver.
This is because the Rust driver is fully utilising our capabilities and is the de-facto reference implementation.
In the future it will also be the literal implementation as we begin to share a common binary (either via foreign function interfaces or WASM), with language specific bindings.

We support REST, JSON WebSocket and MsgPack Binary WebSocket implementations of our protocols.
There is no difference between them, except that the WebSocket implementations allow live messaging - a feature necessary for some of SurrealDB query features.

Beyond baseline protocol support, error handling is also a key feature.
This is tied with both SurrealQL protocol status codes included in the response JSON but also HTTP codes in more extreme cases.

We don’t do any configuration per driver, but it may make sense to have timeout durations as bare-minimum.
Other configuration options may follow and we will update this guide if we change those configurations.

## Frequently Asked Questions

### My code will be useful for others

We absolutely agree!
We appreciate community contributions however we would like them to be in community repositories instead.
These would also be maintained by the community.
The reason for this is that we would like to have a uniform, long-term approach.

### We need ORM support

Fully agree!
We don’t currently do that in the drivers and aren’t likely to do that in the drivers projects themselves.
We're excited to discover what the community creates.
We encourage both newcomers and existing members to explore various projects that address their needs - these can be found in the awesome-surreal project.
To add more projects to the list, simply open a PR!

### Where is language X?

We are currently focusing on building the core features of SurrealDB.
In the meantime, we warmly welcome and wholeheartedly support any contributions you would like to make in developing a driver for a specific language.
Your efforts will be greatly valued and recognized, as we showcase these projects on our awesome-surreal platform.

### What about JDBC drivers and language-specific interface conventions?

We aren’t following that immediately, but we see the future where implementations such as JDBC specification would use the java-driver under the hood and be a wrapping.
If you would like to write such an implementation for a language standard then that would be very exciting and people in the community would certainly use this.
