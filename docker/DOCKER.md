<br>

<p align="center">
    <img width="300" src="https://github.com/surrealdb/surrealdb/blob/main/img/icon.png?raw=true" alt="SurrealDB Icon">
</p>

<br>

<p align="center">
    <img width="300" src="https://github.com/surrealdb/surrealdb/blob/main/img/black/logo.svg?raw=true" alt="SurrealDB Logo">
</p>

<h3 align="center">
    <img src="https://github.com/surrealdb/surrealdb/blob/main/img/black/text.svg?raw=true" height="15" alt="SurrealDB">
    is the ultimate cloud <br> database for tomorrow's applications
</h3>

<h3 align="center">Develop easier. &nbsp; Build faster. &nbsp; Scale quicker.</h3>

<br>

<p align="center">
    <a href="https://github.com/surrealdb/surrealdb"><img src="https://img.shields.io/github/v/release/surrealdb/surrealdb?color=%23ff00a0&include_prereleases&label=version&sort=semver&style=flat-square"></a>
    &nbsp;
    <a href="https://github.com/surrealdb/surrealdb"><img src="https://img.shields.io/badge/built_with-Rust-dca282.svg?style=flat-square"></a>
    &nbsp;
	<a href="https://github.com/surrealdb/surrealdb/actions"><img src="https://img.shields.io/github/actions/status/surrealdb/surrealdb/ci.yml?style=flat-square&branch=main"></a>
    &nbsp;
    <a href="https://status.surrealdb.com"><img src="https://img.shields.io/uptimerobot/ratio/7/m784409192-e472ca350bb615372ededed7?label=cloud%20uptime&style=flat-square"></a>
    &nbsp;
    <a href="https://hub.docker.com/repository/docker/surrealdb/surrealdb"><img src="https://img.shields.io/docker/pulls/surrealdb/surrealdb?style=flat-square"></a>
    &nbsp;
    <a href="https://github.com/surrealdb/license"><img src="https://img.shields.io/badge/license-BSL_1.1-00bfff.svg?style=flat-square"></a>
</p>

<p align="center">
	<a href="https://surrealdb.com/discord"><img src="https://img.shields.io/discord/902568124350599239?label=discord&style=flat-square&color=5a66f6"></a>
	&nbsp;
    <a href="https://twitter.com/surrealdb"><img src="https://img.shields.io/badge/twitter-follow_us-1d9bf0.svg?style=flat-square"></a>
    &nbsp;
    <a href="https://dev.to/surrealdb"><img src="https://img.shields.io/badge/dev-join_us-86f7b7.svg?style=flat-square"></a>
    &nbsp;
    <a href="https://www.linkedin.com/company/surrealdb/"><img src="https://img.shields.io/badge/linkedin-connect_with_us-0a66c2.svg?style=flat-square"></a>
</p>

<p align="center">
	<a href="https://surrealdb.com/blog"><img height="25" src="https://github.com/surrealdb/surrealdb/blob/main/img/social/blog.svg?raw=true" alt="Blog"></a>
	&nbsp;
	<a href="https://github.com/surrealdb/surrealdb"><img height="25" src="https://github.com/surrealdb/surrealdb/blob/main/img/social/github.svg?raw=true" alt="Github	"></a>
	&nbsp;
    <a href="https://www.linkedin.com/company/surrealdb/"><img height="25" src="https://github.com/surrealdb/surrealdb/blob/main/img/social/linkedin.svg?raw=true" alt="LinkedIn"></a>
    &nbsp;
    <a href="https://twitter.com/surrealdb"><img height="25" src="https://github.com/surrealdb/surrealdb/blob/main/img/social/twitter.svg?raw=true" alt="Twitter"></a>
    &nbsp;
    <a href="https://www.youtube.com/channel/UCjf2teVEuYVvvVC-gFZNq6w"><img height="25" src="https://github.com/surrealdb/surrealdb/blob/main/img/social/youtube.svg?raw=true" alt="Youtube"></a>
    &nbsp;
    <a href="https://dev.to/surrealdb"><img height="25" src="https://github.com/surrealdb/surrealdb/blob/main/img/social/dev.svg?raw=true" alt="Dev"></a>
    &nbsp;
    <a href="https://surrealdb.com/discord"><img height="25" src="https://github.com/surrealdb/surrealdb/blob/main/img/social/discord.svg?raw=true" alt="Discord"></a>
    &nbsp;
    <a href="https://stackoverflow.com/questions/tagged/surrealdb"><img height="25" src="https://github.com/surrealdb/surrealdb/blob/main/img/social/stack-overflow.svg?raw=true" alt="StackOverflow"></a>
</p>

<br>

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/whatissurreal.svg?raw=true">&nbsp;&nbsp;What is SurrealDB?</h2>

SurrealDB is an end-to-end cloud native database for web, mobile, serverless, jamstack, backend, and traditional applications. SurrealDB reduces the development time of modern applications by simplifying your database and API stack, removing the need for most server-side components, allowing you to build secure, performant apps quicker and cheaper. SurrealDB acts as both a database and a modern, realtime, collaborative API backend layer. SurrealDB can run as a single server or in a highly-available, highly-scalable distributed mode - with support for SQL querying from client devices, GraphQL, ACID transactions, WebSocket connections, structured and unstructured data, graph querying, full-text indexing, geospatial querying, and row-by-row permissions-based access.

View the [features](https://surrealdb.com/features), the latest [releases](https://surrealdb.com/releases), the product [roadmap](https://surrealdb.com/roadmap), and [documentation](https://surrealdb.com/docs).

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/documentation.svg?raw=true">&nbsp;&nbsp;Documentation</h2>

For guidance on installation, development, deployment, and administration, see our [documentation](https://surrealdb.com/docs).

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/gettingstarted.svg?raw=true">&nbsp;&nbsp;Run using Docker</h2>

Docker can be used to manage and run SurrealDB database instances without the need to install any command-line tools. The SurrealDB docker container contains the full command-line tools for importing and exporting data from a running server, or for running a server itself.

```bash
docker run --rm --pull always --name surrealdb -p 8000:8000 surrealdb/surrealdb:latest start
```

For just getting started with a development server running in memory, you can pass the container a basic initialization to set the user and password as root and enable logging.

```bash
docker run --rm --pull always --name surrealdb -p 8000:8000 surrealdb/surrealdb:latest start --log trace --user root --pass root memory
``` 

Access to the surrealdb CLI with:

```bash
docker exec -it <container_name> /surreal sql -e http://localhost:8000 -u root -p root --ns test --db test --pretty
```

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/gettingstarted.svg?raw=true">&nbsp;&nbsp;Run using Docker Compose</h2>


The Docker image can be used with the `docker-compose` tool.
You can execute the following command to view the help of the `start` command to know the supported _environment variables_:

```shell
docker run --rm surrealdb/surrealdb:latest start --help
```

Here is an example of `docker-compose.yml` file.

```yaml
version: '3'

services:
  surrealdb:
    env_file:
      - .env
    command: start 
    image: surrealdb/surrealdb:latest
    ports:
      - 8000:8000
```

Here is an example of `.env` file.

```dotenv
SURREAL_LOG=trace
SURREAL_USER=root
SURREAL_PASS=root
SURREAL_CAPS_ALLOW_ALL=true
```


<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/community.svg?raw=true">&nbsp;&nbsp;Community</h2>

Join our growing community around the world, for help, ideas, and discussions regarding SurrealDB.

- View our official [Blog](https://surrealdb.com/blog)
- Chat live with us on [Discord](https://surrealdb.com/discord)
- Follow us on [Twitter](https://twitter.com/surrealdb)
- Connect with us on [LinkedIn](https://www.linkedin.com/company/surrealdb/)
- Visit us on [YouTube](https://www.youtube.com/channel/UCjf2teVEuYVvvVC-gFZNq6w)
- Join our [Dev community](https://dev.to/surrealdb)
- Questions tagged #surrealdb on [Stack Overflow](https://stackoverflow.com/questions/tagged/surrealdb)

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/contributing.svg?raw=true">&nbsp;&nbsp;Contributing</h2>

We would &nbsp;<img width="15" src="https://github.com/surrealdb/surrealdb/blob/main/img/love.svg?raw=true">&nbsp; for you to get involved with SurrealDB development! If you wish to help, you can learn more about how you can contribute to this project in the [contribution guide](CONTRIBUTING.md).

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/security.svg?raw=true">&nbsp;&nbsp;Security</h2>

For security issues, view our [vulnerability policy](https://github.com/surrealdb/surrealdb/security/policy), view our [security policy](https://surrealdb.com/legal/security), and kindly email us at [security@surrealdb.com](mailto:security@surrealdb.com) instead of posting a public issue on GitHub.

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/license.svg?raw=true">&nbsp;&nbsp;License</h2>

Source code for SurrealDB is variously licensed under a number of different licenses. A copy of each license can be found in [each repository](https://github.com/surrealdb).

- Libraries and SDKs, each located in its own distinct repository, are released under either the [Apache License 2.0](https://github.com/surrealdb/license/blob/main/APL.txt) or [MIT License](https://github.com/surrealdb/license/blob/main/MIT.txt).
- Certain core database components, each located in its own distinct repository, are released under the [Apache License 2.0](https://github.com/surrealdb/license/blob/main/APL.txt).
- Core database code for SurrealDB, located in [this repository](https://github.com/surrealdb/surrealdb), is released under the [Business Source License 1.1](/LICENSE).

For more information, see the [licensing information](https://github.com/surrealdb/license).
