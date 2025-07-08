<p align="center">
    <a href="https://surrealdb.com" target="_blank">
        <img width="100%" src="https://github.com/surrealdb/surrealdb/blob/main/img/black/hero.png?raw=true" alt="SurrealDB Hero">
    </a>
</p>

<p align="center">
    <a href="https://github.com/surrealdb/surrealdb"><img src="https://img.shields.io/github/v/release/surrealdb/surrealdb?color=%23ff00a0&include_prereleases&label=version&sort=semver&style=flat-square"></a>
    &nbsp;
    <a href="https://github.com/surrealdb/surrealdb"><img src="https://img.shields.io/badge/built_with-Rust-dca282.svg?style=flat-square"></a>
    &nbsp;
	<a href="https://github.com/surrealdb/surrealdb/actions"><img src="https://img.shields.io/github/actions/workflow/status/surrealdb/surrealdb/ci.yml?style=flat-square&branch=main"></a>
    &nbsp;
    <a href="https://status.surrealdb.com"><img src="https://img.shields.io/uptimerobot/ratio/7/m784409192-e472ca350bb615372ededed7?label=cloud%20uptime&style=flat-square"></a>
    &nbsp;
    <a href="https://hub.docker.com/repository/docker/surrealdb/surrealdb"><img src="https://img.shields.io/docker/pulls/surrealdb/surrealdb?style=flat-square"></a>
    &nbsp;
    <a href="https://github.com/surrealdb/license"><img src="https://img.shields.io/badge/license-BSL_1.1-00bfff.svg?style=flat-square"></a>
</p>

<p align="center">
	<a href="https://surrealdb.com/discord"><img src="https://img.shields.io/discord/902568124350599239?label=discord&style=flat-square&color=5a66f6" alt="Discord"></a>
	&nbsp;
    <a href="https://x.com/surrealdb"><img src="https://img.shields.io/badge/x-follow_us-222222.svg?style=flat-square" alt="X"></a>
    &nbsp;
    <a href="https://dev.to/surrealdb"><img src="https://img.shields.io/badge/dev-join_us-86f7b7.svg?style=flat-square" alt="Dev"></a>
    &nbsp;
    <a href="https://www.linkedin.com/company/surrealdb/"><img src="https://img.shields.io/badge/linkedin-connect_with_us-0a66c2.svg?style=flat-square" alt="LinkedIn"></a>
</p>

<p align="center">
	<a href="https://surrealdb.com/blog"><img height="25" src="https://github.com/surrealdb/surrealdb/blob/main/img/social/blog.svg?raw=true" alt="Blog"></a>
	&nbsp;
	<a href="https://github.com/surrealdb/surrealdb"><img height="25" src="https://github.com/surrealdb/surrealdb/blob/main/img/social/github.svg?raw=true" alt="Github"></a>
	&nbsp;
    <a href="https://www.linkedin.com/company/surrealdb/"><img height="25" src="https://github.com/surrealdb/surrealdb/blob/main/img/social/linkedin.svg?raw=true" alt="LinkedIn"></a>
    &nbsp;
    <a href="https://x.com/surrealdb"><img height="25" src="https://github.com/surrealdb/surrealdb/blob/main/img/social/x.svg?raw=true" alt="X"></a>
    &nbsp;
    <a href="https://www.youtube.com/@surrealdb"><img height="25" src="https://github.com/surrealdb/surrealdb/blob/main/img/social/youtube.svg?raw=true" alt="YouTube"></a>
    &nbsp;
    <a href="https://dev.to/surrealdb"><img height="25" src="https://github.com/surrealdb/surrealdb/blob/main/img/social/dev.svg?raw=true" alt="Dev"></a>
    &nbsp;
    <a href="https://surrealdb.com/discord"><img height="25" src="https://github.com/surrealdb/surrealdb/blob/main/img/social/discord.svg?raw=true" alt="Discord"></a>
    &nbsp;
    <a href="https://stackoverflow.com/questions/tagged/surrealdb"><img height="25" src="https://github.com/surrealdb/surrealdb/blob/main/img/social/stack-overflow.svg?raw=true" alt="Stack Overflow"></a>
</p>

<br>

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/whatissurreal.svg?raw=true">&nbsp;&nbsp;What is SurrealDB?</h2>

SurrealDB is an end-to-end cloud native database for web, mobile, serverless, jamstack, backend, and traditional applications. SurrealDB reduces the development time of modern applications by simplifying your database and API stack, removing the need for most server-side components, allowing you to build secure, performant apps quicker and cheaper. SurrealDB acts as both a database and a modern, realtime, collaborative API backend layer. SurrealDB can run as a single server or in a highly-available, highly-scalable distributed mode - with support for SQL querying from client devices, GraphQL, ACID transactions, WebSocket connections, structured and unstructured data, graph querying, full-text indexing, geospatial querying, and row-by-row permissions-based access.

View the [features](https://surrealdb.com/features), the latest [releases](https://surrealdb.com/releases), the product [roadmap](https://surrealdb.com/roadmap), and [documentation](https://surrealdb.com/docs).

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/documentation.svg?raw=true">&nbsp;&nbsp;Documentation</h2>

For guidance on installation, development, deployment, and administration, see our [documentation](https://surrealdb.com/docs).

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/gettingstarted.svg?raw=true">&nbsp;&nbsp;Run using Docker</h2>

Docker can be used to manage and run SurrealDB database instances without the need to install any command-line tools. The SurrealDB docker container contains the full command-line tools for importing and exporting data from a running server, or for running a server itself.

For just getting started with a development server running in memory, you can start the container with basic initialization arguments to create a root user with "root" as username and password and enable debug logging.

```bash
docker run --rm --pull always --name surrealdb -p 8000:8000 surrealdb/surrealdb:latest start --log debug --user root --pass root memory
``` 

You can access the server using the same SurrealDB CLI provided in the image by using the `sql` command:

```bash
docker exec -it <container_name> /surreal sql -e http://localhost:8000 -u root -p root --ns test --db test --pretty
```

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/gettingstarted.svg?raw=true">&nbsp;&nbsp;Run using Docker Compose</h2>

The Docker image can be used with the `docker compose` command.

Here is an example of a basic `docker-compose.yml` file for quickly getting started.

```yaml
services:
  surrealdb:
    command: start 
    image: surrealdb/surrealdb:latest # Consider using a specific version
    pull_policy: always
    ports:
      - 8000:8000
    environment:
      - SURREAL_LOG=info # Use "info" in production
      - SURREAL_USER=root
      - SURREAL_PASS=root # Change this in production!
```

Most of the configuration of SurrealDB can be done through [environment variables](https://surrealdb.com/docs/surrealdb/cli/env).

You can find a comprehensive list of all the available environment variables in the help message of the `start` subcommand:

```shell
docker run --rm surrealdb/surrealdb:latest start --help
```

The image contains timezone data. Specify the required timezone with the `TZ`
environment variable:

```bash
docker run -e TZ=Europe/London surrealdb/surrealdb:latest start
```

SurrealDB can be executed as a non-root user for added security. This ensures that exploiting certain vulnerabilities in the SurrealDB process does not immediately result in privileged access to the container. When doing this, ensure that any files required by SurrealDB are mounted to the container in a volume and that are accessible to that non-root user through their ownership and permissions.

Here is an example of running the container with a persistent volume as a non-root user with Docker Compose:

```yaml
services:
  surrealdb:
    image: surrealdb/surrealdb:latest # Consider using a specific version
    pull_policy: always
    command: start rocksdb:/mydata/mydatabase.db
    user: "1000"
    ports:
      - 8000:8000
    volumes:
      - ./mydata:/mydata
    environment:
      - SURREAL_LOG=debug # Use "info" in production
      - SURREAL_USER=root
      - SURREAL_PASS=root # Change this in production!
```

In this example, you should ensure that the user with UID `1000` exists in the host and that it has access (e.g. ownership) to read and write in the `./mydata` directory. You can find the UID of the active user in the host by running `id -u`. You can also provide a group for the container process to run as, such as for example `user: "1000:1000"`.

The same behavior can be acomplished without Docker Compose by providing the `-u` or `--user` argument to [`docker run`](https://docs.docker.com/reference/cli/docker/container/run/). Similar mechanisms exist in other container management tools such as [Podman](https://docs.podman.io/en/latest/markdown/podman-run.1.html#user-u-user-group) or container orchestration systems such as [Kubernetes](https://kubernetes.io/docs/tasks/configure-pod-container/security-context/#set-the-security-context-for-a-pod).

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/community.svg?raw=true">&nbsp;&nbsp;Community</h2>

Join our growing community around the world, for help, ideas, and discussions regarding SurrealDB.

- View our official [Blog](https://surrealdb.com/blog)
- Chat live with us on [Discord](https://surrealdb.com/discord)
- Follow us on [X](https://x.com/surrealdb)
- Connect with us on [LinkedIn](https://www.linkedin.com/company/surrealdb/)
- Visit us on [YouTube](https://www.youtube.com/@surrealdb)
- Join our [Dev community](https://dev.to/surrealdb)
- Questions tagged #surrealdb on [Stack Overflow](https://stackoverflow.com/questions/tagged/surrealdb)

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/contributing.svg?raw=true">&nbsp;&nbsp;Contributing</h2>

We would &nbsp;<img width="15" src="https://github.com/surrealdb/surrealdb/blob/main/img/love.svg?raw=true">&nbsp; for you to get involved with SurrealDB development! If you wish to help, you can learn more about how you can contribute to this project in the [contribution guide](../CONTRIBUTING.md).

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/security.svg?raw=true">&nbsp;&nbsp;Security</h2>

For security issues, view our [vulnerability policy](https://github.com/surrealdb/surrealdb/security/policy), view our [security policy](https://surrealdb.com/legal/security), and kindly email us at [security@surrealdb.com](mailto:security@surrealdb.com) instead of posting a public issue on GitHub.

<h2><img height="20" src="https://github.com/surrealdb/surrealdb/blob/main/img/license.svg?raw=true">&nbsp;&nbsp;License</h2>

Source code for SurrealDB is variously licensed under a number of different licenses. A copy of each license can be found in [each repository](https://github.com/surrealdb).

- Libraries and SDKs, each located in its own distinct repository, are released under either the [Apache License 2.0](https://github.com/surrealdb/license/blob/main/APL.txt) or [MIT License](https://github.com/surrealdb/license/blob/main/MIT.txt).
- Certain core database components, each located in its own distinct repository, are released under the [Apache License 2.0](https://github.com/surrealdb/license/blob/main/APL.txt).
- Core database code for SurrealDB, located in [this repository](https://github.com/surrealdb/surrealdb), is released under the [Business Source License 1.1](/LICENSE).

For more information, see the [licensing information](https://github.com/surrealdb/license).
