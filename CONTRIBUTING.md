<h1><img height="25" src="/img/contributing.svg">&nbsp;&nbsp;Contributing</h1>

We would &nbsp;<img width="15" src="/img/love.svg">&nbsp; for you to contribute to SurrealDB and help make it better! We want contributing to SurrealDB to be fun, enjoyable, and educational for anyone and everyone. All contributions are welcome, including features, bugfixes, and documentation changes, as well as updates and tweaks, blog posts, workshops, and everything else.

## How to start

If you are worried or don’t know where to start, check out our next section explaining what kind of help we could use and where can you get involved. You can ask us a question on [GitHub discussions](https://github.com/surrealdb/surrealdb/discussions), or by tweeting [@tobiemh](https://twitter.com/tobiemh) or [@surrealdb](https://twitter.com/surrealdb) on Twitter. Alternatively chat with the [SurrealDB team on Discord](https://surrealdb.com/discord).

## Code of conduct

Help us keep SurrealDB open and inclusive. Please read and follow our [Code of Conduct](/CODE_OF_CONDUCT.md).

## Coding standards

SurrealDB uses [`rustfmt`](https://github.com/rust-lang/rustfmt) to ensure that all code is formatted to the same standards. To install `rustfmt` run the following code:

```bash
rustup component add rustfmt
```

## Getting started from source

To set up a working **development environment**, you can either [use the Nix package manager](pkg/nix#readme) or you can [install dependencies manually](doc/BUILDING.md#building-surrealdb) and ensure that you have `rustup` installed, and fork the project git repository.

> Please note that these instructions are for setting up a functional dev environment. If you just want to install SurrealDB for day-to-day usage and not as a code maintainer use this [installation guide](https://surrealdb.com/docs/install). If you want to get started integrating SurrealDB into your app, view the [integration tutorials](https://surrealdb.com/docs/integration).

```bash
# Use the default (stable) release channel if prompted
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
git clone git@github.com:[YOUR_FORK_HERE]/surrealdb.git
cd surrealdb
cargo run -- help
```

To run the SurrealDB database server, use the following command:

```bash
cargo run -- start --log trace --user root --pass root memory
```

To listen to code changes as you develop, use the following command:

```bash
cargo watch -x 'run -- start --log trace --user root --pass root memory'
```

SurrealDB runs by default on port 8000. To change the default port, use the following command:

```bash
cargo run -- start --log trace --user root --pass root --bind 0.0.0.0:9000 memory
```

To run all tests manually, use the SurrealDB command-line from your terminal:

```bash
cargo test
```

To build a production-ready SurrealDB binary, execute the following command:

```bash
cargo build --release
```

## Scalability and Performance

SurrealDB is designed to be fast, and to scale. It is built to work in both a single-node setup, and as a distributed cluster. In distributed mode, SurrealDB builds upon [TiKV](https://tikv.org). Please keep in mind that SurrealDB is designed to be run in different environments, with different configurations, and at differing scales.

When contributing code, please take into account the following considerations:

- SurrealDB startup time
- Query execution time
- Query response times
- Query throughput
- Requests per second
- Websocket connections
- Network usage
- Memory usage

## Security and Privacy

We take the security of SurrealDB code, software, and cloud platform very seriously. If you believe you have found a security vulnerability in SurrealDB, we encourage you to let us know right away. We will investigate all legitimate reports and do our best to quickly fix the problem.

Please report any issues or vulnerabilities to security@surrealdb.com, instead of posting a public issue in GitHub. Please include the SurrealDB version identifier, by running `surreal version` on the command-line, and details on how the vulnerability can be exploited.

When developing, make sure to follow the best industry standards and practices.

## External dependencies

Please avoid introducing new dependencies to SurrealDB without consulting the team. New dependencies can be very helpful but also introduce new security and privacy issues, complexity, and impact total docker image size. Adding a new dependency should have vital value on the product with minimum possible risk.

## Introducing new features

We would &nbsp;<img width="15" src="/img/love.svg">&nbsp; you to contribute to SurrealDB, but we would also like to make sure SurrealDB is as great as possible and loyal to its vision and mission statement. For us to find the right balance, please open a question on [GitHub discussions](https://github.com/surrealdb/surrealdb/discussions) with any ideas before introducing a new pull request. This will allow the SurrealDB community to have sufficient discussion about the new feature value and how it fits in the product roadmap and vision.

This is also important for the SurrealDB lead developers to be able to give technical input and different emphasis regarding the feature design and architecture. Some bigger features might need to go through our [RFC process](https://github.com/surrealdb/rfcs).

## Submitting a pull request

Branch naming convention is as following 

`TYPE-ISSUE_ID-DESCRIPTION`

For example:
```
bugfix-548-ensure-queries-execute-sequentially
```

Where `TYPE` can be one of the following:

- **refactor** - code change that neither fixes a bug nor adds a feature
- **feature** - code changes that add a new feature
- **bugfix** - code changes that fix a bug
- **docs** - documentation only changes
- **ci** - changes related to CI system

For the initial start, fork the project and use git clone command to download the repository to your computer. A standard procedure for working on an issue would be to:

1. Clone the `surrealdb` repository and download to your computer.
```bash
git clone https://github.com/surrealdb/surrealdb
```

1. Pull all changes from the upstream `main` branch, before creating a new branch - to ensure that your `main` branch is up-to-date with the latest changes:
```bash
git pull
```

2. Create new branch from `main` like: `bugfix-548-ensure-queries-execute-sequentially`:
```bash
git checkout -b "[the name of your branch]"
```

3. Make changes to the code, and ensure all code changes are formatted correctly:
```bash
cargo fmt
```

4. Commit your changes when finished:
```bash
git add -A
git commit -m "[your commit message]"
```

5. Push changes to GitHub:
```bash
git push origin "[the name of your branch]"
```

6. Submit your changes for review, by going to your repository on GitHub and clicking the `Compare & pull request` button.

7. Ensure that you have entered a commit message which details about the changes, and what the pull request is for.

8. Now submit the pull request by clicking the `Create pull request` button.

9. Wait for code review and approval.

10. After approval, merge your pull request.

## Other Ways to Help

Pull requests are great, but there are many other areas where you can help.

### Blogging and speaking

Blogging, speaking about, or creating tutorials about one of SurrealDB's many features. Mention [@surrealdb](https://twitter.com/surrealdb) on Twitter, and email team@surrealdb.com so we can give pointers and tips and help you spread the word by promoting your content on the different SurrealDB communication channels. Please add your blog posts and videos of talks to our [showcase](https://github.com/surrealdb/showcase) repo on GitHub.

### Presenting at meetups

Presenting at meetups and conferences about your SurrealDB projects. Your unique challenges and successes in building things with SurrealDB can provide great speaking material. We’d love to review your talk abstract, so get in touch with us if you’d like some help!

### Feedback, bugs, and ideas

Sending feedback is a great way for us to understand your different use cases of SurrealDB better. If you want to share your experience with SurrealDB, or if you want to discuss any ideas, you can start a discussion on [GitHub discussions](https://github.com/surrealdb/surrealdb/discussions), chat with the [SurrealDB team on Discord](https://surrealdb.com/discord), or you can tweet [@tobiemh](https://twitter.com/tobiemh) or [@surrealdb](https://twitter.com/surrealdb) on Twitter. If you have any issues, or have found a bug, then feel free to create an issue on [GitHub issues](https://github.com/surrealdb/surrealdb/issues).

### Documentation improvements

Submitting documentation updates, enhancements, designs, or bug fixes, and fixing any spelling or grammar errors will be very much appreciated.

### Joining our community

Join our growing community around the world, for help, ideas, and discussions regarding SurrealDB.

- View our official [Blog](https://medium.com/surrealdb)
- Follow us on [Twitter](https://twitter.com/surrealdb)
- Connect with us on [LinkedIn](https://www.linkedin.com/company/surrealdb/)
- Join our [Dev community](https://dev.to/surrealdb)
- Chat live with us on [Discord](https://discord.gg/GSeTUeA)
- Questions tagged #surrealdb on [StackOverflow](https://stackoverflow.com/questions/tagged/surrealdb)
