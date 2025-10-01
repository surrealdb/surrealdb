# Contributing

We would &nbsp;<img width="15" src="./img/love.svg">&nbsp; for you to contribute to SurrealDB and help make it better! We want contributing to SurrealDB to be fun, enjoyable, and educational for anyone and everyone. All contributions are welcome, including features, bugfixes, and [documentation changes](https://github.com/surrealdb/docs.surrealdb.com), as well as updates and tweaks, blog post ideas, workshops, and everything else.

## How to start

If you are worried or don’t know where to start, check out our next section explaining what kind of help we could use and where can you get involved. You can ask us a question on the [SurrealDB Discord Server](https://surrealdb.com/discord) or [GitHub Discussions](https://github.com/surrealdb/surrealdb/discussions). Alternatively, you can message us on any channel in the [SurrealDB Community](https://surrealdb.com/community)!

## Code of conduct

Help us keep SurrealDB open and inclusive. Please read and follow our [Code of Conduct](/CODE_OF_CONDUCT.md).

## General notes on contributing to SurrealDB

SurrealDB is open source and contributions are most welcome. On the other hand, most of the development for SurrealDB is planned and done internally among the engineering team. It is very likely that a PR that shows up will not be reviewed for some time. Many merged PRs have spent a good number of weeks before being reviewed!

In theory, any and all PRs are welcome. In practice, there are two metrics that generally determine what will happen to a PR:

* How crucial is it?
* How large is it?

These metrics lead to two possible extremes:

* Crucial and small: A one-line bug fix, for example.
* Not crucial and large: A large implementation of some new functionality.

The former is quick to review and merge. The latter is still possible, but is best done with a good deal of discussion up front.

Some bigger features might need to go through our [RFC process](https://github.com/surrealdb/rfcs).

### What if my PR hasn't been looked at?

Here are some tips if you have an outstanding PR that you would like to get merged.

* **Current activity**: you can get an idea of the latest activity by [viewing PRs that are most recently updated](https://github.com/surrealdb/surrealdb/pulls?q=is%3Apr+is%3Aopen+sort%3Aupdated-desc), or [active branches](https://github.com/surrealdb/surrealdb/branches). If these tend to be particularly large, some major development may be going on and engineers may not have the head space to review PRs.
* **Talk about it**: you can cut through the noise a bit by mentioning your PR from time to time in our Discord community. SurrealDB staff are also present on Discord, and the majority of engineering team members monitor channels pertaining to their part of the code base. But also feel free to mention the PR in a more general channel if you would like the community to see and comment on it as well.
* **Adding images or videos**: a visual demonstration of the change can be the easiest way to cut through the noise and show what the PR does.

Finally, please keep in mind that **a PR may conflict with an upcoming addition that has not yet been announced**. It is possible that your PR may do something similar to an unannounced functionality that the engineering team is already working on! In this case it is not possible to even comment on the PR, as doing so may leak the fact that a similar functionality is already in development.

In short, feel free to submit any and all PRs, and then sit back and relax. Discuss it and ping people from time to time, update the PR if there are any merge conflicts, and enjoy the ride!

## Coding standards

SurrealDB uses cargo commands to ensure that code is formatted and linted to the same standards. To run them, use the following commands:

```bash
// Use this for formatting because nightly rustfmt is used
make fmt (or cargo make fmt)
cargo clippy
```

## Getting started from the source

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
cargo run --no-default-features --features \
storage-mem,http,scripting -- start --log trace \
--user root --pass root memory
```

To listen to code changes as you develop, use the following command:

```bash
cargo watch -x 'run --no-default-features \
--features storage-mem,http,scripting -- start \
--log trace --user root --pass root memory'
```

By default, SurrealDB runs locally on port 8000. To change the default listening address or port, use the following command:

```bash
cargo run --no-default-features --features \
storage-mem,http,scripting -- start --log trace \
--user root --pass root --bind 0.0.0.0:9000 memory
```

To run all tests manually, use the SurrealDB command-line from your terminal:

```bash
cargo test
```

Many tests have recently moved to the [language-tests](https://github.com/surrealdb/surrealdb/tree/main/crates/language-tests) crate which allows a test to be created using only SurrealQL via a .toml file which includes the queries and expected output. An example of a test:

```toml
/**
# The env map configures the general environment of the test
[env]
namespace = false
database = false
auth = { level = "owner" }
signin = {}
signup = {}

[test]
# Sets the reason behind this test; what exactly this test is testing.
reason = "Ensure multi line comments are properly parsed as toml."
# Whether to actually run this file, some files might only be used as an import,
# setting this to false disables running that test.
run = true

# set the expected result for this test
# Can also be a plain array i.e. results = ["foo",{ error = true }]
[[test.results]]
# the first result should be foo
value = "'foo'"

[[test.results]]
# the second result should be an error.
# You can error to a string for an error test, then the test will ensure that
# the error has the same text. Otherwise it will just check for an error without
# checking it's value.
error = true
*/

// The actual queries tested in the test.
RETURN "foo";
1 + "1";
```

To build a production-ready SurrealDB binary, execute the following command:

```bash
cargo build --release
```

We also have [a blog post](https://surrealdb.com/blog/making-your-own-pr-to-the-surrealdb-source-code) introducing the steps involved in creating a small sample PR.

## Scalability and Performance

SurrealDB is designed to be fast and to scale. It is built to work in both a single-node setup and as a distributed cluster. In distributed mode, SurrealDB builds upon [TiKV](https://tikv.org). Please keep in mind that SurrealDB is designed to be run in different environments, with different configurations, and at differing scales.

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

## Revisioned structs

SurrealDB uses Revision to manage versions of internal types, if these types are changed then the revisioning must be updated accordingly.
To keep track of these versions revision-lock is used generate a lock file. If the revision.lock check fails in CI you can install and run it with:
```bash
cargo install revision-lock
revision-lock
```

## Check existing topic labels

Issues on SurrealDB's GitHub repo have a label that begins with `topic:`, such as [`topic:typing`](https://github.com/surrealdb/surrealdb/issues?q=is%3Aissue%20state%3Aopen%20label%3Atopic%3Atyping) or `topic:record ids`. Referencing them may give greater insight into how to resolve an issue and perhaps even resolve other related issues at the same time.

## Submitting a pull request

The **branch name** is your first opportunity to give your task context.

Branch naming convention is as follows:

`TYPE-ISSUE_ID-DESCRIPTION`

It is recommended to combine the relevant [**GitHub Issue**](https://github.com/surrealdb/surrealdb/issues) with a short description that describes the task resolved in this branch. If you don't have GitHub issue for your PR, then you may avoid the prefix, but keep in mind that more likely you have to create the issue first. For example:

```
bugfix-548-ensure-queries-execute-sequentially
```

Where `TYPE` can be one of the following:

- **refactor** - code change that neither fixes a bug nor adds a feature
- **feature** - code changes that add a new feature
- **bugfix** - code changes that fix a bug
- **docs** - documentation only changes
- **ci** - changes related to CI system

### Commit your changes

- Write a descriptive **summary**: The first line of your commit message should be a concise summary of the changes you are making. It should be no more than 50 characters and should describe the change in a way that is easy to understand.

- Provide more **details** in the body: The body of the commit message should provide more details about the changes you are making. Explain the problem you are solving, the changes you are making, and the reasoning behind those changes.

- Use the **commit history** in your favour: Small and self-contained commits allow the reviewer to see exactly how you solved the problem. By reading the commit history of the PR, the reviewer can already understand what they'll be reviewing, even before seeing a single line of code.

### Create a pull request

- The **title** of your pull request should be clear and descriptive. It should summarize the changes you are making in a concise manner.

- Provide a detailed **description** of the changes you are making. Explain the reasoning behind the changes, the problem it solves, and the impact it may have on the codebase. Keep in mind that a reviewer was not working on your task, so you should explain why you wrote the code the way you did.

- Describe the scene and provide everything that will help to understand the background and a context for the reviewers by adding related GitHub issues to the description, and links to the related PRs, projects or third-party documentation. If there are any potential drawbacks or trade-offs to your changes, be sure to mention them too.

- Be sure to **request reviews** from the appropriate people. This might include the project maintainers, other contributors, or anyone else who is familiar with the codebase and can provide valuable feedback. You can also join our [Weekly Developer Office Hours](https://github.com/orgs/surrealdb/discussions/2118) to chat with the maintainers who will review your code! 

### Getting a better review

- [**Draft pull requests**](https://github.blog/2019-02-14-introducing-draft-pull-requests/) allow you to create a pull request that is still a work in progress and not ready for review. This is useful when you want to share your changes with others but aren't quite ready to merge them or request immediate feedback.    
https://github.blog/2019-02-14-introducing-draft-pull-requests/

- Once your pull request has been reviewed, be sure to **respond** to any feedback you receive. This might involve making additional changes to your code, addressing questions or concerns, or simply thanking reviewers for their feedback.  

- By using the [**re-request review** feature](https://github.blog/changelog/2019-02-21-re-request-review-on-a-pull-request/), you can prompt the reviewer to take another look at your changes and provide feedback if necessary.  

- The [**CODEOWNERS** file](https://github.com/surrealdb/surrealdb/blob/main/.github/CODEOWNERS) in GitHub allows you to specify who is responsible for code in a specific part of your repository. You can use this file to automatically assign pull requests to the appropriate people or teams and to ensure that the right people are notified when changes are made to certain files or directories.  

### Finalize the change

- We are actively using **threads** to allow for more detailed and targeted discussions about specific parts of the pull request. A resolved thread means that the conversation has been addressed and the issue has been resolved. Reviewers are responsible for resolving the comment and not the author. The author can simply add a reply comment that the change has been done or decline a request.

- When your pull request is approved, our team will be sure to **merge it responsibly**. This might involve running additional tests or checks, ensuring that the codebase is still functional.

### Summary

To summarize, fork the project and use the `git clone` command to download the repository to your computer. A standard procedure for working on an issue would be to:

1. Clone the `surrealdb` repository and download it to your computer.
    ```bash
    git clone https://github.com/surrealdb/surrealdb
    ```

    (Optional): Install [pre-commit](https://pre-commit.com/#install) to run the checks before each commit and run:

    ```bash
    pre-commit install
    ```

2. Pull all changes from the upstream `main` branch, before creating a new branch - to ensure that your `main` branch is up-to-date with the latest changes:
    ```bash
    git pull
    ```

3. Create a new branch from `main` like: `bugfix-548-ensure-queries-execute-sequentially`:
    ```bash
    git checkout -b "[the name of your branch]"
    ```

4. Make changes to the code, and ensure all code changes are formatted correctly:
    ```bash
    cargo fmt
    ```

5. Commit your changes when finished:
    ```bash
    git add -A
    git commit -m "[your commit message]"
    ```

6. Push changes to GitHub:
    ```bash
    git push origin "[the name of your branch]"
    ```

7. Submit your changes for review, by going to your repository on GitHub and clicking the `Compare & pull request` button.

8. Ensure that you have entered a commit message which details the changes, and what the pull request is for.

9. Now submit the pull request by clicking the `Create pull request` button.

10. Wait for code review and approval.

11. After approval, merge your pull request.

## Other Ways to Help

Pull requests are great, but there are many other areas where you can help.

### Blogging and speaking

Blogging, speaking about, or creating tutorials about one of SurrealDB's many features. Mention [@surrealdb](https://twitter.com/surrealdb) on Twitter, and email community@surrealdb.com so we can give pointers and tips and help you spread the word by promoting your content on the different SurrealDB communication channels. Please add your blog posts and videos of talks to our [showcase](https://github.com/surrealdb/showcase) repo on GitHub.

### Presenting at meetups

Presenting at meetups and conferences about your SurrealDB projects. Your unique challenges and successes in building things with SurrealDB can provide great speaking material. We’d love to review your talk abstract, so get in touch with us if you’d like some help!

### Feedback, bugs, and ideas

Sending feedback is a great way for us to understand your different use cases of SurrealDB better. If you want to share your experience with SurrealDB, or if you want to discuss any ideas, you can start a discussion on [GitHub discussions](https://github.com/surrealdb/surrealdb/discussions), chat with the [SurrealDB team on Discord](https://surrealdb.com/discord), or you can tweet [@tobiemh](https://twitter.com/tobiemh) or [@surrealdb](https://twitter.com/surrealdb) on Twitter. If you have any issues or have found a bug, then feel free to create an issue on [GitHub issues](https://github.com/surrealdb/surrealdb/issues).

### Documentation improvements

Submitting [documentation](https://surrealdb.com/docs) updates, enhancements, designs, or bug fixes, and fixing any spelling or grammar errors will be very much appreciated.

### Joining our community

Join the growing [SurrealDB Community](https://surrealdb.com/community) around the world, for help, ideas, and discussions regarding SurrealDB.

- View our official [Blog](https://surrealdb.com/blog)
- Follow us on [Twitter](https://twitter.com/surrealdb)
- Connect with us on [LinkedIn](https://www.linkedin.com/company/surrealdb/)
- Join our [Dev community](https://dev.to/surrealdb)
- Chat live with us on [Discord](https://discord.gg/surrealdb)
- Get involved on [Reddit](http://reddit.com/r/surrealdb/)
- Read our blog posts on [Medium](https://medium.com/surrealdb)
- Questions tagged #surrealdb on [Stack Overflow](https://stackoverflow.com/questions/tagged/surrealdb)
