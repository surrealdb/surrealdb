# SurrealQL Language Tests

This directory contains the surrealql language test suite, consisting of a 
directory of tests specified in surrealql as well as a command line tool able 
to run the files and verify their output. 

## SurrealQL Language Test CLI tool

The surrealql language test CLI tool is the thing which will actually run the
test and verify its output. It can be run within this directory with a simple
`cargo run run` (the second run is a command to the tool itself). This will use
the `test` directory as the test root and run all the tests contained within.

The tool can take some flags to change it's behavior. Of note are the filter,
and the `--failure` flag.

When you run `cargo run run foo` you are running the tests with a filter. In
this case only test which contain the word `foo` within its path will be run.
This is usefull for running a single or more test you are working on.

The `--failure` flag will change what the tool does with it's result. If you run
the test with `--failure accept` the tool will automatically fill in results for
any tests which did not specify any results. This is useful for quickly
generating expected output for new tests. You can also specify `--failure
overwrite`, which will overwrite the specified results of test if they do not
match the actual results of the test. This flag should be used sparingly and
only after inspecting if the new results are actually valid. 

## SurrealQL Language Test Format

Language test are plain surrealql files that are parse-able by the normal
surrealql parser. Any surrealql file can be a surrealql test, however for a test
to be useful it needs to specify a test configuration. This is done by
including a special comment, called here after a test comment, in the surrealql 
file which specifies how the test should be run and their results verified.

Test comments are either single line forward slash comments in the form of `//!`
or a multi line comment in the form of `/** */` (note the `!` and extra `*`).
When a test is run all the tests comments are concatenated and parsed as TOML.

See below an example of a surrealql test file.
```surrealql
/**
# The env map configures the general environment of the test
[env]
namespace = false
database = false

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

## SurrealQL Language Test Config Format.

The surrealql test config is specified within the test comment. It is formatted
in toml and contains the following sections:

- `[test]` Defines the information about the test
	- `reason` a string for detailing why the test exists.
	- `run` a flag whether this file should be run as a test
	- `issue` the github issue number to which this test is related.
	- `wip` whether this test is of a known issue or a work in progress feature
	- `[results]` an table which specifies the expected results of the test

- `[env]` Defines information about how the test should be run.
	- `sequential` should the test be run without any other test running in
	  parallel
	- `clean` should the test be run in a completely clean database.
	- `namespace` the namespace, if any, to run the test in
	- `database` the database, if any, to run the test in
	- `imports` a list of files to run before the test.
	- `timeout` a duration in milliseconds that the test is allowed to take.
	- `[login]` the login configuration into the dataset.
	- `[capabilities]` a configuration of database capabilities in which the
	  test should run.

All keys in the config are optional and have default values if not explicitly 
specified.

### `[test]`

The `[test]` table specifies the information about the test itself. 

#### `[test.run]`
Sometimes you want to include a SurrealQL file into the testing suite but not
actually care about it's results, for example when the file is intended for as
an import for other test files. In this case you can disable running the file as
a test by setting `[test.run]` to false. Defaults to `true`

#### `[test.wip]`
Some tests can be test for a work in progress feature or a known issue or bug
that we cannot fix right now. We can still test these these bugs and features by
setting `[test.wip]` to true. Doing so will turn errors in the
tests results into warnings which prevents a test run which includes such a test
from failing. Furthermore it will also exclude the tests from having it's
results automatically be updated by the CI tool when running with 
`--failure accept.` or `--failure overwrite`. Defaults to `false`

#### `[test.reason]` and `[test.issue]`
Information about the test it's reason to exist and the issue from which it
originated. These are mostly just for documentation however when a test is
`wip` and has an issue specified the CLI will then suggest closing the issue if
the test succeeded. Both default to `None` i.e. have no value.


#### `[test.results]`
The test results table specifies the expected out of the test. The command line
tool will warn about every test that does not includes a this table in its
configuration. This table can either be a straight table or an array of tables.

Examples: 
```toml
[test.results]
parse-error = "foo"
```
This tests if the test returns a parsing error with the text `foo`. A test is
parsed once and can only return a single parsing error. So when testing for a
parsing error only a single result is allowed to be specified.

Note that the following are also allowed:
```toml
[test.results]
parse-error = true

[test.results]
parse-error = false
```
Specifying a boolean will check for the presence or absence of a parsing error
but will not check the value of the error.

If the test is not intended to return a parsing error it is general best to
specify the actual expected output of the tests. A single SurrealQL query can 
consist of multiple statements which produce either zero or one result. The test 
allow specifying the expected number and value of the results. Generally
specified test results will look like the following:
```toml
[[test.results]]
value = "[{ id: foo:bar, name: 'bar' }]"

[[test.results]]
error = "Some error is happening here"

[[test.results]]
error = false 

[[test.results]]
error = true
```
Above we specify that the test should return two results. The first should be a
value as described by the value in the string. When specifying a value in TOML
strings containing SurrealQL are used. The second result should be an error with 
the given string as the error text. The third result just specifies that that
result should not be an error but gives no details about what the actual value
should be. The final result only specifies that the result should be an error
but not what the error text should be.

A test with these results will fail if the test returned less or more values and
the results are not equal to the given results.

##### Rough equality
There are some values in SurrealQL which are inherently indeterministic and so
would cause problems for test which check if the output is always the same.
Generic record-id's is an example, they generally have a ULID key which is
always random. 

To solve this problem in tests, you can have rough equality when matching a
value. With rough equality the value of record-id keys are not tested, only if
they are of the right type.

##### Matching expressions
When matching against a value is not possible you can also fall back to running
a SurrealQL expression to match the output. Results which should be validated
with a matching expression are created by setting the `match` field on
`[[test.results]]`. The match expression must be a valid surrealql expression
which should return a boolean true when the expression found the output to be
valid. The matching expression can access the value with either the `$result`
param or the `$error` param. The latter being defined when the output of the
current matched statement was an error, being defined with the text of the error
as a string. It is often the case that a matching expression should only match a
value or an error but not both. In this case you can set the `error` field on
the same `[[test.results]]` to either true or false depending on if an error was
expected or not. See below for some examples of matching expressions:

```toml

# Tests if the statement output was either the string foo or an error: 'An error
# occurred: foo'
[[test.results]]
match = "$result == 'foo' || $error == 'An error occurred: foo'"

# Tests an error with a regex as some parts of the error are non-deterministic.
[[test.results]]
match = "$error = /Found record: `thing:.*` which is not a relation, but expected a  NORMAL/"
# This matching should only match errors.
error = true

# Test whether the field of a result matched the regex
[[test.results]]
match = """
$result.users.test = /DEFINE USER test ON ROOT PASSHASH '\\$argon2id\\$.*' ROLES VIEWER DURATION FOR TOKEN 1h, FOR SESSION NONE/
"""
error = false
```



### `[env]`

The `[env]` table specifies the environment in which the test must be run. 

#### `[env.clean]`
To ensure test run quickly the CLI will generally try to reuse databases between
tests. When a test is completed the database and namespace it ran in will be
removed so that the next test can be ran in a clean environment. If a test can
cause a database to be altered even after the database and namespace where
removed then the test should be run within a fully clean database. This can be
done with this flag which will cause the test to be run in a freshly create
database which will be destroyed after the test completes. Defaults to `false`.

#### `[env.sequential]`
To run test as quickly as possible test the CLI will try to run tests in
parallel when possible. If this would cause issues or if this test consume a lot
of threads it might be better to ensure that no other test is running at the
same time. Setting `[env.sequential]` will ensure that a test is run when no
other test is running. Defaults to `false`.

#### `[env.namespace]` and `[env.database]`
These keys set the name of the namespace and database the test is run in. These
can either be a string or a boolean. When the key is a string the string will be
the name for the namespace or database. If the key is false then the test will
not be run in a namespace or database. If the key is true it will default to the
default namespace and database name: `"test"`. Defaults to `true`

#### `[env.imports]`
An array of string which you can use to specify files to run before running the
test. Each string is a path to a file relative to the root of the test 
directory. Each of these files will be run in a database with full capabilities 
and the given namespace and database. The test is only run after the files in
imports are run to completion. 

This can be used to import a dataset before running queries, or importing
utility functions. 

Defaults to `[]`

#### `[env.timeout]`
Specifies a duration in milliseconds within which the test should finish. If the
test takes longer than the given duration it will be considered an error and it
will cause a test run to fail. This key can also be set to `false` to disable
the timeout altogether or `true` to default to 1 second. Defaults to `1000`.


#### `[env.login]`
Specify the login into the datastore. This can be either a level login or a 
record login. The first is a login into a specific part of the datastore, either 
`root`, `database`, or `namespace`. When specifying `database` and `namespace`
the database and namespace that is used for login is the same a the one
specified in `[env.database]` or `[env.namespace]`. When database or namespace
are specifically unset the login will be for the `test` database and/or
namespace.

When logging into a level you can also specify a role, either `owner`, `editor` 
or `viewer`. 

Configuring a level login is done by setting the `level` field on the 
`env.login` table and optionally the role field. See below for an example:
```toml
[env]
login = { level = "database", role = "viewer" }
```

Instead of configuring a login for a level one can also use a record based
login. This is done by specifying the `access` and `rid` fields on `login`. Note
that record based and level based login are mutally exclusive, you can only
specify one.

For the record based login the `access` field is the name of the access grant
for which the record-id in `rid` is the specific record for which the test was
authenticated.


#### `[env.capabilities]`
This is a table which can be used to specify the capabilities with which the
database should be run. This can be used to disable functions, net targets, http
routs and scripting just like with the SurrealDB binary/rust SDK. Defaults to all 
capabilities enabled.
