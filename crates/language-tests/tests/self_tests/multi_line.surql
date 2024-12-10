/**
[env]
# If true this will cause the test to run in a clean datastore instead of reusing an existing on.
clean = false 
# Set the namespace and database
# By default the namespace and database are set to "test".
# You can also run tests without a namespace or database by setting the option to false
namespace = "test"
database = "test"
# set timeout for the test to 10 miliseconds 
# Also takes true or false for the default or disabled timeout
# Defaults to 1 second.
timeout = 1000
# Whether this test must be run on it's own without other tests running at the same time.
sequential = false
# Allows you to define a file to run first, use to for example load some records before running queries.
# Files are relative to the root of test directory.
imports = []

[test]
# Sets the reason behind this test; what exactly this test is testing.
reason = "Unsure multi line comments are properly parsed as toml."
# Whether to actually run this file, some files might only be used as an import, setting this to 
# false disables running that test.
run = true

# set the expected result for this test
# Can also be a plain array i.e. results = ["foo",{ error = true }]
[[test.results]]
# the first result should be foo
value = "'foo'"

[[test.results]]
# the second result should be an error. 
# You can error to a string for an error test, then the test will ensure that the error has the same text.
# Otherwise it will just check for an error without checking it's value.
error = true
*/

RETURN "foo";
1 + "1";
