/**
[env]
imports = ["harness/assert.surql"]

[test]

[[test.results]]
value = "'OK'"

[[test.results]]
error = "Incorrect arguments for function array::all(). Argument 1 was the wrong type. Expected `array` but found `'some text'`"

[[test.results]]
error = "Incorrect arguments for function array::all(). Argument 1 was the wrong type. Expected `array` but found `{ bar: 2, foo: 1 }`"


*/

BEGIN;
fn::assert(array::all([]));
fn::assert(array::all([1]));
fn::assert(array::all([1,2]));
fn::assert(array::all([true]));
fn::assert(array::all(["a"]));
fn::assert(!array::all([false]));
fn::assert(!array::all([1,2,"text",3,NONE,3,4]));

fn::assert(array::all(["foo","foo"],"foo"));
fn::assert(!array::all(["foo","foo"],"bar"));
fn::assert(!array::all(["bar","foo"],"bar"));

// TODO: Should this be false? "bar" = /foo|bar/ evals to true and "foo" = /foo|bar/ also evals to true.
fn::assert(!array::all(["bar","foo"],/foo|bar/));
fn::assert("bar" = /foo|bar/);
fn::assert("foo" = /foo|bar/);

fn::assert(array::all([/foo|bar/],/foo|bar/));

fn::assert(!array::all([1,2],[1,2]));
fn::assert(array::all([[1,2]],[1,2]));


RETURN "OK";
COMMIT;

array::all("some text");
array::all({ foo: 1, bar: 2});
