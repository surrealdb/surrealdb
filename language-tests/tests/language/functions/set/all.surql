/**
[env]
imports = ["harness/assert.surql"]

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Incorrect arguments for function set::all(). Argument 1 was the wrong type. Expected `set` but found `'some text'`"

[[test.results]]
error = "Incorrect arguments for function set::all(). Argument 1 was the wrong type. Expected `set` but found `{ bar: 2, foo: 1 }`"


*/

BEGIN;
{
    fn::assert(set::all({,}));
    fn::assert(set::all({1,}));
    fn::assert(set::all({1,2}));
    fn::assert(set::all({true,}));
    fn::assert(set::all({"a",}));
    fn::assert(!set::all({false,}));
    fn::assert(!set::all({1,2,"text",3,NONE,3,4}));

    fn::assert(set::all({"foo","foo"},"foo"));
    fn::assert(!set::all({"foo","foo"},"bar"));
    fn::assert(!set::all({"bar","foo"},"bar"));

    fn::assert(!set::all({"bar","foo"},/foo|bar/));
    fn::assert("bar" = /foo|bar/);
    fn::assert("foo" = /foo|bar/);

    fn::assert(set::all({/foo|bar/,},/foo|bar/));

    fn::assert(!set::all({1,2},{1,2}));
    fn::assert(set::all({{1,2},},{1,2}));


    RETURN "OK";
};
COMMIT;

set::all("some text");
set::all({ foo: 1, bar: 2});
