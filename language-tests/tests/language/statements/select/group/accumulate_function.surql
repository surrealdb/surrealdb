/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''[{ "array::join": 'Hello grouping world' }]'''

*/

{
    CREATE tgt:1 SET txt = "Hello";
    CREATE tgt:2 SET txt = "grouping";
    CREATE tgt:3 SET txt = "world";
    RETURN "OK";
};

SELECT array::join(txt," ") FROM tgt GROUP ALL;
