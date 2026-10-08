/**
[test]
reason = """
Schema statements inside FOR bodies and IF branches: what follows each sees what it defined.
A field's VALUE is definer code, run in a privileged frame that holds none of the caller's
bindings, so a VALUE reading the loop's own variable reads NONE; one reading a parameter the
iteration defines reads that iteration's value.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: sl_a:0, m: 'none', n: 0 }, { id: sl_a:1, m: 'one', n: 1 }, { id: sl_a:2, m: 'one', n: 2 }]"

[[test.results]]
value = "[[{ id: sl_later:1, k: 7 }], [sl_b:0, sl_b:2]]"

[[test.results]]
value = "[NONE, NONE, NONE]"

[[test.results]]
value = "[sl_c:1, sl_c:3]"

[[test.results]]
value = "'defined'"

[[test.results]]
error = "An error occurred: stop at 1"

[[test.results]]
error = "The table 'sl_e' does not exist"

[[test.results]]
value = "[1, 2, 3]"

*/

FOR $i IN 0..3 {
    IF $i = 1 { DEFINE FIELD OVERWRITE m ON sl_a VALUE 'one' } ELSE IF $i = 0 { DEFINE FIELD m ON sl_a VALUE 'none' };
    CREATE type::record('sl_a', $i) SET n = $i;
};

SELECT * FROM sl_a ORDER BY id;

{
    FOR $i IN 0..3 {
        IF $i = 1 {
            DEFINE TABLE sl_later;
            DEFINE FIELD k ON sl_later VALUE 7;
            CREATE sl_later:1;
        } ELSE {
            CREATE type::record('sl_b', $i);
        };
    };
    RETURN [(SELECT * FROM sl_later), (SELECT VALUE id FROM sl_b ORDER BY id)];
};

{
    FOR $i IN 1..4 {
        DEFINE FIELD OVERWRITE v ON sl_v VALUE $i;
        CREATE type::record('sl_v', $i);
    };
    RETURN SELECT VALUE v FROM sl_v ORDER BY id;
};

{
    FOR $i IN 0..5 {
        IF $i = 0 { DEFINE INDEX sl_c_n ON sl_c FIELDS n; CONTINUE };
        IF $i = 4 { BREAK };
        CREATE type::record('sl_c', $i) SET n = $i % 2;
    };
    RETURN SELECT VALUE id FROM sl_c WHERE n = 1 ORDER BY id;
};

IF true { DEFINE TABLE sl_d; CREATE sl_d:1; RETURN 'defined' };

FOR $i IN 0..3 {
    IF $i = 1 { THROW 'stop at ' + <string> $i };
    DEFINE TABLE IF NOT EXISTS sl_e;
    CREATE type::record('sl_e', $i);
};

SELECT * FROM sl_e;

{
    FOR $i IN 1..4 {
        DEFINE PARAM OVERWRITE $sl_cur VALUE $i;
        DEFINE FIELD OVERWRITE v ON sl_w VALUE $sl_cur;
        CREATE type::record('sl_w', $i);
    };
    RETURN SELECT VALUE v FROM sl_w ORDER BY id;
};
