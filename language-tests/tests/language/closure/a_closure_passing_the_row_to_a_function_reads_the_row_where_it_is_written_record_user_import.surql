/**
[test]
reason = "Setup for a_closure_passing_the_row_to_a_function_reads_the_row_where_it_is_written_record_user.surql"
run = false
*/
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid ) DURATION FOR TOKEN 1h, FOR SESSION 1h;
CREATE person:bob SET lim = 1;
DEFINE FUNCTION fn::thr($r: any) { RETURN [0, 8, 100][$r.lim]; };
DEFINE TABLE pt SCHEMALESS PERMISSIONS FOR select WHERE array::any(xs, |$e| $e >= fn::thr($this));
CREATE pt:1 SET lim = 2, xs = [0, 5];
CREATE pt:2 SET lim = 0, xs = [2, 9];
CREATE pt:3 SET lim = 1, xs = [3, 0];
