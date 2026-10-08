/**
[test]
reason = "Setup for 3.03.00_function_eval.surql: 3.3.0 writes a function whose body calls eval::surql."
run = false
*/
DEFINE FUNCTION fn::run_eval() { RETURN eval::surql("RETURN 1"); } PERMISSIONS FULL;
