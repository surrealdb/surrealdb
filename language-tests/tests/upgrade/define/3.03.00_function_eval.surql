/**
[env]
imports = ["upgrade/define/3.03.00_function_eval_import.surql"]


[test]
upgrade = true
reason = """
A function a released binary stored with a body calling `eval::surql` still
loads and renders on the current checkout, and reaching the call is judged by
the eval capability, which upgrade tests run without."""
# eval::surql first shipped in 3.3.0.
introduced = ">=3.3.0"

[[test.results]]
value = '''"DEFINE FUNCTION fn::run_eval() { RETURN eval::surql('RETURN 1') } PERMISSIONS FULL"'''

[[test.results]]
error = "Function 'eval::surql' is not allowed to be executed"
*/
RETURN (INFO FOR DB).functions.run_eval;
RETURN fn::run_eval();
