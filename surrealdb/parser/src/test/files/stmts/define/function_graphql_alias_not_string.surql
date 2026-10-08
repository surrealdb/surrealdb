DEFINE FUNCTION fn::a() { RETURN 1 } GRAPHQL_ALIAS 1;
/* ===== result =====
ERROR:Error: Unexpected token `1`, expected a string
  |> ???:1:52
  |
1 | DEFINE FUNCTION fn::a() { RETURN 1 } GRAPHQL_ALIAS 1;
  |                                                    ^ 


*/