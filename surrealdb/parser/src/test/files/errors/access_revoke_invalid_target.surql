ACCESS my_access ON DATABASE REVOKE EVERYTHING;
/* ===== result =====
ERROR:Error: Unexpected token `EVERYTHING`, expected `ALL`, `GRANT`, or `WHERE`
  |> ???:1:37
  |
1 | ACCESS my_access ON DATABASE REVOKE EVERYTHING;
  |                                     ^^^^^^^^^^ 


*/