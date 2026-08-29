ACCESS my_access ON DATABASE PURGE FOR 1h;
/* ===== result =====
ERROR:Error: Unexpected token `FOR`, expected `EXPIRED` or `REVOKED`
  |> ???:1:36
  |
1 | ACCESS my_access ON DATABASE PURGE FOR 1h;
  |                                    ^^^ 


*/