ACCESS my_access ON DATABASE PURGE EXPIRED, EXPIRED;
/* ===== result =====
ERROR:Error: Unexpected token `EXPIRED`, expected `FOR` or `;`
  |> ???:1:45
  |
1 | ACCESS my_access ON DATABASE PURGE EXPIRED, EXPIRED;
  |                                             ^^^^^^^ 


*/