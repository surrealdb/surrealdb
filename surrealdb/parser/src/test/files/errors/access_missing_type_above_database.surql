-- `TYPE RECORD` is what an omitted `TYPE` means, and only a database accepts
-- it. Rejecting the default here keeps this parser in step with the rule it
-- already applies to an explicit `TYPE RECORD` on ROOT or NAMESPACE.
DEFINE ACCESS a ON ROOT;
/* ===== result =====
ERROR:Error: Missing access type, an access at this level must specify a `TYPE`
  |> ???:4:1
  |
4 | DEFINE ACCESS a ON ROOT;
  | ^^^^^^^^^^^^^^^^^^^^^^^ 


*/