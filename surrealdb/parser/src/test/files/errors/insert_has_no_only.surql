-- Pins that INSERT takes no ONLY. Every other record-returning statement has
-- one, so the asymmetry reads like an oversight; it is not. INSERT writes a
-- list and answers with a list, and `InsertStatement` carries no `only` field
-- for a parser clause to fill. Adding one to make the statement family uniform
-- would make this parse and quietly give INSERT a single-record cardinality
-- rule it has never had.
INSERT ONLY INTO person { name: 'tobie' };
/* ===== result =====
ERROR:Error: Unexpected token `INTO`, expected `;`
  |> ???:7:13
  |
7 | INSERT ONLY INTO person { name: 'tobie' };
  |             ^^^^ Maybe missing a semicolon before this token?
7 | INSERT ONLY INTO person { name: 'tobie' };
  | ----------- This last statement here was parsed as an expression
7 | INSERT ONLY INTO person { name: 'tobie' };
  |            - Expected a `;` here


*/