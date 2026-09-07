-- Pins that `SESSION` is reachable only after `FOR` inside DURATION. Do not
-- "fix" this by adding a top-level SESSION clause to DEFINE ACCESS: that would
-- turn this mistyping of the documented form into a valid statement meaning
-- something else entirely, silently. CONTEXT was chosen over SESSION for the
-- authorisation-context clause for exactly this reason.
DEFINE ACCESS a ON DATABASE TYPE RECORD DURATION FOR TOKEN 1h, SESSION 1h;
/* ===== result =====
ERROR:Error: Unexpected token `SESSION`, expected `;`
  |> ???:6:64
  |
6 | DEFINE ACCESS a ON DATABASE TYPE RECORD DURATION FOR TOKEN 1h, SESSION 1h;
  |                                                                ^^^^^^^ Maybe missing a semicolon before this token?
6 | DEFINE ACCESS a ON DATABASE TYPE RECORD DURATION FOR TOKEN 1h, SESSION 1h;
  | --------------------------- This last statement here was parsed as a statement expression
6 | DEFINE ACCESS a ON DATABASE TYPE RECORD DURATION FOR TOKEN 1h, SESSION 1h;
  |                            - Expected a `;` here


*/