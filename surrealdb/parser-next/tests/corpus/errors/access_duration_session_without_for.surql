-- Pins that `SESSION` is reachable only after `FOR` inside DURATION. Do not
-- "fix" this by adding a top-level SESSION clause to DEFINE ACCESS: that would
-- turn this mistyping of the documented form into a valid statement meaning
-- something else entirely, silently. CONTEXT was chosen over SESSION for the
-- authorisation-context clause for exactly this reason.
DEFINE ACCESS a ON DATABASE TYPE RECORD DURATION FOR TOKEN 1h, SESSION 1h;
