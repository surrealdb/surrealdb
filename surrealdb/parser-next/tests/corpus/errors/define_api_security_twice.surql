-- `SECURITY` is set once per `DEFINE API`; a second clause is refused rather than
-- silently overriding the first.
DEFINE API "/x" FOR get THEN { RETURN 1; } SECURITY INVOKER SECURITY DEFINER;
