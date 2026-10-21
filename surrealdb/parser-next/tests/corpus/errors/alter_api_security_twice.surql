-- `SECURITY` is set once per `ALTER API`; a second clause is refused rather than
-- silently overriding the first.
ALTER API "/x" SECURITY INVOKER SECURITY DEFINER;
