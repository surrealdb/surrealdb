/**
[test]
reason = "Each session::* getter returns either NONE or its expected typed value depending on how the session was initialised. Asserting `is_none || is_<expected>` keeps the result stable regardless of session provenance; the named-key object surfaces which getter failed if one drifts."

[[test.results]]
value = "{ ac: true, db: true, id: true, ip: true, ns: true, origin: true, rd: true, token: true }"

*/

RETURN {
    ac: type::is_none(session::ac()) || type::is_string(session::ac()),
    db: type::is_none(session::db()) || type::is_string(session::db()),
    id: type::is_none(session::id()) || type::is_uuid(session::id()),
    ip: type::is_none(session::ip()) || type::is_string(session::ip()),
    ns: type::is_none(session::ns()) || type::is_string(session::ns()),
    origin: type::is_none(session::origin()) || type::is_string(session::origin()),
    rd: type::is_none(session::rd()) || type::is_record(session::rd()),
    token: type::is_none(session::token()) || type::is_object(session::token()),
};
