/**
[test]
reason = "Builtin function signatures behave identically on the legacy compute executor and the streaming executor: optional arguments, arity validation, and functions that previously were missing from the streaming function registry"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
error = "Incorrect arguments for function time::timezone(). Expected no arguments"

[[test.results]]
error = "Incorrect arguments for function time::hour(). Expected 0 to 1 arguments"

[[test.results]]
error = "Incorrect arguments for function rand::float(). Expected 0 or 2 arguments"

[[test.results]]
error = "Incorrect arguments for function session::db(). Expected no arguments"

[[test.results]]
error = "Incorrect arguments for function crypto::bcrypt::generate(). Expected 1 argument"

[[test.results]]
error = "Incorrect arguments for function crypto::argon2::compare(). Expected 2 arguments"

[[test.results]]
error = "Incorrect arguments for function http::get(). Expected 1 to 2 arguments"

[[test.results]]
error = "Incorrect arguments for function sequence::nextval(). Expected 1 argument"

[[test.results]]
error = "Incorrect arguments for function search::analyze(). Expected 2 arguments"

[[test.results]]
value = "'ok'"

[[test.results]]
error = "Incorrect arguments for function api::timeout(). Expected 3 arguments"

[[test.results]]
error = "Incorrect arguments for function api::req::body(). Expected 2 to 3 arguments"

[[test.results]]
error = "Incorrect arguments for function api::res::body(). Expected 2 to 3 arguments"

[[test.results]]
error = "Incorrect arguments for function api::res::status(). Expected 3 arguments"

[[test.results]]
error = "Incorrect arguments for function api::res::header(). Expected 3 to 4 arguments"

[[test.results]]
error = "Incorrect arguments for function api::res::headers(). Expected 3 arguments"
*/

-- Optional-argument functions default to the current time on both executors
RETURN type::is_int(time::hour());
RETURN type::is_int(time::day());
RETURN type::is_bool(time::is_leap_year());
RETURN type::is_string(time::timezone());

-- Arity validation matches the legacy executor
RETURN time::timezone(d"2024-05-05T12:34:56Z");
RETURN time::hour(d"2024-05-05T12:34:56Z", d"2024-05-05T12:34:56Z");
RETURN rand::float(1);
RETURN session::db(1);
RETURN crypto::bcrypt::generate("a", "b");
RETURN crypto::argon2::compare("a");
RETURN http::get();
RETURN sequence::nextval();
RETURN search::analyze("a");

-- API middleware functions are callable on both executors
RETURN api::timeout({}, |$req| "ok", 1s);
RETURN api::timeout();
RETURN api::req::body();
RETURN api::res::body();
RETURN api::res::status();
RETURN api::res::header();
RETURN api::res::headers();
