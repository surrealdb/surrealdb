/**
[env]
namespace = true
database = true

[test]
reason = "REMOVE TYPE is refused while a database access method's SIGNUP, SIGNIN, AUTHENTICATE or CONTEXT clause names the type"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'mood': the SIGNIN clause of the access method 'acc' names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'mood': the AUTHENTICATE clause of the access method 'acc' names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/
DEFINE TYPE mood AS 'sad' | 'ok';
DEFINE ACCESS acc ON DATABASE TYPE RECORD SIGNIN (SELECT * FROM user WHERE m = <mood>'ok');
REMOVE TYPE mood;
REMOVE ACCESS acc ON DATABASE;
DEFINE ACCESS acc ON DATABASE TYPE RECORD SIGNUP (CREATE user SET m = <mood>'ok') AUTHENTICATE { <mood>'ok'; $auth };
REMOVE TYPE mood;
REMOVE ACCESS acc ON DATABASE;
REMOVE TYPE mood;
