/**
[test]
run = false
*/

-- Setup: define users at each scope with a password. The PASSHASH must not
-- be exfiltrable through INFO FOR by a low-privilege Viewer principal.
DEFINE USER root_secret ON ROOT PASSWORD 'r00t-pass' ROLES VIEWER;
USE NS test;
DEFINE USER ns_secret ON NAMESPACE PASSWORD 'ns-pass' ROLES VIEWER;
USE NS test DB test;
DEFINE USER db_secret ON DATABASE PASSWORD 'db-pass' ROLES VIEWER;
