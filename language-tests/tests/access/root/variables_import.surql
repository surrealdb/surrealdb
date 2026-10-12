/**
[env]
clean = true # Requires clean as we define data on the global root.
remote-skip = "defines a root user named 'root', which collides with the bootstrap root user the remote server must run with for the harness to authenticate"

[test]

[[test.results]]
value = "NONE"

*/
DEFINE USER root ON ROOT PASSWORD "root" ROLES OWNER;
