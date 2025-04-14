/**
[test]

[[test.results]]
value = "d'-262143-01-01T00:00:00Z'"

[[test.results]]
value = "Incorrect arguments for function time::from::unix(). The argument must be a number of seconds relative to January 1, 1970 0:00:00 UTC that produces a datetime between d'-262143-01-01T00:00:00Z' and d'+262142-12-31T23:59:59Z'."

[[test.results]]
value = "d'+262142-12-31T23:59:59Z'"

[[test.results]]
value = ""Incorrect arguments for function time::from::unix(). The argument must be a number of seconds relative to January 1, 1970 0:00:00 UTC that produces a datetime between d'-262143-01-01T00:00:00Z' and d'+262142-12-31T23:59:59Z'.""

*/

# Note: this function is identical to time::from::unix
time::from::secs(-8334601228800);
time::from::secs(-8334601228801);
time::from::secs(8210266876799);
time::from::secs(8210266876800);