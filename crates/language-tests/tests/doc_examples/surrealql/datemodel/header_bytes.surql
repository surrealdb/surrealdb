/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = '[{ id: foo:ve4sc2rmebi9bpa4ac4m, value: encoding::base64::decode("YmFy") }]'
rough = true

*/
-- Define a field with a single type
DEFINE FIELD image ON TABLE product TYPE bytes;

-- Create a record with a bytes field and set the value
CREATE foo SET value = <bytes>"bar";
