/**
[bench]
reason = "Upsert 120 records by literal id, one statement each, in one transaction"
run = true
rebuild = true
warmup = "2s"
sample-size = 30
measurement-time = "20s"

[test]
run = false
*/

BEGIN TRANSACTION;
UPSERT type::record('record', ['s', 0]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 1]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 2]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 3]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 4]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 5]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 6]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 7]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 8]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 9]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 10]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 11]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 12]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 13]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 14]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 15]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 16]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 17]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 18]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 19]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 20]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 21]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 22]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 23]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 24]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 25]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 26]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 27]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 28]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 29]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 30]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 31]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 32]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 33]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 34]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 35]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 36]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 37]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 38]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 39]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 40]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 41]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 42]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 43]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 44]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 45]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 46]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 47]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 48]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 49]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 50]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 51]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 52]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 53]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 54]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 55]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 56]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 57]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 58]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 59]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 60]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 61]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 62]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 63]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 64]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 65]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 66]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 67]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 68]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 69]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 70]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 71]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 72]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 73]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 74]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 75]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 76]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 77]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 78]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 79]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 80]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 81]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 82]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 83]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 84]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 85]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 86]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 87]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 88]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 89]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 90]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 91]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 92]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 93]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 94]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 95]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 96]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 97]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 98]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 99]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 100]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 101]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 102]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 103]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 104]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 105]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 106]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 107]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 108]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 109]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 110]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 111]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 112]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 113]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 114]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 115]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 116]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 117]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 118]) CONTENT { a: 1 };
UPSERT type::record('record', ['s', 119]) CONTENT { a: 1 };
COMMIT TRANSACTION;
