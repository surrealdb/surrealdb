/**
[test]
reason = "Ensure that type::try_* functions actually do what they need to"

# try_int
[[test.results]] # 0
value = "123"
[[test.results]] # 1
value = "NONE"
[[test.results]] # 2
value = "NONE"
[[test.results]] # 3
error = "Incorrect arguments for function type::try_int(). Expected 1 argument"

# try_float
[[test.results]] # 4
value = "123f"
[[test.results]] # 5
value = "123f"
[[test.results]] # 6
value = "NONE"
[[test.results]] # 7
error = "Incorrect arguments for function type::try_float(). Expected 1 argument"

# try_string
[[test.results]] # 8
value = "\"['a', 'b']\""
[[test.results]] # 9
value = "'123'"
[[test.results]] # 10
error = "Incorrect arguments for function type::try_string(). Expected 1 argument"

# try_string_lossy
[[test.results]] # 11
value = "\"['a', 'b']\""
[[test.results]] # 12
value = "'123'"
[[test.results]] # 13
error = "Incorrect arguments for function type::try_string_lossy(). Expected 1 argument"

# try_array
[[test.results]] # 14
value = "NONE"
[[test.results]] # 15
value = "['123']"
[[test.results]] # 16
error = "Incorrect arguments for function type::try_array(). Expected 1 argument"

# try_bool
[[test.results]] # 17
value = "NONE"
[[test.results]] # 18
value = "true"
[[test.results]] # 19
value = "true"
[[test.results]] # 20
value = "false"
[[test.results]] # 21
error = "Incorrect arguments for function type::try_bool(). Expected 1 argument"

# try_bytes
[[test.results]] # 22
value = "b\"6869\""
[[test.results]] # 23
value = "NONE"
[[test.results]] # 24
value = "b\"01\""
[[test.results]] # 25
error = "Incorrect arguments for function type::try_bytes(). Expected 1 argument"

# try_datetime
[[test.results]] # 26
value = "NONE"
[[test.results]] # 27
value = "d'2018-08-08T00:00:00Z'"
[[test.results]] # 28
error = "Incorrect arguments for function type::try_datetime(). Expected 1 argument"

# try_decimal
[[test.results]] # 29
value = "NONE"
[[test.results]] # 30
value = "6123.634dec"
[[test.results]] # 31
error = "Incorrect arguments for function type::try_decimal(). Expected 1 argument"

# try_duration
[[test.results]] # 32
value = "2d"
[[test.results]] # 33
value = "NONE"
[[test.results]] # 34
error = "Incorrect arguments for function type::try_duration(). Expected 1 argument"

# try_number
[[test.results]] # 35
value = "NONE"
[[test.results]] # 36
value = "36"
[[test.results]] # 37
value = "36.23f"
[[test.results]] # 38
error = "Incorrect arguments for function type::try_number(). Expected 1 argument"

# try_point
[[test.results]] # 39
value = "NONE"
[[test.results]] # 40
value = "(1f, 2f)"
[[test.results]] # 41
value = "(1f, 2f)"
[[test.results]] # 42
error = "Incorrect arguments for function type::try_point(). Expected 1 argument"

# try_table
[[test.results]] # 43
value = "NONE"
[[test.results]] # 44
value = "'table'" # Unfortunately, I could not bork it to work
[[test.results]] # 45
error = "Incorrect arguments for function type::try_table(). Expected 1 argument"

# try_range
[[test.results]] # 46
value = "NONE"
[[test.results]] # 47
value = "2..3"
[[test.results]] # 48
error = "Incorrect arguments for function type::try_range(). Expected 1 argument"

# try_record
[[test.results]] # 49
value = "NONE"
[[test.results]] # 50
value = "NONE"
[[test.results]] # 51
value = "a:a"
[[test.results]] # 52
value = "a:a"
[[test.results]] # 53
error = "Incorrect arguments for function type::try_record(). Expected 1 to 2 arguments"

# try_set
[[test.results]] # 54
value = "NONE"
[[test.results]] # 55
value = "{'a', 'b'}"
[[test.results]] # 56
value = "{'a', 'b'}"
[[test.results]] # 57
value = "NONE"
[[test.results]] # 58
error = "Incorrect arguments for function type::try_set(). Expected 1 argument"

# try_uuid
[[test.results]] # 59
value = "NONE"
[[test.results]] # 60
value = "u'019d1b17-f391-7fb0-b190-006ff8c745ce'"
[[test.results]] # 61
error = "Incorrect arguments for function type::try_uuid(). Expected 1 argument"

# try_geometry
# What do I even pass in it?
*/

RETURN type::try_int('123');
RETURN type::try_int('123.2');
RETURN type::try_int('abc');
RETURN type::try_int(123, 'NO');

RETURN type::try_float('123');
RETURN type::try_float(123);
RETURN type::try_float('abc');
RETURN type::try_float('abc', 'NAH');

RETURN type::try_string(['a', 'b']);
RETURN type::try_string(123);
RETURN type::try_string(123, 'NADA');

RETURN type::try_string_lossy(['a', 'b']);
RETURN type::try_string_lossy(123);
RETURN type::try_string_lossy(123, 'NANI');

RETURN type::try_array(123);
RETURN type::try_array(['123']);
RETURN type::try_array(123, 'NOPE');

RETURN type::try_bool(123);
RETURN type::try_bool(true);
RETURN type::try_bool('true');
RETURN type::try_bool('false');
RETURN type::try_bool(123, 'NEIN');

RETURN type::try_bytes('hi');
RETURN type::try_bytes(10);
RETURN type::try_bytes([1]);
RETURN type::try_bytes([1], 'NAY');

RETURN type::try_datetime('asd');
RETURN type::try_datetime('2018-08-08');
RETURN type::try_datetime([1], 'NON');

RETURN type::try_decimal('asd');
RETURN type::try_decimal('6123.634');
RETURN type::try_decimal('6123.634', 'NAO');

RETURN type::try_duration('2d');
RETURN type::try_duration('2');
RETURN type::try_duration('2d', 'NEE');

RETURN type::try_number('a');
RETURN type::try_number('36');
RETURN type::try_number('36.23');
RETURN type::try_number('36', 'NA');

RETURN type::try_point(2);
RETURN type::try_point((1f, 2f));
RETURN type::try_point([1f, 2f]);
RETURN type::try_point([1f, 2f], 'JO');

RETURN type::try_table(2);
RETURN type::of(type::try_table('hi'));
RETURN type::try_table('hi', 'EI');

RETURN type::try_range('2');
RETURN type::try_range([2, 3]);
RETURN type::try_range([2, 3], 'NEM');

RETURN type::try_record(2);
RETURN type::try_record('a');
RETURN type::try_record('a:a');
RETURN type::try_record('a', 'a');
RETURN type::try_record('a', 'a', 'a');

RETURN type::try_set('abc');
RETURN type::try_set(['a', 'b']);
RETURN type::try_set({'a', 'b'});
RETURN type::try_set('{\"a\", \"b\"}');
RETURN type::try_set(['a', 'b'], 'TIDAK');

RETURN type::try_uuid('123');
RETURN type::try_uuid('019d1b17-f391-7fb0-b190-006ff8c745ce');
RETURN type::try_uuid('019d1b17-f391-7fb0-b190-006ff8c745ce', 'LE');
