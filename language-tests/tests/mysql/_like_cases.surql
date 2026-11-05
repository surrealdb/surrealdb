/**
[test]
run = false
reason = "Setup only: subjects and the LIKE patterns they are matched with, read back by the like_runtime_patterns and like_literal_patterns tests of both SQL dialects."
*/

CREATE like_cases:1 SET subject = 'Alice', pattern = 'A%' RETURN NONE;
CREATE like_cases:2 SET subject = 'Bob', pattern = 'A%' RETURN NONE;
CREATE like_cases:3 SET subject = 'Alice', pattern = '_lice' RETURN NONE;
CREATE like_cases:4 SET subject = 'Alice', pattern = '%lic%' RETURN NONE;
CREATE like_cases:5 SET subject = 'Alice', pattern = 'alice' RETURN NONE;
CREATE like_cases:6 SET subject = '50%', pattern = '50\\%' RETURN NONE;
CREATE like_cases:7 SET subject = '500', pattern = '50\\%' RETURN NONE;
CREATE like_cases:8 SET subject = 'a_b', pattern = 'a\\_b' RETURN NONE;
CREATE like_cases:9 SET subject = 'axb', pattern = 'a\\_b' RETURN NONE;
CREATE like_cases:10 SET subject = 'a\\b', pattern = 'a\\\\b' RETURN NONE;
CREATE like_cases:11 SET subject = 'a\\xyz', pattern = 'a\\\\%' RETURN NONE;
CREATE like_cases:12 SET subject = 'axyz', pattern = 'a\\\\%' RETURN NONE;
CREATE like_cases:13 SET subject = 'axb', pattern = 'a\\xb' RETURN NONE;
CREATE like_cases:14 SET subject = 'a\\xb', pattern = 'a\\xb' RETURN NONE;
CREATE like_cases:15 SET subject = 'abc', pattern = 'a.c' RETURN NONE;
CREATE like_cases:16 SET subject = 'a.c', pattern = 'a.c' RETURN NONE;
CREATE like_cases:17 SET subject = 'aab', pattern = 'a+b' RETURN NONE;
CREATE like_cases:18 SET subject = 'a+b', pattern = 'a+b' RETURN NONE;
CREATE like_cases:19 SET subject = '(x)', pattern = '(x)' RETURN NONE;
CREATE like_cases:20 SET subject = 'x', pattern = '(x)' RETURN NONE;
CREATE like_cases:21 SET subject = '[x]', pattern = '[x]' RETURN NONE;
CREATE like_cases:22 SET subject = 'x', pattern = '[x]' RETURN NONE;
CREATE like_cases:23 SET subject = 'a{1}', pattern = 'a{1}' RETURN NONE;
CREATE like_cases:24 SET subject = 'a', pattern = 'a{1}' RETURN NONE;
CREATE like_cases:25 SET subject = '^x$', pattern = '^x$' RETURN NONE;
CREATE like_cases:26 SET subject = 'x', pattern = '^x$' RETURN NONE;
CREATE like_cases:27 SET subject = 'a|b', pattern = 'a|b' RETURN NONE;
CREATE like_cases:28 SET subject = 'a', pattern = 'a|b' RETURN NONE;
CREATE like_cases:29 SET subject = 'a*', pattern = 'a*' RETURN NONE;
CREATE like_cases:30 SET subject = 'aa', pattern = 'a*' RETURN NONE;
CREATE like_cases:31 SET subject = 'a?', pattern = 'a?' RETURN NONE;
CREATE like_cases:32 SET subject = 'a', pattern = 'a?' RETURN NONE;
CREATE like_cases:33 SET subject = 'a\nb', pattern = 'a%b' RETURN NONE;
CREATE like_cases:34 SET subject = 'a\nb', pattern = 'a_b' RETURN NONE;
CREATE like_cases:35 SET subject = 'a\n', pattern = 'a' RETURN NONE;
CREATE like_cases:36 SET subject = 'é', pattern = '_' RETURN NONE;
CREATE like_cases:37 SET subject = 'éa', pattern = '_a' RETURN NONE;
CREATE like_cases:38 SET subject = 'ab', pattern = '_' RETURN NONE;
CREATE like_cases:39 SET subject = '', pattern = '' RETURN NONE;
CREATE like_cases:40 SET subject = '', pattern = '%' RETURN NONE;
CREATE like_cases:41 SET subject = 'x', pattern = '' RETURN NONE;
CREATE like_cases:42 SET subject = 'a', pattern = 'a ' RETURN NONE;
CREATE like_cases:43 SET subject = '日本語', pattern = '日_語' RETURN NONE;
CREATE like_cases:44 SET subject = '日本語', pattern = '日%' RETURN NONE;
CREATE like_cases:45 SET subject = 'a\\', pattern = 'a\\\\' RETURN NONE;
CREATE like_cases:46 SET subject = '\\', pattern = '\\\\' RETURN NONE;
CREATE like_cases:48 SET subject = '%', pattern = '\\%' RETURN NONE;
CREATE like_cases:49 SET subject = '_', pattern = '\\_' RETURN NONE;
CREATE like_cases:50 SET subject = 'a', pattern = '\\a' RETURN NONE;
CREATE like_cases:51 SET subject = '\\', pattern = '\\\\\\\\' RETURN NONE;
CREATE like_cases:52 SET subject = '\\\\', pattern = '\\\\\\\\' RETURN NONE;
CREATE like_cases:53 SET subject = 'a\\\\%', pattern = 'a\\\\\\\\\\%' RETURN NONE;
CREATE like_cases:54 SET subject = 'a_', pattern = 'a\\_' RETURN NONE;
CREATE like_cases:55 SET subject = 'ab', pattern = 'a\\_' RETURN NONE;
CREATE like_cases:56 SET subject = 'abc', pattern = '%' RETURN NONE;
CREATE like_cases:57 SET subject = 'a%c', pattern = 'a\\%%' RETURN NONE;
CREATE like_cases:58 SET subject = 'A', pattern = 'a' RETURN NONE;
CREATE like_cases:100 SET subject = 'x', pattern = NULL RETURN NONE;
CREATE like_cases:101 SET subject = NULL, pattern = 'x' RETURN NONE;
