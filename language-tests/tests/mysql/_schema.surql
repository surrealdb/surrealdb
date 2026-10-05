/**
[test]
run = false
*/

CREATE users:1 SET user_id = 1, name = 'Alice', age = 30;
CREATE users:2 SET user_id = 2, name = 'Bob', age = 25;
CREATE users:3 SET user_id = 3, name = 'Charlie', age = 35;

CREATE orders:1 SET user_id = 1, total = 100;
CREATE orders:2 SET user_id = 1, total = 200;
CREATE orders:3 SET user_id = 2, total = 150;

CREATE products:1 SET category = 'electronics', price = 500;
CREATE products:2 SET category = 'electronics', price = 300;
CREATE products:3 SET category = 'books', price = 20;
