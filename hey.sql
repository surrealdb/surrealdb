SELECT * FROM $auth;

select * from account;

select * from item;

select * from unit WHERE item.account.alias = "charlie";
