# Security

This document describes how authentication and user access works for accessing the database.

#### Authentication levels

** ROOT authentication **

- Signin with root email and password, set at server initialisation
- Can create, select, delete all namespaces
- Can create, select, delete all databases
- Can create, select, delete all tables
- Can create, select, delete all data
- Not restricted by any Table or Field permissions

** NS authentcation **

- Signin with NS, email, and password, which must exist as a NAMESPACE USER
- Can create, select, delete any database under the NS
- Can create, select, delete any tables under the NS
- Can create, select, delete any data under the NS
- Not restricted by any Table or Field permissions

** DB authentication **

- Signup
- Signin with NS, DB, email, and password, which must exist as a DATABASE USER
- Can create, select, delete any tables under the DB
- Can create, select, delete any data under the DB
- Not restricted by any Table or Field permissions

** SC authentcation **

- Signup with NS, DB, SC, email, and password, which must successfully pass an scope SIGNUP clause
- Signin with NS, DB, SC, email, and password, which must successfully pass an scope SIGNIN clause
- Can create, select, delete any data under the DB, as long as permission match
- Restricted by any Table or Field permissions

#### Database signup

** SC signup **

- POST an HTTP FORM or JSON to /signin

	```json
	{
		"NS": "abcum", 
		"DB": "acreon", 
		"SC": "account", 
		"user": "user@example.com", 
		"pass": "123456"
	}
	```

- Receive a HTTP 200 code from server

#### Database signin

** ROOT signin **

- Use HTTP Basic Auth specifying username:password with each request

	```HTTP
	POST /sql HTTP/1.1
	Host: localhost:8000
	Content-Type: application/json
	Authorization: Basic cm9vdDpyb290
	```

** NS signin **

- POST an HTTP FORM or JSON to /signin

	```json
	{
		"NS": "abcum", 
		"user": "user@example.com", 
		"pass": "123456"
	}
	```

- Receive a JSON Web Token from the server

- Use the JSON Web Token to authenticate requests

	```HTTP
	POST /sql HTTP/1.1
	Host: localhost:8000
	Content-Type: application/json
	Authorization: Bearer eyJhbGciOiIkpXVCJ9.eyJEQiI6ImFiY30Nzk3Mzc2NDh9.RMVkex6OpHPZY1BQIQKlQ
	```

** DB signin **

- POST an HTTP FORM or JSON to /signin

	```json
	{
		"NS": "abcum", 
		"DB": "acreon", 
		"user": "user@example.com", 
		"pass": "123456"
	}
	```

- Receive a JSON Web Token from the server

- Use the JSON Web Token to authenticate requests

	```HTTP
	POST /sql HTTP/1.1
	Host: localhost:8000
	Content-Type: application/json
	Authorization: Bearer eyJhbGciOiIkpXVCJ9.eyJEQiI6ImFiY30Nzk3Mzc2NDh9.RMVkex6OpHPZY1BQIQKlQ
	```

** SC signin **

- POST an HTTP FORM or JSON to /signup

	```json
	{
		"NS": "abcum", 
		"DB": "acreon", 
		"SC": "account", 
		"user": "user@example.com", 
		"pass": "123456"
	}
	```

- Receive a JSON Web Token from the server

- Use the JSON Web Token to authenticate requests

	```HTTP
	POST /sql HTTP/1.1
	Host: localhost:8000
	Content-Type: application/json
	Authorization: Bearer eyJhbGciOiIkpXVCJ9.eyJEQiI6ImFiY30Nzk3Mzc2NDh9.RMVkex6OpHPZY1BQIQKlQ
	```
