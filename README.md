# Surreal

Surreal is a scalable, distributed, strongly-consistent, collaborative document-graph database.

[![](https://img.shields.io/circleci/token/adb5ca379a334a4011fa894275c312fe35833d6d/project/abcum/surreal/master.svg?style=flat-square)](https://circleci.com/gh/abcum/surreal) [![](https://img.shields.io/badge/status-alpha-ff00bb.svg?style=flat-square)](https://github.com/abcum/surreal) [![](https://img.shields.io/badge/godoc-reference-blue.svg?style=flat-square)](https://godoc.org/github.com/abcum/surreal) [![](https://goreportcard.com/badge/github.com/abcum/surreal?style=flat-square)](https://goreportcard.com/report/github.com/abcum/surreal) [![](https://img.shields.io/badge/license-Apache_License_2.0-00bfff.svg?style=flat-square)](https://github.com/abcum/surreal) 

#### Features

- NoSQL document-graph database written in [Go](http://golang.org)
- Administrative **database tools**
	- Easily import data into a cluster
	- Easily export data from a cluster
	- Accessible and intuitive web interface
- Multiple **connection methods**
	- Connect using REST
	- Connect using JSON-RPC
	- Connect using Websockets
- Multiple **data querying** methods
	- Use advanced SQL queries
	- Query using REST url endpoints
	- Query using Websocket methods
- Customisable **authentication** access
	- Specify public or private access
	- Admin access to all database data
	- Token access to all database data
	- End-user multi-tenancy authentication
- Flexible **data manipulation** queries
	- Automatic creation of tables
	- Schema-less or schema-full tables
	- Automatic data field sanitization
	- Mandatory, readonly, and validated data fields
	- Define embedded fields, and object arrays
- Advanced customisable **indexing** support
	- Single-column indexes
    - Multiple-column indexes
    - Multiple-compound indexes
	- Indexing of embedded data fields
	- JS/LUA scripting for custom indexes
	- Full-text indexing of all data by default
- **Collaborative** editing and manipulation of data
	- Live realtime queries
	- Publish data changes
	- Subscribe to data changes
	- Built-in concurrency control
	- Pub/sub over websocket for data updates
- **Encryption** out-of-the-box as standard
	- End-to-end intra-cluster communications
	- End-user SSL encryption for http endpoints
	- Encryption of all data at rest using AES-256

#### Installation

```bash
go get github.com/abcum/surreal
```

#### Running

```bash
surreal start --port-web 8000
```

#### Clustering

```bash
surreal start --port-web 8000 --port-tcp 33693 --db-path file://surreal-1.db --join localhost:33693 --log-level debug
surreal start --port-web 8001 --port-tcp 33694 --db-path file://surreal-2.db --join localhost:33693 --log-level debug
surreal start --port-web 8002 --port-tcp 33695 --db-path file://surreal-3.db --join localhost:33693 --log-level debug
```

#### Deployment

```bash
docker run --name surreal-1 abcum/surreal start --port-web 8000 --port-tcp 33693 --join localhost:33693 --log-level debug
docker run --name surreal-2 abcum/surreal start --port-web 8001 --port-tcp 33694 --join localhost:33693 --log-level debug
docker run --name surreal-3 abcum/surreal start --port-web 8002 --port-tcp 33695 --join localhost:33693 --log-level debug
```
