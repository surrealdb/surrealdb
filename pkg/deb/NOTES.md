# NOTES

used ubuntu 20.04 system

## Pre Requisites

```shell
$ rustup upgrade
$ sudo apt-get update && sudo apt-get install -y cmake g++ libprotobuf-dev protobuf-compiler
$ cargo install cargo-deb
```

## Optinal test build SurrealDB

```shell
$ make build
```

## Make Debian

```shell
$ make deb
# or 
$ cargo deb
# outcome
...
info: Generating maintainer script postrm
info: compressed/original ratio 6274596/20791808 (30%)
target/debian/surreal_1.0.0~beta.5_amd64.deb
```

## Install local deb

```shell
$ sudo dpkg -i target/debian/surreal_1.0.0~beta.5_amd64.deb 
```

## Some Service 

```shell
$ sudo service surreal status
# outcome
● surreal.service - SurrealDB Service
     Loaded: loaded (/lib/systemd/system/surreal.service; enabled; vendor preset: enabled)
     Active: active (running) since Thu 2022-08-11 23:34:35 UTC; 5min ago
   Main PID: 23177 (surreal)
      Tasks: 5 (limit: 4605)
     Memory: 3.2M
     CGroup: /system.slice/surreal.service
             └─23177 /usr/share/surreal/surreal start --log trace --user root --pass root
# other commands
$ sudo service surreal start
$ sudo service surreal stop
$ sudo service surreal enable
$ sudo service surreal disable
```

## Test Service

```shell
$ curl -k -L -s POST \
  --header "Content-Type: application/json" \
  --header 'NS: test' \
  --header 'DB: test' \
  --user "root:root" \
  --data "INFO FOR DB" \
  http://localhost:8000/sql
# outcome
[
  {
    "time": "75.653µs",
    "status": "OK",
    "result": {
      "dl": {},
      "dt": {},
      "sc": {},
      "tb": {
        "account": "DEFINE TABLE account SCHEMALESS PERMISSIONS NONE"
      }
    }
  }
]
```

## See logs with JournalCtl

```shell
$ sudo journalctl -f -u surreal
# or with log
$ tail -f /var/log/surrealdb.log
```

## Install online deb

```shell
$ curl --proto '=https' --tlsv1.2 -sSf https://deb.surrealdb.com | sh
```

upload `install.sh` to <https://download.surrealdb.com/debian/install.sh>

contents of `install.sh`

```shell
curl https://download.surrealdb.com/debian/surrealdb-latest.darwin-amd64.deb -o /tmp/surrealdb-latest.darwin-amd64.deb \
  && sudo dpkg -i /tmp/surrealdb-latest.darwin-amd64.deb
```

> always create a symbolic link to latest version, or replace surrealdb-latest.darwin-amd64.deb, this way latest will have always the same filename ex 

```shell
$ ln -s surrealdb-v1.0.0-beta.5.darwin-amd64.deb surrealdb-latest.darwin-amd64.deb
```

## Build debian for Rasperry Pi

```shell
# preRequisites
$ sudo apt install gcc-arm-linux-gnueabihf
# check if armv7-unknown-linux-gnueabihf is installed
$ rustup component list | grep armv7-unknown-linux-gnueabihf
# install target armv7-unknown-linux-gnueabihf
$ rustup target add armv7-unknown-linux-gnueabihf
# check if armv7-unknown-linux-gnueabihf is installed
$ rustup component list | grep armv7-unknown-linux-gnueabihf
rust-std-armv7-unknown-linux-gnueabihf (installed)
# now make debian with
$ make debRpi
14 | include!(concat!("bindings/", bindings_env!("TARGET"), ".rs"));
   | ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
   |
   = note: this error originates in the macro `include` (in Nightly builds, run with -Z macro-backtrace for more info)

error: could not compile `rquickjs-sys` due to previous error
```
