# NOTES

The following instructions are for Ubuntu 20.04

## Setup

```shell
rustup upgrade
sudo apt-get -y update
sudo apt-get -y install -y cmake g++ libprotobuf-dev protobuf-compiler
cargo install cargo-deb
```

## Building

```shell
cargo deb
```

## Testing

```shell
sudo dpkg -i target/debian/surreal_1.0.0~beta.9_amd64.deb
```

## Installing

```shell
curl --proto '=https' --tlsv1.2 -sSf https://deb.surrealdb.com | sh
```

## Running

#### Start the service
```shell
$ sudo service surreal start
```

#### Stop the service
```shell
$ sudo service surreal stop
```

#### Enable the service
```shell
$ sudo service surreal enable
```

#### Stop the service
```shell
$ sudo service surreal disable
```

#### Get the service status
```shell
$ sudo service surreal status
```

Below is an example response

```shell
● surreal.service - SurrealDB Service
     Loaded: loaded (/lib/systemd/system/surreal.service; enabled; vendor preset: enabled)
     Active: active (running) since Thu 2022-08-11 23:34:35 UTC; 5min ago
   Main PID: 23177 (surreal)
      Tasks: 5 (limit: 4605)
     Memory: 3.2M
     CGroup: /system.slice/surreal.service
             └─23177 /usr/share/surreal/surreal start --log info --user root --pass root
```

#### View service logs

```shell
sudo journalctl -f -u surreal
```
