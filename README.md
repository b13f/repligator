# Repligator

[![Build Status](https://travis-ci.org/b13f/repligator.svg?branch=master)](https://travis-ci.org/b13f/repligator) [![Go Report Card](https://goreportcard.com/badge/github.com/b13f/repligator)](https://goreportcard.com/report/github.com/b13f/repligator) [![GitPitch](https://gitpitch.com/assets/badge.svg)](https://gitpitch.com/b13f/repligator?grs=github&t=black)

Repligator is a heterogeneous replication service heavily expired by [tungsten replicator](https://github.com/continuent/tungsten-replicator).
For now it support replication from MySQL to Vertica.

## Getting Started

### Purposes

Repligator is used for "no ETL" on your MySQL servers setup.

You can use your MySQL in your application while havily load statistic/analytics queries can be run in Vertica setup.

Or if you have sharded MySQL setup you can run queries in all of your data in Vertica.

### How it works
Repligator aggregate all MySQL replication events and periodically load it in Vertica.

MySQL MUST use GTID replication with full row binlog format.
Update events is used like delete then inserts.

Repligator support some DDL statements.

All events run in Vertica in transaction for easely restart replication process.

### Prerequisites
For run Repligator you need to install unixodbc and [Vertica ODBC driver](https://my.vertica.com/download/vertica/client-drivers/)

### Installing
1. build from source, clone this repo
    ```
    curl https://glide.sh/get | sh
    glide install
    go build
    ```
2. download latest release [here](https://github.com/b13f/repligator/releases/latest)
3. for ubuntu14.04 use https://packagecloud.io/b13f/repligator

## Usage
For your current setup in MySQL you should 

1. dump your db with --tab option of mysqldump.
2. run `repligator -df` on your MySQL dump folder.
3. execute result DDL in Vertica.
4. Load dumped data to Vertica.
5. Run Repligator with GTID position from dump. Config example at [config.yml](https://github.com/b13f/repligator/blob/master/builds/etc/repligator/config.sample.yml)

You can skip Not supported DDL statements throw web interface or Slack interface.

### Known issues
If you have massive updates or deletes Vertica may perform it slowly.

For some tables in Vertica 7.2 have issues for constraint rise error with delete and insert in one transactions.

For some tables in Vertica 7.2 have issues with very slow delete for few rows.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details

## Acknowledgments

* This couldn't be done without https://github.com/siddontang/go-mysql and https://github.com/alexbrainman/odbc