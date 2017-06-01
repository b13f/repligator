# Repligator

[![Build Status](https://travis-ci.org/b13f/repligator.svg?branch=master)](https://travis-ci.org/b13f/repligator) [![Go Report Card](https://goreportcard.com/badge/github.com/b13f/repligator)](https://goreportcard.com/report/github.com/b13f/repligator) [![GitPitch](https://gitpitch.com/assets/badge.svg)](https://gitpitch.com/b13f/repligator?grs=github&t=black)

Repligator is a heterogeneous replication service. The idea for this service came from [tungsten replicator](https://github.com/continuent/tungsten-replicator).
Currently it supports replication from MySQL to Vertica.

## Getting Started

### Purposes

Repligator works best if you don’t already have ETL processes set up for your MySQL servers. 

With Repligator, you can use MySQL in your own application but launch all requests for statistics or analytics for the same data already in Vertica.

Or, if you have sharded data in MySQL, you can gather it all in a single place – Vertica.

### How it works
Repligator aggregates all MySQL replication events and loads them to Vertica with the frequency indicated in the config.

MySQL replication MUST use a GTID replication with full_row binlog format.
Update events are used in Vertica as delete, then insert.


Repligator supports some DDL statements: table creation, deletion, renaming, and some ALTER statements.

All changes in Vertica take place in a transaction, if Repligator stops, the consistency of data in Vertica is not impaired.

### Prerequisites
Running Repligator requires the unixodbc and [Vertica ODBC driver](https://my.vertica.com/download/vertica/client-drivers/) installed in the system.

### Installation
1. Compilation from source codes
    ```
    curl https://glide.sh/get | sh
    glide install
    go build
    ```
2. Download the latest release from [the Releases page](https://github.com/b13f/repligator/releases/latest)
3. Use a [repository](https://packagecloud.io/b13f/repligator) for Ubuntu 14.04.
4. Use Docker image from https://hub.docker.com/r/b13f/repligator/

## Usage
1. Dump your databases with the `--tab` option to `mysqldump`. Save the GTID from stdout.
2. Launch the `repligator -df`, indicating the folder with *.sql files.
3. Execute the resulting DDL in Vertica.
4. Download the data from the dump to Vertica using COPY.
5. Launch repligator by entering the GTID from the dump in the config. See the config details [here](https://github.com/b13f/repligator/blob/master/builds/etc/repligator/config.sample.yml)

You can skip Not supported DDL statements throw web interface or Slack interface.

### Known issues
If you have massive update or delete requests for tens of thousands of lines, Vertica may process these requests very slowly.

For some tables in Vertica 7.2, deletion and insertion in one transaction caused a unique key constraint error.

For some tables in Vertica 7.2, deleting even a few lines took a very long time. It’s better to add these tables to exceptions. This is detected in the following way: repligator has not been updating data in Vertica for a very long time. Check all processes in Vertica and find the request that is hanging from repligator.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details

## Acknowledgments

* This couldn't be done without https://github.com/siddontang/go-mysql and https://github.com/alexbrainman/odbc