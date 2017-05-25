## Repligator
#### <span style="font-family:Arial;">A  Repligator Tour</span>

---

## If this not about you, you may leave

- You have many MySQL servers (microservices?) with tables >100m rows.
- You not use Kafka or some other data pipelines.
- You wanna do some queries for statistic or analytic purposes.
- You have slaves where you run this queries and wait for hours to get result.
- You not use ETL yet.

---

## Heterogeneous replication to data warehouse

With Repligator you can replicate all your MySQl data to [Vertica](https://my.vertica.com/docs/8.1.x/HTML/index.htm#Authoring/ConceptsGuide/Other/ConceptsGuide.htm%3FTocPath%3DVertica%2520Concepts%7C_____0)
<br>
And you don't need ETL processes
---

## Requirements

- MySQL servers with GTID replication enabled
- Vertica ODBC driver installed
- Vertica 7.2-8.1

---

## Install

- build from source https://github.com/b13f/repligator
- download latest release from https://github.com/b13f/repligator/releases/latest
- for ubuntu14.04 use https://packagecloud.io/b13f/repligator

---
### Configure
configure MySQL sources
```
sources:
  - name: shard1 #unique in this config
    type: mysql
    server_id: 101 #unique in your mysql setup
    host: 192.168.0.1
    port: 3306
    user: root
    password:
    timeout: 10000 #in seconds
    try_after: 2 #in minutes
    gtid: ccffeb16-0b05-11e7-852a-080027c2ddae:1-2
    schemas: # when exists apply rows event only in schemas
     - name: testing # when exists apply only rows event only in schema, ddl for all
       sync:
         - test # when exists apply only rows events for this tables. exclude not use
       exclude: # when exists not apply rows events for this tables
         - balance_oou
         - balance_demo_oou
       gtid: ccffeb16-0b05-11e7-852a-080027c2ddae:1-6 # you can set gtidset per schema, this schema start sync rows(!) events from this position
```
+++
configure Vertica destination
```
destination:
  odbc: Vertica
  host: 192.168.50.85
  port: 5433
  user: dbadmin
  password: password
  database: main
  pack: 10000
  flush_count: 200000
  flush_time: 120 #seconds
  data_dir: /opt/repligator/data
```
+++
configure repligator
```
port: 8080
log_file: /var/log/repligator/repligator.log
log_level: debug #panic fatal error warn info debug
slack:
  bot_token:
  hook:
  channel: "#repligator"
  username: repligator
  icon: ":crocodile:"
```
---

## Run

you can see current cache state at http://repligator.host:8080/info

To skip not supported DDL you can use http://repligator.host:8080/skip or chat bot command `skip`