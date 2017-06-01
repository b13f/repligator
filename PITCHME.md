## repligator
#### <span style="font-family:Arial;">replication from MySQL to Vertica</span>

---

##### If this is not about you, repligator will not help you

- You have several MySQL servers (perhaps from microservices) with tables of more than 100 million rows
- You are not using Kafka or other data pipelines
- You want to execute analytical requests or collect statistics
- Maybe you have a slave server where you launch these requests and wait hours for results
- You do not have an ETL process

---

## Heterogeneous replication from a relational DBMS in a data warehouse

With repligator, you can replicate all your data from MySQL to [Vertica](https://my.vertica.com/docs/8.1.x/HTML/index.htm#Authoring/ConceptsGuide/Other/ConceptsGuide.htm%3FTocPath%3DVertica%2520Concepts%7C_____0)
, and you do not need any ETL processes. 

Repligator supports both data replication and data schema modification events.

---

## Required

- MySQL master server(s) with enabled GTID replication
- ODBC Vertica driver installed
- Vertica version 7.2-8.1

---

## Install

- Compilation from source codes
- Download the latest release from [the Releases page](https://github.com/b13f/repligator/releases/latest)
- Use a [repository](https://packagecloud.io/b13f/repligator) for Ubuntu 14.04.
- Use Docker image from https://hub.docker.com/r/b13f/repligator/

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

## Operation

You can view cache status through a web interface http://yourrepligator.host:8080/info

When Repligator encounters a DDL event that it cannot execute, it stops the replication process and waits for the operator’s decision. To continue replication, you can use the web interface http://yourrepligator.host:8080/skip or send the `Skip` command to Slack chat bot.