# rgkv
rgkv is a distributed kv storage service using raft consensus algorithm.  
+ *Get/put/append operation*
+ *High Availability*
+ *Sharding*
+ *Linearizability*

## Table of Content
+ [Test Case](https://github.com/LutongZhang/rgkv#Test-Case)
+ [Architecture](https://github.com/LutongZhang/rgkv#Architecture)
  + [shard controller](https://github.com/LutongZhang/rgkv#shard-controller)
  + [shardkv server](https://github.com/LutongZhang/rgkv#shardkv-server)
  + [shards movement](https://github.com/LutongZhang/rgkv#shards-movement)
+ [Raft](https://github.com/LutongZhang/rgkv#Raft)
+ [Linearizability](https://github.com/LutongZhang/rgkv#Linearizability)

## Test Case
   Try to use go test to run test case
+ raft test (src/raft/test_test.go)
  ```
  cd src/raft/
  go test -run <test case>
  ```
+ shardctrler test (src/shardctrler/test_test.go)
  ```
  cd src/shardctrler/
  go test -run <test case>
  ```
+ shardkv test (src/shardkv/test_test.go)
  ```
  cd src/shardkv/
  go test -run <test case>
  ```
## Architecture
![rgkv architecture](https://github.com/LutongZhang/rgkv/blob/main/diagrams/rgkv.png)
+ ### shard controller
  The shardctrler manages a sequence of numbered configurations. Each configuration describes a set of replica groups and an assignment of shards to replica groups. 
  Shard controller use raft to prevent single-point failure.
  
  Support API:
  + Query: Retrieve config with a config number. if config number is -1, get the latest config
  + Join: Add a new replica group
  + Leave: Leave a list of replica groups 
  + Move: assign a shard to a certain replica group

+ ### shardkv server
  Each shardkv server operates as part of a raft replica group. Each replica group serves Get/Put/Append API for some of the shards. 
  
  Support API:
  + Get
  + Put
  + Apppend

+ ### shards movement
  rgkv uses a way similar to two phases commit to implement shards movement logic. It ensures correct movement logic, garbage collection for stale shards, and uninterrupted service during movement.
  + phase 1: shard controller sends prepare request to kv servers to get shards they need and install shards by sending it to raft log
  + phase 2: shard controller sends commit request to kv servers to garbage collect stale shards. 
  
  *each shard carry a config number to prevent tasks that have already been executed from being repeated
  
## Raft
   [raft](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf) is a distributed consensus algorithm, which is easy to understand. The raft library in rgkv supported: 
   + Leader election
   + Log
   + Persistence
   + Snapshot

   ### design diagram 

   ![raft diagram](https://github.com/LutongZhang/rgkv/blob/main/diagrams/raft.png)
     
## Linearizability
Linearizability ensured by:
   + Read/get/append operations order is determined by raft log
   + Duplicate client request detection mechanism (but each client can only send req sequentially, wait for optimize)