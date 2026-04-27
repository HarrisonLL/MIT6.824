# KV Server Workflow

## Component Overview

```
┌─────────┐         ┌─────────────────────────────────────────────────────────────────┐         
│         │         │                        KV Server                                 │         
│ Client  │         │                                                                  │         
│         │         │  ┌───────────────────────────┐   ┌───────────────────────────┐  │         
│         │         │  │     RPC Handler goroutine  │   │   applyOpsLoop goroutine  │  │         
│         │──RPC───▶│  │  (one per client request)  │   │    (single background)    │  │         
│         │         │  │                            │   │                           │  │         
│         │         │  │ 1. build Op{}              │   │ 1. recv from applyCh      │  │         
│         │         │  │ 2. rf.Start(op) -> index   │   │ 2. cast msg.Command.(Op)  │  │         
│         │         │  │ 3. create notifyChans[idx] │   │ 3. Get: read kvStore      │  │         
│         │         │  │ 4. block on chan           │   │    Put/Append:            │  │         
│         │         │  │        ...waiting...       │   │      check dupTable       │  │         
│         │         │  │ 5. recv OpResult           │   │      apply to kvStore     │  │         
│         │         │  │ 6. check GetState()        │   │      update dupTable      │  │         
│         │         │  │ 7. delete notifyChans[idx] │   │ 4. notifyChans[idx]<-result│  │         
│         │◀─reply──│  └───────────┬───────────────┘   └─────────────┬─────────────┘  │         
│         │         │              │ notifyChans                      │                │         
│         │         │              └──────────────────────────────────┘                │         
└─────────┘         └──────────────────────┬──────────────────────────────────────────┘         
                                           │ rf.Start() / applyCh                               
                                    ┌──────┴──────┐                                             
                                    │    Raft     │                                             
                                    │             │                                             
                                    │  consensus  │                                             
                                    │  log repl.  │                                             
                                    └─────────────┘                                             
```

## Happy Path (Leader receives request)

```
Client                 KV Server (Leader)                    Raft                  KV Server (All)
  |                         |                                  |                         |
  |--- Get/PutAppend RPC -->|                                  |                         |
  |                         | Get/PutAppend builds Op{}        |                         |
  |                         | calls waitForRaft(op)            |                         |
  |                         |                                  |                         |
  |                         |-- rf.Start(op) ---------------->|                         |
  |                         |<- returns log index             |                         |
  |                         |                                  |                         |
  |                         |  create notifyChans[index]       |                         |
  |                         |  block on notifyChans[index]     |                         |
  |                         |        (waiting...)              |                         |
  |                         |                                  |-- replicate to majority |
  |                         |                                  |-- commit entry          |
  |                         |                                  |-- applyCh <- msg ------>|
  |                         |                                  |                         | applyOpsLoop:
  |                         |                                  |                         | 1. check CommandValid
  |                         |                                  |                         | 2. cast msg.Command.(Op)
  |                         |                                  |                         | 3. Get: read kvStore
  |                         |                                  |                         |    Put/Append: check dupTable
  |                         |                                  |                         |    if not duplicate, apply to kvStore
  |                         |                                  |                         |    update dupTable[clientId] = seqNum
  |                         |                                  |                         | 4. notifyChans[idx] <- OpResult
  |                         |                                  |                         |
  |                         |<-- notifyChans[index] unblocks --|-------------------------|
  |                         |    check rf.GetState() still leader                        |
  |                         |    delete notifyChans[index]                               |
  |                         |    return OpResult to Get/PutAppend                        |
  |<-- reply (Err+Value) ---|                                  |                         |
```

## Wrong Leader Path

```
Client                 KV Server (Follower)
  |                         |
  |--- Get/PutAppend RPC -->|
  |                         | waitForRaft: rf.Start() returns isLeader=false
  |                         | return OpResult{Err: ErrWrongLeader}
  |<-- ErrWrongLeader ------|
  |                         |
  | rotate leaderId and retry
```

## Timeout Path (Leader loses leadership after rf.Start)

```
Client                 KV Server (was Leader)
  |                         |
  |--- Get/PutAppend RPC -->|
  |                         | waitForRaft: rf.Start() -> index
  |                         | block on notifyChans[index]
  |                         |
  |                         |   ... leader loses election ...
  |                         |
  |                         |   500ms timeout fires
  |                         |   return OpResult{Err: ErrWrongLeader}
  |<-- ErrWrongLeader ------|
  |                         |
  | client rotates leaderId and retries
```

## Stale Leader Path (result received but no longer leader)

```
Client                 KV Server (was Leader)
  |                         |
  |--- Get/PutAppend RPC -->|
  |                         | waitForRaft: rf.Start() -> index
  |                         | block on notifyChans[index]
  |                         |
  |                         | notifyChans[index] receives result
  |                         | rf.GetState() -> isLeader=false
  |                         | override result.Err = ErrWrongLeader
  |<-- ErrWrongLeader ------|
  |                         |
  | client rotates leaderId and retries
```

## Duplicate Request Path (Put/Append only)

```
Client                 KV Server (Leader)
  |                         |
  |--- PutAppend (seq=5) -->| committed and applied, reply lost
  |                         |
  |--- PutAppend (seq=5) -->| waitForRaft: rf.Start() -> index
  |   (retry)               | block on notifyChans[index]
  |                         |
  |                         | applyOpsLoop: dupTable[clientId]=5 >= op.SeqNum=5
  |                         | skip applying to kvStore
  |                         | still signal notifyChans[idx] <- OpResult{Err: OK}
  |                         |
  |<-- reply (OK) ----------|
```

## Data Structures

```
KVServer
├── kvStore      map[string]string        key -> value (the actual state machine)
├── dupTable     map[int64]int64          clientId -> lastAppliedSeqNum (dedup for Put/Append)
├── notifyChans  map[int]chan OpResult     logIndex -> channel to signal waitForRaft
└── rf           *raft.Raft               the Raft instance underneath

Op
├── Type      string    "Get", "Put", "Append"
├── Key       string
├── Value     string
├── ClientId  int64
└── SeqNum    int64

OpResult
├── Err    Err      OK, ErrNoKey, ErrWrongLeader
└── Value  string   only populated for Get
```
