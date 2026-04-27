package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     string
	Key      string
	Value    string
	ClientId int64
	SeqNum   int64
}

type OpResult struct {
	Err   Err
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStore     map[string]string
	dupTable    map[int64]int64 // clientId -> sequNum
	notifyChans map[int]chan OpResult
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Type:     "Get",
		Key:      args.Key,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}
	result := kv.waitForRaft(op)
	reply.Err = result.Err
	reply.Value = result.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}
	result := kv.waitForRaft(op)
	reply.Err = result.Err

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) waitForRaft(op Op) OpResult {
	var result OpResult
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		result.Err = ErrWrongLeader
		return result
	}
	kv.mu.Lock()
	ch, ok := kv.notifyChans[index]
	if !ok {
		ch = make(chan OpResult, 1)
		kv.notifyChans[index] = ch
	}
	kv.mu.Unlock()

	select {
	case result = <-ch:
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			result.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		result.Err = ErrWrongLeader
	}
	kv.mu.Lock()
	delete(kv.notifyChans, index)
	kv.mu.Unlock()
	return result
}

// === Snapshot ===
func (kv *KVServer) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.kvStore) != nil || e.Encode(kv.dupTable) != nil {
		panic("Failed to encode snapshot")
	}
	return w.Bytes()
}

func (kv *KVServer) restoreSnapshot(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvStore map[string]string
	var dupTable map[int64]int64
	if d.Decode(&kvStore) != nil || d.Decode(&dupTable) != nil {
		panic("Failed to decode snapshot")
	}
	kv.kvStore = kvStore
	kv.dupTable = dupTable
}

// === Apply ops from raft log ===
func (kv *KVServer) applyOpsLoop() {
	for {
		msg := <-kv.applyCh
		if !msg.CommandValid {
			kv.mu.Lock()
			kv.restoreSnapshot(msg.Command.([]byte))
			kv.mu.Unlock()
			continue
		}

		op := msg.Command.(Op)
		idx := msg.CommandIndex

		kv.mu.Lock()
		var result OpResult
		switch op.Type {
		case "Get":
			val, ok := kv.kvStore[op.Key]
			if ok {
				result = OpResult{Err: OK, Value: val}
			} else {
				result = OpResult{Err: ErrNoKey, Value: ""}
			}
		case "Put":
			lastSeqNum, ok := kv.dupTable[op.ClientId]
			isDuplicated := ok && lastSeqNum >= op.SeqNum
			if !isDuplicated {
				kv.kvStore[op.Key] = op.Value
				kv.dupTable[op.ClientId] = op.SeqNum
				result = OpResult{Err: OK}
			}
		case "Append":
			lastSeqNum, ok := kv.dupTable[op.ClientId]
			isDuplicated := ok && lastSeqNum >= op.SeqNum
			if !isDuplicated {
				kv.kvStore[op.Key] += op.Value
				kv.dupTable[op.ClientId] = op.SeqNum
				result = OpResult{Err: OK}
			}
		}
		if chanel, ok := kv.notifyChans[idx]; ok {
			chanel <- result
		}
		// Start snapshotting if necessary
		if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
			snapshot := kv.encodeSnapshot()
			go kv.rf.Snapshot(idx, snapshot)
		}
		kv.mu.Unlock()

	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvStore = make(map[string]string)
	kv.dupTable = make(map[int64]int64)
	kv.notifyChans = make(map[int]chan OpResult)
	kv.restoreSnapshot(persister.ReadSnapshot())

	// Start backgroud loop to recieve logs commited from raft
	go kv.applyOpsLoop()

	return kv
}
