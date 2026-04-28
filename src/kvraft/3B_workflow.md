# Snapshot Workflow

**section 7 of raft paper** 
- http://nil.csail.mit.edu/6.824/2020/papers/raft-extended.pdf
- Each node is responsible for their own log compaction. 
  Only if follower log is so far behind, leader sends install snapshot rpc.
  Therefore, this is local storage optimization.


KV server grows too large 
↓
KV server calls rf.Snapshot(index, data) 
↓
Raft trims log, saves snapshot 
|
↓ if follower logs are behind (slow follower)
On next heartbeat, leader sees nextIndex[i] <= lastIncludedIndex for slow follower 
↓
Leader calls sendInstallSnapshot instead of AppendEntries 
↓
Follower's InstallSnapshot handler replaces log, sends to applyCh 
↓
KV server's applyOpsLoop receives CommandValid=false, restores kvStore from snapshot