package shardkv

import (
	"labs/src/shardmaster"
	"time"
)

func (kv *ShardKV) Reconfigure(oldConfig shardmaster.Config, newConfig shardmaster.Config) {

	incomeShards := kv.getIncomeShards(oldConfig, newConfig)
	DPrintf("old %v newConfig %v\n", oldConfig, newConfig)
	for _, shardNum := range incomeShards {
		targetGroup := oldConfig.Shards[shardNum]
		kv.shardWaitToPull[shardNum] = targetGroupInfo{
			TargetGroupID: targetGroup,
			Servers:       oldConfig.Groups[targetGroup],
			OldConfigNum:  oldConfig.Num,
		}
	}
	outShards := kv.getOutShards(oldConfig, newConfig)
	for _, shardNum := range outShards {
		kv.shardWaitToOut[shardNum] = oldConfig.Num
	}
}

func (kv *ShardKV) PullShardsDeamon() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			for shardNum, g := range kv.shardWaitToPull {
				newShard := kv.requestShardData(shardNum, g)
				if len(newShard) == 0 {
					continue
				}
				command := Op{
					Op:     UpdateShard,
					Shards: newShard,
				}
				kv.mu.Unlock()
				commandIndex, _, _ := kv.rf.Start(command)
				DPrintf("get shardNum-%v, put shard into raft, logIndex-%v\n", shardNum, commandIndex)
				time.Sleep(ServerWaitInterval)
				kv.mu.Lock()
			}
			kv.mu.Unlock()
		}
		time.Sleep(ServerWaitInterval)
	}
}

func (kv *ShardKV) PullShard(args *PullShardArgs, reply *PullShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.ConfigNum = kv.stateMachine[args.ShardNum].ConfigNum
	if args.ConfigNum >= kv.stateMachine[args.ShardNum].ConfigNum {
		return
	}
	reply.ShardData = deepCopyShardData(kv.stateMachine[args.ShardNum].Data)
	reply.ClientRequestSeq = deepCopyClientRequestSeq(kv.stateMachine[args.ShardNum].LastSeqMapping)
}

func getShardsByGID(gid int, config shardmaster.Config) map[int]int {
	result := make(map[int]int)
	for i := 0; i < len(config.Shards); i++ {
		if config.Shards[i] == gid {
			result[i] = gid
		}
	}
	return result
}

func (kv *ShardKV) sendReadySignal(shardNum int, target targetGroupInfo) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	kv.mu.Lock()
	servers := target.Servers
	targetGroup := target.TargetGroupID
	args := SendReadyArgs{
		ShardNum:  shardNum,
		ConfigNum: target.OldConfigNum,
	}
	reply := SendReadyReply{}

	for {
		if _, ok := kv.groupLeader[targetGroup]; !ok {
			kv.groupLeader[targetGroup] = 0
		}
		for i := 0; i < len(servers); i++ {
			srv := kv.make_end(servers[kv.groupLeader[targetGroup]])
			//DPrintf("SendReady RPC to server-%v-%v, shardNum-%v, oldCfgNum-%v\n", targetGroup,
			//	kv.groupLeader[targetGroup], shardNum, target.OldConfigNum)
			kv.mu.Unlock()
			ok := srv.Call("ShardKV.SendReady", &args, &reply)
			kv.mu.Lock()
			if !ok || reply.Err == ErrWrongLeader {
				kv.groupLeader[targetGroup] = (kv.groupLeader[targetGroup] + 1) % len(servers)
				continue
			}
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		time.Sleep(ServerWaitInterval)
		kv.mu.Lock()
	}
}

func (kv *ShardKV) getIncomeShards(oldConfig shardmaster.Config, newConfig shardmaster.Config) []int {
	var incomeShard []int
	oldShards := getShardsByGID(kv.gid, oldConfig)
	newShards := getShardsByGID(kv.gid, newConfig)
	if len(oldShards) >= len(newShards) {
		return incomeShard
	}
	for shardNum, _ := range newShards {
		if _, ok := oldShards[shardNum]; ok {
			continue
		}
		incomeShard = append(incomeShard, shardNum)
	}
	return incomeShard
}

func (kv *ShardKV) getOutShards(oldConfig shardmaster.Config, newConfig shardmaster.Config) []int {
	var outShard []int
	oldShards := getShardsByGID(kv.gid, oldConfig)
	newShards := getShardsByGID(kv.gid, newConfig)
	if len(oldShards) <= len(newShards) {
		return outShard
	}
	for shardNum, _ := range oldShards {
		if _, ok := newShards[shardNum]; ok {
			continue
		}
		outShard = append(outShard, shardNum)
	}
	return outShard
}

func (kv *ShardKV) requestShardData(shardNum int, g targetGroupInfo) map[int]Shard {

	targetGroup := g.TargetGroupID
	result := make(map[int]Shard)
	if targetGroup == 0 {
		result[shardNum] = Shard{
			ShardID:        shardNum,
			ConfigNum:      1,
			Data:           make(map[string]string),
			LastSeqMapping: make(map[int64]int64),
		}
		return result
	}
	args := PullShardArgs{
		ConfigNum: g.OldConfigNum,
		ShardNum:  shardNum,
	}
	reply := PullShardReply{}
	servers := g.Servers
	if _, ok := kv.groupLeader[targetGroup]; !ok {
		kv.groupLeader[targetGroup] = 0
	}

	for i := 0; i < len(servers); i++ {
		srv := kv.make_end(servers[kv.groupLeader[targetGroup]])
		DPrintf("send PullShard request to server-%v-%v, shardNum-%v, oldShard.configNum-%v\n",
			targetGroup, kv.groupLeader[targetGroup], shardNum, g.OldConfigNum)
		kv.mu.Unlock()
		ok := srv.Call("ShardKV.PullShard", &args, &reply)
		DPrintf("send PullShard request reply %v, reply.ConfigNum %v\n", reply, reply.ConfigNum)
		kv.mu.Lock()
		if !ok || reply.ConfigNum <= g.OldConfigNum {
			kv.groupLeader[targetGroup] = (kv.groupLeader[targetGroup] + 1) % len(servers)
			DPrintf("do not get shardNum-%v from server-%v-%v, ok-%v, reply.ConfigNum-%v\n",
				shardNum, targetGroup, kv.groupLeader[targetGroup], ok, reply.ConfigNum)
			kv.MissShardTime++
			continue
		}
		DPrintf("already get shardNum-%v from server-%v-%v, ok-%v, reply.ConfigNum-%v\n",
			shardNum, targetGroup, kv.groupLeader[targetGroup], ok, reply.ConfigNum)
		result[shardNum] = Shard{
			ShardID:        shardNum,
			ConfigNum:      min(reply.ConfigNum, kv.config.Num),
			Data:           deepCopyShardData(reply.ShardData),
			LastSeqMapping: deepCopyClientRequestSeq(reply.ClientRequestSeq),
		}
		kv.MissShardTime = 0
		DPrintf("get shardNum-%v from server-%v-%v, newShardVersion-%v\n",
			shardNum, targetGroup, kv.groupLeader[targetGroup], result[shardNum].ConfigNum)
		break
	}
	return result
}

func (kv *ShardKV) SendReady(args *SendReadyArgs, reply *SendReadyReply) {
	command := Op{
		ShardNum: args.ShardNum,
		Config: shardmaster.Config{
			Num: args.ConfigNum,
		},
		Op: DeleteShard,
	}
	_, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = OK
}
