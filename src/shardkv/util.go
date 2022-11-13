package shardkv

import (
	"encoding/json"
	"labs/src/shardmaster"
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func deepCopyConfig(src shardmaster.Config) shardmaster.Config {
	buffer, _ := json.Marshal(src)
	var dst shardmaster.Config
	_ = json.Unmarshal(buffer, &dst)
	return dst
}

func deepCopyShard(src Shard) Shard {
	buffer, _ := json.Marshal(src)
	var dst Shard
	_ = json.Unmarshal(buffer, &dst)
	return dst
}

func deepCopyShardData(src map[string]string) map[string]string {
	buffer, _ := json.Marshal(src)
	var dst map[string]string
	_ = json.Unmarshal(buffer, &dst)
	return dst
}

func deepCopyClientRequestSeq(src map[int64]int64) map[int64]int64 {
	buffer, _ := json.Marshal(src)
	var dst map[int64]int64
	_ = json.Unmarshal(buffer, &dst)
	return dst
}
