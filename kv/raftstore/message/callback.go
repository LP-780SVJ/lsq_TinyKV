package message

import (
	"time"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
)

/*
Callback在TinyKV中的作用：
1.提供异步通知机制
2.存储命令执行结果
3.支持快照操作
4.解耦命令执行和结果处理
*/
type Callback struct {
	Resp *raft_cmdpb.RaftCmdResponse //存储Raft命令的相应结果
	Txn  *badger.Txn                 // used for GetSnap，存储一个Badger数据库的事务对象
	done chan struct{}               //一个无缓冲的通道，用于通知回调完成
}

func (cb *Callback) Done(resp *raft_cmdpb.RaftCmdResponse) {
	if cb == nil {
		return
	}
	if resp != nil {
		cb.Resp = resp
	}
	cb.done <- struct{}{}
}

func (cb *Callback) WaitResp() *raft_cmdpb.RaftCmdResponse {
	select {
	case <-cb.done:
		return cb.Resp
	}
}

func (cb *Callback) WaitRespWithTimeout(timeout time.Duration) *raft_cmdpb.RaftCmdResponse {
	select {
	case <-cb.done:
		return cb.Resp
	case <-time.After(timeout):
		return cb.Resp
	}
}

func NewCallback() *Callback {
	done := make(chan struct{}, 1)
	cb := &Callback{done: done}
	return cb
}
