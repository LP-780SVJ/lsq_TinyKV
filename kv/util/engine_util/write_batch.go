package engine_util

import (
	"github.com/Connor1996/badger"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
)

type WriteBatch struct {
	entries       []*badger.Entry //存储需要写入或者删除的键值对
	size          int             //记录当前批次中所有键值对的总大小，用于限制批次大小或统计写入量
	safePoint     int             //表示当前安全点的entries列表索引，用于支持回滚操作
	safePointSize int             //表示安全点时的size值，用于在回滚时恢复批次大小
	safePointUndo int
}

const (
	CfDefault string = "default"
	CfWrite   string = "write"
	CfLock    string = "lock"
)

var CFs [3]string = [3]string{CfDefault, CfWrite, CfLock}

func (wb *WriteBatch) Len() int {
	return len(wb.entries)
}

// 在指定的列族CF中设置一个键值对
// 将键值对封装为badger.Entry对象，并添加到entries列表中
// 同时更新size属性，表示当前批次中所有键值对的总大小
func (wb *WriteBatch) SetCF(cf string, key, val []byte) {
	wb.entries = append(wb.entries, &badger.Entry{
		Key:   KeyWithCF(cf, key),
		Value: val,
	})
	wb.size += len(key) + len(val)
}

// 删除指定的元数据键
// 将键封装为badger.Entry对象（值为空），并添加到entries列表中
func (wb *WriteBatch) DeleteMeta(key []byte) {
	wb.entries = append(wb.entries, &badger.Entry{
		Key: key,
	})
	wb.size += len(key)
}

// 删除指定列族CF中的键
func (wb *WriteBatch) DeleteCF(cf string, key []byte) {
	wb.entries = append(wb.entries, &badger.Entry{
		Key: KeyWithCF(cf, key),
	})
	wb.size += len(key)
}

// 设置一个元数据键值对，其中值是一个Protobuf消息
// 将消息序列化为字节数组后，封装为badger.Entry并添加到entries中，同时更新size
func (wb *WriteBatch) SetMeta(key []byte, msg proto.Message) error {
	val, err := proto.Marshal(msg)
	if err != nil {
		return errors.WithStack(err)
	}
	wb.entries = append(wb.entries, &badger.Entry{
		Key:   key,
		Value: val,
	})
	wb.size += len(key) + len(val)
	return nil
}

func (wb *WriteBatch) SetSafePoint() {
	wb.safePoint = len(wb.entries)
	wb.safePointSize = wb.size
}

func (wb *WriteBatch) RollbackToSafePoint() {
	wb.entries = wb.entries[:wb.safePoint]
	wb.size = wb.safePointSize
}

func (wb *WriteBatch) WriteToDB(db *badger.DB) error {
	if len(wb.entries) > 0 {
		err := db.Update(func(txn *badger.Txn) error {
			for _, entry := range wb.entries {
				var err1 error
				if len(entry.Value) == 0 {
					err1 = txn.Delete(entry.Key)
				} else {
					err1 = txn.SetEntry(entry)
				}
				if err1 != nil {
					return err1
				}
			}
			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (wb *WriteBatch) MustWriteToDB(db *badger.DB) {
	err := wb.WriteToDB(db)
	if err != nil {
		panic(err)
	}
}

func (wb *WriteBatch) Reset() {
	wb.entries = wb.entries[:0]
	wb.size = 0
	wb.safePoint = 0
	wb.safePointSize = 0
	wb.safePointUndo = 0
}
