package raft

import (
	"fmt"
	"testing"
)

func main() {
	for i := 1; i <= 100; i++ {
		fmt.Printf("第 %d 次测试: ", i)
		result := testing.RunTests(func(pat string, str string) (bool, error) {
			return pat == "TestCommitWithHeartbeat2AB", nil
		}, []testing.InternalTest{
			{"TestCommitWithHeartbeat2AB", TestCommitWithHeartbeat2AB},
		})

		if result {
			fmt.Println("成功")
		} else {
			fmt.Println("失败")
		}
	}
}
