#!/bin/bash

for i in {1..100}
do
    echo "第 $i 次测试:"
    go test -run ^TestCommitWithHeartbeat2AB$ 
    if [ $? -eq 0 ]; then
        echo "测试成功"
    else
        echo "测试失败"
    fi
    echo "----------------------"
done