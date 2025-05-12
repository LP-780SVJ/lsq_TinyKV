#!/bin/bash


# 全局开关：是否将失败结果写入日志文件
# 设置为 true 记录日志，设置为 false 不记录日志
LOG_FAILURES=true

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# 测试名称
# TEST_NAME="TestBasic2B"
#通过
# TEST_NAME="TestConcurrent2B"
#通过
# TEST_NAME="TestUnreliable2B"
#通过
# TEST_NAME="TestOnePartition2B"
#通过
# TEST_NAME="TestManyPartitionsOneClient2B"
#通过
# TEST_NAME="TestManyPartitionsManyClients2B"
#通过
# TEST_NAME="TestPersistOneClient2B"
#通过
# TEST_NAME="TestPersistConcurrent2B"
#通过
# TEST_NAME="TestPersistConcurrentUnreliable2B"
# 通过
# TEST_NAME="TestPersistPartition2B"
# 通过
# TEST_NAME="TestPersistPartitionUnreliable2B"
# 通过
# TEST_NAME="TestOneSnapshot2C"
# TEST_NAME="TestSnapshotRecover2C"
# TEST_NAME="TestSnapshotRecoverManyClients2C"
# TEST_NAME="TestSnapshotUnreliable2C"
# TEST_NAME="TestSnapshotUnreliableRecover2C"
TEST_NAME="TestSnapshotUnreliableRecoverConcurrentPartition2C"


TOTAL_RUNS=10 
PASSED=0
FAILED=0

# 创建日志文件只用于记录失败的测试
FAILURE_LOG_FILE="failed_${TEST_NAME}_runs.log"

# 只有在需要记录日志时才创建/清空日志文件
if [ "$LOG_FAILURES" = true ]; then
    > $FAILURE_LOG_FILE
    echo "============== 测试开始 ==============" >> $FAILURE_LOG_FILE
    echo "测试名称: $TEST_NAME" >> $FAILURE_LOG_FILE
    echo "开始时间: $(date)" >> $FAILURE_LOG_FILE
    echo "总运行次数: $TOTAL_RUNS" >> $FAILURE_LOG_FILE
    echo "" >> $FAILURE_LOG_FILE
fi

echo -e "${YELLOW}开始运行 $TEST_NAME 测试 $TOTAL_RUNS 次...${NC}"
echo "==============================================="

for i in $(seq 1 $TOTAL_RUNS); do
    TEMP_LOG_FILE=$(mktemp)

    echo -n "运行 #$i: "

    # 使用go test运行特定测试
    # if go test -count=1 -v ./kv/test_raftstore -run $TEST_NAME -timeout 5m > $TEMP_LOG_FILE 2>&1; then
    if go test -count=1 -v -run $TEST_NAME -timeout 5m > $TEMP_LOG_FILE 2>&1; then

        echo -e "${GREEN}通过✓${NC}"
        ((PASSED++))
    else
        echo -e "${RED}失败✗${NC}"
        ((FAILED++))

        # 只有在开关开启时才记录日志
        if [ "$LOG_FAILURES" = true ]; then
            echo "============== 测试运行 #$i 失败 ==============" >> $FAILURE_LOG_FILE
            echo "失败时间: $(date)" >> $FAILURE_LOG_FILE
            echo "" >> $FAILURE_LOG_FILE

            # 将测试输出添加到失败日志文件
            cat $TEMP_LOG_FILE >> $FAILURE_LOG_FILE

            # 添加三个空行作为分隔符
            echo "" >> $FAILURE_LOG_FILE
            echo "" >> $FAILURE_LOG_FILE
            echo "" >> $FAILURE_LOG_FILE
        fi
    fi

    # 删除临时文件
    rm $TEMP_LOG_FILE
    rm -rf /tmp/*test-raftstore*
done

# 计算通过率
PASS_RATE=$(echo "scale=2; $PASSED*100/$TOTAL_RUNS" | bc)

echo "==============================================="
echo -e "${YELLOW}测试结果汇总:${NC}"
echo "总共运行: $TOTAL_RUNS"
echo -e "${GREEN}通过: $PASSED${NC}"
echo -e "${RED}失败: $FAILED${NC}"
echo -e "${YELLOW}通过率: ${PASS_RATE}%${NC}"

# 将通过率写入日志文件
if [ "$LOG_FAILURES" = true ]; then
    echo "============== 测试结果汇总 ==============" >> $FAILURE_LOG_FILE
    echo "总共运行: $TOTAL_RUNS" >> $FAILURE_LOG_FILE
    echo "通过: $PASSED" >> $FAILURE_LOG_FILE
    echo "失败: $FAILED" >> $FAILURE_LOG_FILE
    echo "通过率: ${PASS_RATE}%" >> $FAILURE_LOG_FILE
    echo "结束时间: $(date)" >> $FAILURE_LOG_FILE

    # 如果有失败，提示查看日志文件
    if [ $FAILED -gt 0 ]; then
        echo -e "${RED}失败的测试详情已保存到 $FAILURE_LOG_FILE${NC}"

        # 显示失败测试的运行编号
        echo -e "${RED}失败的测试运行:${NC}"
        grep -n "测试运行 #" $FAILURE_LOG_FILE | sed 's/.*测试运行 #\([0-9]*\).*/运行 #\1/'
    else
        echo -e "${GREEN}所有测试均通过，未生成失败日志${NC}"
    fi
fi

exit $FAILED