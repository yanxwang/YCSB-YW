#!/bin/bash

# 检查权限
if [ "$EUID" -ne 0 ]; then 
  echo "请使用 sudo 运行此脚本"
  exit 1
fi

echo "--- 正在检测 CXL/DAX 设备状态 ---"

# 使用 grep 和 awk 从文本中提取设备名
# 寻找包含 "chardev":"dax... 的行
devices=$(daxctl list | grep "chardev" | cut -d'"' -f4)

if [ -z "$devices" ]; then
    echo "未发现任何 DAX 设备。"
    exit 1
fi

for dev in $devices; do
    # 检查该设备对应的 Node
    # 提取 "target_node":X 后面的数字
    node=$(daxctl list -d "$dev" | grep "target_node" | awk -F: '{print $2}' | tr -d ' ,')
    
    echo "检测到设备: $dev -> 对应 Node: $node"
    
    # 检查 Node 是否已经上线 (size > 0)
    size=$(numactl --hardware | grep "node $node size" | awk '{print $4}')
    
    if [ "$size" -eq 0 ]; then
        echo "正在上线 $dev..."
        daxctl online-memory "$dev"
        echo "上线完成。"
    else
        echo "Node $node 已经是 Online 状态 ($size MB)。"
    fi
done

echo "--- 当前内存分布 ---"
numactl --hardware | grep "size"