#!/bin/bash

# 检查是否为 root 权限
if [ "$EUID" -ne 0 ]; then 
  echo "use sudo to run this script"
  exit
fi

echo "--- 1. current CPU frequency status ---"
# 查看每个核心的当前频率、最大/最小支持频率
grep -E '^processor|cpu MHz' /proc/cpuinfo | head -n 10

# 使用 cpupower 查看更详细的范围（如果安装了的话）
if command -v cpupower &> /dev/null; then
    cpupower frequency-info | grep -E "limits|driver"
else
    echo "Notice: cpupower is not installed, installing it (apt install linux-tools-common)"
fi

echo -e "\n--- 2. Setting all cores to maximum performance mode ---"

# 1. 禁用 Intel Turbo Boost
# 这能防止频率在实验中波动，虽然数值上少了睿频，但结果更一致
if [ -f "/sys/devices/system/cpu/intel_pstate/no_turbo" ]; then
    echo 1 > /sys/devices/system/cpu/intel_pstate/no_turbo
    echo "Turbo Boost has been disabled"
else
    echo "Warning: intel_pstate interface not found, Turbo Boost may not be disabled"
fi

# 尝试通过 sysfs 设定所有核心
for cpu in /sys/devices/system/cpu/cpu[0-9]*; do
    # 设置 Governor 为 performance
    if [ -f "$cpu/cpufreq/scaling_governor" ]; then
        echo "performance" > "$cpu/cpufreq/scaling_governor"
    fi
    
    # 将最小频率设为最大支持频率，实现强制“锁频”
    if [ -f "$cpu/cpufreq/scaling_max_freq" ]; then
        cat "$cpu/cpufreq/scaling_max_freq" > "$cpu/cpufreq/scaling_min_freq"
    fi
done

echo "Set all CPUs to max frequency mode"


echo -e "\n--- 3. Checking current cpu frequency status---"
# 查看当前各个核心的 Governor 和 频率
echo "current Governor status:"
cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor | sort | uniq -c

echo "current real-time frequency (MHz):"
cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_cur_freq | head -n 5