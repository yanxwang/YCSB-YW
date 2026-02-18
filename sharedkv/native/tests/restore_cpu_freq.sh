#!/bin/bash

# 检查 root 权限
if [ "$EUID" -ne 0 ]; then 
  echo "use sudo to run this script"
  exit
fi

echo "--- Restoring default CPU power settings ---"

# 1. Restoring Turbo Boost (if previously disabled)
if [ -f "/sys/devices/system/cpu/intel_pstate/no_turbo" ]; then
    echo 0 > /sys/devices/system/cpu/intel_pstate/no_turbo
    echo "Intel Turbo Boost has been re-enabled"
fi

# 2. Restoring Governor and frequency range
for cpu in /sys/devices/system/cpu/cpu[0-9]*; do
    # Prefer restoring powersave (default on modern Intel systems) or ondemand
    if [ -d "$cpu/cpufreq" ]; then
        # 获取系统支持的最小频率并还原
        MIN_SUPPORTED=$(cat "$cpu/cpufreq/cpuinfo_min_freq")
        echo "$MIN_SUPPORTED" > "$cpu/cpufreq/scaling_min_freq"
        
        # 设定 Governor
        if grep -q "powersave" "$cpu/cpufreq/scaling_available_governors"; then
            echo "powersave" > "$cpu/cpufreq/scaling_governor"
        else
            echo "ondemand" > "$cpu/cpufreq/scaling_governor"
        fi
    fi
done

echo "--- Restoration complete ---"
echo "Current Governor status:"
cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor | sort | uniq -c