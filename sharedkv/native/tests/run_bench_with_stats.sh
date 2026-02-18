#!/bin/bash

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <prod_cpu> <cons_cpu> <mem_node>"
    exit 1
fi

PROD_CPU=$1
CONS_CPU=$2
MEM_NODE=$3

# 定义采样函数：计算特定 CPU 的利用率
get_cpu_util() {
    # 读取 /proc/stat 中指定 CPU 的行
    read -r cpu user nice system idle iowait irq softirq steal guest guest_nice < <(grep "cpu$1 " /proc/stat)
    echo "$user $nice $system $idle $iowait $irq $softirq $steal"
}

echo "--- 开始背景 CPU 监测 (采样频率: 0.1s) ---"

# 启动后台监测
(
    while true; do
        # 记录起始快照
        S1_P=($(get_cpu_util $PROD_CPU))
        S1_C=($(get_cpu_util $CONS_CPU))
        
        sleep 0.1
        
        # 记录结束快照
        S2_P=($(get_cpu_util $PROD_CPU))
        S2_C=($(get_cpu_util $CONS_CPU))

        # 计算增量 (这里简化计算，只看总忙碌时间比)
        # 忙碌时间 = Total - Idle
        calc_util() {
            local s1=("$@")
            local s2=("${@:9}") # 获取第二组快照
            
            local total1=0; for x in "${s1[@]}"; do total1=$((total1 + x)); done
            local total2=0; for x in "${s2[@]}"; do total2=$((total2 + x)); done
            
            local idle1=${s1[3]}
            local idle2=${s2[3]}
            
            local diff_total=$((total2 - total1))
            local diff_idle=$((idle2 - idle1))
            
            if [ $diff_total -eq 0 ]; then echo "0"; else
                echo "scale=2; 100 * ($diff_total - $diff_idle) / $diff_total" | bc
            fi
        }

        U_P=$(calc_util "${S1_P[@]}" "${S2_P[@]}")
        U_C=$(calc_util "${S1_C[@]}" "${S2_C[@]}")
        
        echo "Time: $(date +%H:%M:%S.%N) | CPU $PROD_CPU (Prod): $U_P% | CPU $CONS_CPU (Cons): $U_C%" >> cpu_util.log
    done
) &
MONITOR_PID=$!

# 运行测试程序
g++ -O3 -march=native cxl_spsc_queue_test.cpp -o cxl_spsc_queue_test -lnuma -lpthread
./cxl_spsc_queue_test $PROD_CPU $CONS_CPU $MEM_NODE

# 杀掉监测进程并清理
kill $MONITOR_PID
echo "--- 监测结束，结果已存入 cpu_util.log ---"
tail -n 5 cpu_util.log