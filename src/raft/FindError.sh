#!/bin/bash  
  
# nohup ./mybash.sh &后台执行
# 命令  
COMMAND="go test -run 2"
  
# 输出文件  
OUTPUT_FILE="DEBUG1.txt"  

declare -i counter=0

rm -rf $OUTPUT_FILE
# 无限循环执行命令并将输出重定向到文件  
while true; do
    counter=$((counter+1))
    echo "第${counter}次运行：" >> $OUTPUT_FILE
    $COMMAND >> $OUTPUT_FILE  #>覆盖    #>>追加
    wait  # 可选，暂停1秒，以控制脚本执行的频率
    if [ $(cat ./$OUTPUT_FILE | grep -e FAIL | wc -l) -gt 0 ]; then
        break
    fi
done