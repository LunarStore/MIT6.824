#!/bin/bash  
  
# nohup ./mybash.sh &后台执行
# 命令  
COMMAND="go test -run 2C"  
  
# 输出文件  
OUTPUT_FILE="output.txt"  

rf -rf OUTPUT_FILE
# 无限循环执行命令并将输出重定向到文件  
while true; do  
    $COMMAND >> $OUTPUT_FILE  #>覆盖    #>>追加
    wait  # 可选，暂停1秒，以控制脚本执行的频率  
done