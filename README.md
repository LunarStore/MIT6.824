# 更新以下lab4b的代码 -- 2023.12.14
lab4b的代码是写完了，但是卡在了TestSnapshot测试上，组群重启后，shards数组的state成员都是Invalid状态（难道persister没有保存shards数组？？？），最大的槽点是太难调试了！！！只能看日志，简直是地狱级难度！！！，还是gdb香啊。以后有时间再调吧，感觉一两天很难肝完，最近真要被这个实验整崩溃了。

# 更新lab4b代码 -- 2023.12.15
搞定了昨天的bug跑通TestSnapshot。后续测试还是存在bug，待修复。

# 更新lab4b代码 -- 2023.12.16
修复昨天的死锁bug，只将应用成功（res == OK）的PutAppend操作记录到去重集中，否则client会**重复使用同一个seq号**向server发请求，server因为将上一次记录“应用失败”的结果不断返回同一个失败。从而导致死锁