# 一.项目用于提取库中数据拼装词组规则
## 1. 数据提取(3500个常用汉字) 
### 1.1   定义JOB读取word-han.txt 文件，生成汉字词库
    word-han.txt

## 2. 定义离线计算，封装词组并且过滤重复词组
### 2.1 spark离线计算命令
    /tools/spark/bin/spark-submit --master  yarn-cluster  --class  /tools/jars/spark-web-1.0-SNAPSHOT.jar

### 2.2 spark离线计算命令，增加配置参数
    /tools/spark/bin/spark-submit --master  yarn-cluster --num-executors 4 --executor-cores 2 --executor-memory 1G  --class  GoodsWordCombinationByCategory /tools/apps/spark-web-2.0-SNAPSHOT.jar
    /tools/spark/bin/spark-submit --master  yarn-cluster --num-executors 4 --executor-cores 2 --executor-memory 1G  --class  GoodsFromMySQL /tools/apps/spark-web-1.0-SNAPSHOT.jar


