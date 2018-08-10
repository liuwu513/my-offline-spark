# 一.项目用于提取库中数据拼装词组规则
## 1. 数据提取(3500个常用汉字) 
### 1.1   定义JOB读取words-han.txt 文件，生成汉字词库
    words-han.txt

## 2. 定义离线计算，封装词组并且过滤重复词组
### 2.1 spark离线计算命令,加载并生成词库
    /tools/spark/bin/spark-submit --master  yarn-cluster --num-executors 4 --executor-cores 2 --executor-memory 3G  --class com.woody.spark.job.loadtxt.WordsFromText  /tools/apps/my-offlie-spark-1.0-SNAPSHOT.jar

### 2.2 spark离线计算命令，增加配置参数
    /tools/spark/bin/spark-submit --master  yarn-cluster --num-executors 4 --executor-cores 2 --executor-memory 3G  --class  GoodsWordCombinationByCategory /tools/apps/my-offlie-spark-1.0-SNAPSHOT.jar
    /tools/spark/bin/spark-submit --master  yarn-cluster --num-executors 4 --executor-cores 2 --executor-memory 3G  --class  GoodsFromMySQL /tools/apps/my-offlie-spark-1.0-SNAPSHOT.jar