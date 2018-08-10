package com.woody.spark.job.loadtxt;

import com.woody.spark.entity.KeyByCounts;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by liuwu on 2018/8/10 0010.
 */
public class WordsFromText {

    /**
     * 加载文件 words-han.txt
     * @param sc
     * @param sparkSession
     */
    private static void loadWordsFormText(SparkContext sc,SparkSession sparkSession){
        //1.读取3500个常用汉字库
        JavaRDD<String> textFile = sc.textFile("/data/words/words-han.txt",1).toJavaRDD();

        //2.计算单个汉字数量
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        //3.转换成对象
        JavaRDD<KeyByCounts> keyByCountsJavaRDD = counts.map(new Function<Tuple2<String, Integer>, KeyByCounts>() {
            @Override
            public KeyByCounts call(Tuple2<String, Integer> v1) throws Exception {
                KeyByCounts keyByCounts = new KeyByCounts();
                keyByCounts.setName(v1._1.toString());
                keyByCounts.setCounts(Long.valueOf(v1._2().toString()));
                return keyByCounts;
            }
        });

        //4.转换成spark数据集
        Encoder<KeyByCounts> nameKeyEncoder = Encoders.bean(KeyByCounts.class);
        Dataset<KeyByCounts> dataset = sparkSession.createDataset(keyByCountsJavaRDD.rdd(),nameKeyEncoder);

        //5.存储到hadoop hdfs文件系统
        dataset.repartition(1).write().mode(SaveMode.Overwrite).json("/data/app/all.json");
    }

    public static void main(String[] args) {
        //自定义比较器
        SparkConf conf = new SparkConf().setAppName("my-app");
        SparkContext sc = new SparkContext(conf);

        SparkSession sparkSession = new SparkSession(sc);
        //加载数据
        loadWordsFormText(sc, sparkSession);

        sparkSession.close();
    }
}
