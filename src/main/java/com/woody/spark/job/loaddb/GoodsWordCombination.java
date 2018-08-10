package com.woody.spark.job.loaddb;

import com.woody.spark.entity.RDDKeyByCounts;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 组合
 */
public class GoodsWordCombination {

    private static final Pattern SPACE = Pattern.compile("");
    private static final String regexStr = "[^\u4E00-\u9FA5]";  //匹配中文的正则表达式


    /**
     * 组合
     * @param sc
     * @param sparkSession
     */
    public static void read2(SparkContext sc, SparkSession sparkSession){
        Dataset<Row> goodsDF1 = sparkSession.read().format("json").json("/data/app/all/category.json");
        JavaRDD<Row> rdd1 = goodsDF1.select("name", "counts").toJavaRDD();
        JavaRDD<Row> rdd2 = goodsDF1.select("name", "counts").toJavaRDD();
        List<Tuple2<Row, Row>> output = rdd1.cartesian(rdd2).collect();
        List<RDDKeyByCounts> list = new ArrayList<>();
        for (int i=0; i<output.size(); i++) {
            Tuple2<Row,Row> tuple = output.get(i);
            RDDKeyByCounts keyByCounts = new RDDKeyByCounts();
            keyByCounts.setName(tuple._1().getString(0)+tuple._2().getString(0));
            keyByCounts.setCounts(Long.valueOf(tuple._1().get(1).toString())+Long.valueOf(tuple._2().get(1).toString()));
            if(!tuple._1().getString(0).contains(tuple._2().getString(0))){
                list.add(keyByCounts);
            }
        }
        Dataset<Row> df = sparkSession.createDataFrame(list,  RDDKeyByCounts.class);
        df.write().mode(SaveMode.Overwrite).json("/data/app/all/combination2.json");
        read3(sc, sparkSession);
    }

    /**
     * 组合
     * @param sc
     * @param sparkSession
     */
    public static void read3(SparkContext sc, SparkSession sparkSession){
        Dataset<Row> goodsDF1 = sparkSession.read().format("json").json("/data/app/all/category.json");
        JavaRDD<Row> rdd1 = goodsDF1.select("name", "counts").toJavaRDD();
        Dataset<Row> goodsDF2 = sparkSession.read().format("json").json("/data/app/all/combination2.json");
        JavaRDD<Row> rdd2 = goodsDF2.select("name", "counts").toJavaRDD();
        List<Tuple2<Row, Row>> output = rdd1.cartesian(rdd2).collect();
        List<RDDKeyByCounts> list = new ArrayList<>();
        for (int i=0; i<output.size(); i++) {
            Tuple2<Row,Row> tuple = output.get(i);
            RDDKeyByCounts keyByCounts = new RDDKeyByCounts();
            keyByCounts.setName(tuple._1().getString(0)+tuple._2().getString(0));
            keyByCounts.setCounts(Long.valueOf(tuple._1().get(1).toString())+Long.valueOf(tuple._2().get(1).toString()));
            if(!tuple._2().getString(0).contains(tuple._1().getString(0))){
                list.add(keyByCounts);
            }
        }
        Dataset<Row> df = sparkSession.createDataFrame(list,  RDDKeyByCounts.class);
        df.write().mode(SaveMode.Overwrite).json("/data/app/all/combination3.json");
        read4(sc, sparkSession);
    }

    /**
     * 组合
     * @param sc
     * @param sparkSession
     */
    public static void read4(SparkContext sc, SparkSession sparkSession){
        Dataset<Row> goodsDF1 = sparkSession.read().format("json").json("/data/app/all/category.json");
        JavaRDD<Row> rdd1 = goodsDF1.select("name", "counts").toJavaRDD();
        Dataset<Row> goodsDF2 = sparkSession.read().format("json").json("/data/app/all/combination3.json");
        JavaRDD<Row> rdd2 = goodsDF2.select("name", "counts").toJavaRDD();
        List<Tuple2<Row, Row>> output = rdd1.cartesian(rdd2).collect();
        List<RDDKeyByCounts> list = new ArrayList<>();
        for (int i=0; i<output.size(); i++) {
            Tuple2<Row,Row> tuple = output.get(i);
            RDDKeyByCounts keyByCounts = new RDDKeyByCounts();
            keyByCounts.setName(tuple._1().getString(0)+tuple._2().getString(0));
            keyByCounts.setCounts(Long.valueOf(tuple._1().get(1).toString())+Long.valueOf(tuple._2().get(1).toString()));
            if(!tuple._2().getString(0).contains(tuple._1().getString(0))){
                list.add(keyByCounts);
            }
        }
        Dataset<Row> df = sparkSession.createDataFrame(list,  RDDKeyByCounts.class);
        df.write().mode(SaveMode.Overwrite).json("/data/app/all/combination4.json");
    }

    public static void main(String[] args) {
        //自定义比较器
        SparkConf conf = new SparkConf().setAppName("my-app");
        SparkContext sc = new SparkContext(conf);

        SparkSession sparkSession = new SparkSession(sc);
        read2(sc, sparkSession);

        sparkSession.stop();
    }
}
