package com.woody.spark.job.loaddb;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;

/**
 * 数据加载
 * Created by liuwu on 2018/8/6 0006.
 */
public class GoodsFromMySQL {

    /**
     * 加载数据库数据
     *
     * @param sc           spark context
     * @param sparkSession spark session
     */
    public static void loadGoodsInfo(SparkContext sc, SparkSession sparkSession) {
        String url = "jdbc:mysql://x.x.x.x:3306/db-test";

        String sql = "(SELECT item_name as itemName, goods_category as goodsCategory FROM goods where dict_type='100203' and item_name " +
                "is not null) as my-goods";

        SQLContext sqlContext = SQLContext.getOrCreate(sc);
        DataFrameReader reader = sqlContext.read().format("jdbc").
                option("url", url).option("dbtable", sql).
                option("driver", "com.mysql.jdbc.Driver").
                option("user", "root").
                option("password", "xxxxx");


        Dataset<Row> goodsDataSet = reader.load();

        // Looks the schema of this DataFrame.
        goodsDataSet.printSchema();

        goodsDataSet.write().mode(SaveMode.Overwrite).json("/data/app/source_new.json");
    }


    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("my-app");
        SparkContext sc = new SparkContext(conf);

        SparkSession sparkSession = new SparkSession(sc);

        loadGoodsInfo(sc, sparkSession);
    }
}
