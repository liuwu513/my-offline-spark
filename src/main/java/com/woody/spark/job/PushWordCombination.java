package com.woody.spark.job;

import com.woody.spark.config.ESProperties;
import com.woody.spark.entity.PinyinTool;
import com.woody.spark.entity.RDDKeyByCounts;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.spark_project.guava.collect.ImmutableMap;

/**
 * Created by liuwu on 2018/8/1 0001.
 */
public class PushWordCombination {

    /**
     * 最大推送数据量
     */
    private static final int maxPushSize = 10000;

    private static PinyinTool tool = new PinyinTool();

    public static void pushDataByLen(SparkContext sc, SparkSession sparkSession, String goodsCategory, Integer len) {
        Dataset<Row> goodsDF1 = sparkSession.read().format("json").json(String.format("/data/app/%s/combination%d.json", goodsCategory, len));
        if (goodsDF1.count() == 0) {
            return;
        }

        sparkSession.udf().register("pinYin", (String s) -> tool.toPinYin(s, "", PinyinTool.Type.LOWERCASE), DataTypes.StringType);

        Encoder<RDDKeyByCounts> nameKeyEncoder = Encoders.bean(RDDKeyByCounts.class);
        Dataset<RDDKeyByCounts> dataset = goodsDF1.selectExpr("name as name", "counts as counts", String.format("%d as goodsCategory", 0),
                String.format("%d as nameLen", len), "pinYin(name) as pinYin").as(nameKeyEncoder);

        JavaEsSpark.saveToEs(dataset.javaRDD(),"goods-category/category", ImmutableMap.of("es.mapping.id", "name"));
    }

    public static void main(String[] args) {
        //自定义比较器
        SparkConf conf = new SparkConf().setAppName("my-app").
                set("es.nodes", ESProperties.IP).
                set("es.port",ESProperties.PORT).
                set("pushdown",ESProperties.PUSH_DOWN).
                set("es.index.auto.create",ESProperties.INDEX_AUTO_CREATE).
                set("es.nodes.wan.only","true").//在这种模式下，连接器禁用发现，并且只在所有操作中通过声明的ESE节点连接，包括读和写
                set("es.net.http.auth.user",ESProperties.SECURITY_USER).
                set("es.net.http.auth.pass",ESProperties.SECURITY_PWD);

        SparkContext sc = new SparkContext(conf);

        SparkSession sparkSession = new SparkSession(sc);

        for (int j = 2; j <= 4; j++) {
            pushDataByLen(sc, sparkSession, "all", j);
        }
        sparkSession.stop();
    }
}
