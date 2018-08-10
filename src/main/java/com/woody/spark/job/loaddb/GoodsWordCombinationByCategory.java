package com.woody.spark.job.loaddb;

import com.woody.spark.entity.KeyByCounts;
import com.woody.spark.entity.SchemaBuilder;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * 分类组合
 * Created by liuwu on 2018/7/30 0030.
 */
public class GoodsWordCombinationByCategory {

    private static final int minCounts = 10;
    private static final Pattern SPACE = Pattern.compile("");

    public static void read2ByCategory(SparkContext sc, SparkSession sparkSession, String goods_category) {
        //第一位数据集合
        Dataset<Row> goodsDF0 = sparkSession.read().format("json").json(
                String.format("/data/category/%s.json",goods_category));

        //第二位数据集合
        Dataset<Row> goodsDF1 = sparkSession.read().schema(SchemaBuilder.nameCountsSchema()).format("json").json(String.format(
                "/data/position/%s/%s.json", goods_category, 2));

        if (goodsDF1.count() == 0) {
            goodsDF1.write().mode(SaveMode.Overwrite).json(String.format("/data/app/%s/combination2.json", goods_category));
            //拼装 1,2,3位
            read3ByCategory(sc, sparkSession, goods_category);
            return;
        }

        //过滤数据集合
        Dataset<KeyByCounts> distinctCombination2DF = getCombinationByFilter(goodsDF0, goodsDF1);

        //排序
//        Dataset<Row> sortCombination2DF = distinctCombination2DF.sort(col("counts").desc());

        distinctCombination2DF.repartition(1).write().mode(SaveMode.Overwrite).json(String.format("/data/app/%s/combination2.json", goods_category));
        //拼装 1,2,3位
        read3ByCategory(sc, sparkSession, goods_category);

    }

    public static void read3ByCategory(SparkContext sc, SparkSession sparkSession, String goods_category) {
        //1和2组合
        Dataset<Row> goodsDF1 = sparkSession.read().schema(SchemaBuilder.nameCountsSchema()).format("json").json(String.format(
                    "/data/app/%s/combination2.json", goods_category));
        if (goodsDF1.count() == 0) {
            goodsDF1.write().mode(SaveMode.Overwrite).json(String.format("/data/app/%s/combination3.json", goods_category));

            //拼装 1,2,3,4位
            read4ByCategory(sc, sparkSession, goods_category);
            return;
        }
        //第三位数据集合
        Dataset<Row> goodsDF2 = sparkSession.read().schema(SchemaBuilder.nameCountsSchema()).format("json").json(String.format(
                    "/data/position/%s/%s.json", goods_category, 3));
        if (goodsDF2.count() == 0) {
            goodsDF2.write().mode(SaveMode.Overwrite).json(String.format("/data/app/%s/combination3.json", goods_category));

            //拼装 1,2,3,4位
            read4ByCategory(sc, sparkSession, goods_category);
            return;
        }

        //过滤数据集合
        Dataset<KeyByCounts> distinctCombination3DF = getCombinationByFilter(goodsDF1, goodsDF2);

        //排序
//        Dataset<Row> sortCombination3DF = distinctCombination3DF.sort(col("counts").desc());

        distinctCombination3DF.repartition(1).write().mode(SaveMode.Overwrite).json(String.format("/data/app/%s/combination3.json", goods_category));

        //拼装 1,2,3,4位
        read4ByCategory(sc, sparkSession, goods_category);
    }

    public static void read4ByCategory(SparkContext sc, SparkSession sparkSession, String goods_category) {
        //1、2、3组合
        Dataset<Row> goodsDF1 = sparkSession.read().schema(SchemaBuilder.nameCountsSchema()).format("json").json(String.format(
                    "/data/app/%s/combination3.json", goods_category));
        if (goodsDF1.count() == 0) {
            goodsDF1.write().mode(SaveMode.Overwrite).json(String.format("/data/app/%s/combination4.json", goods_category));
            return;
        }

        //第4位数据集合
        Dataset<Row> goodsDF2 = sparkSession.read().schema(SchemaBuilder.nameCountsSchema()).format("json").json(String.format(
                    "/data/position/%s/%s.json", goods_category, 4));
        if (goodsDF2.count() == 0) {
            goodsDF2.write().mode(SaveMode.Overwrite).json(String.format("/data/app/%s/combination4.json", goods_category));
            return;
        }
        Dataset<KeyByCounts> distinctCombination3DF = getCombinationByFilter(goodsDF1, goodsDF2);

        //排序
//        Dataset<Row> sortCombination3DF = distinctCombination3DF.sort(col("counts").desc());

        distinctCombination3DF.repartition(1).write().mode(SaveMode.Overwrite).json(String.format("/data/app/%s/combination4.json", goods_category));
    }

    /***
     * 组合并过滤名字重复字符
     * @param goodsDF1
     * @param goodsDF2
     * @return
     */
    private static Dataset<KeyByCounts> getCombinationByFilter(Dataset<Row> goodsDF1, Dataset<Row> goodsDF2){
        //过滤数据集合
        Dataset<Row> filterDF2 = goodsDF2.selectExpr("name as secondName", "counts as secondCounts").where(String.format("counts >= %d", minCounts));

        Encoder<KeyByCounts> nameKeyEncoder = Encoders.bean(KeyByCounts.class);
        //1、2、3、4位组合
        Dataset<KeyByCounts> combination3DF = goodsDF1.crossJoin(filterDF2).selectExpr("concat(name,secondName) as name", "(counts + secondCounts) as counts").as(nameKeyEncoder);

        //移除重复值
        return combination3DF.dropDuplicates("name").filter(new FilterFunction<KeyByCounts>(){

            @Override
            public boolean call(KeyByCounts value) throws Exception {
                String name = value.getName();
                //过滤重复字
                List<String> nameList = Arrays.asList(SPACE.split(name)).stream().distinct().collect(Collectors.toList());
                //过滤后，字数跟名字长度一致则无相同字
                if (name.length() == nameList.size()){
                    return true;
                }
                return false;
            }
        });
    }


    public static void main(String[] args) {
        //自定义比较器
        SparkConf conf = new SparkConf().setAppName("my-app");
        SparkContext sc = new SparkContext(conf);

        SparkSession sparkSession = new SparkSession(sc);
        for (int i = 1; i <= 45; i++) {
            read2ByCategory(sc, sparkSession, String.valueOf(i));
        }
        sparkSession.stop();
    }
}
