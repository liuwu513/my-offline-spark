package com.woody.spark.job;

import com.woody.spark.entity.RDDKeyByCounts;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 拆分分词库
 * 2~4位置
 */
public class GoodsItemNameByPosition {

    private static final Pattern SPACE = Pattern.compile("");
    private static final String regexStr = "[^\u4E00-\u9FA5]";  //匹配中文的正则表达式

    private static class TupleComparator implements Comparator<Tuple2<String, Integer>>, Serializable {
        @Override
        public int compare(Tuple2<String, Integer> tuple1, Tuple2<String, Integer>tuple2) {
            return tuple1._2 < tuple2._2 ? 0 : 1;
        }
    }


    public static void save(String goods_category, SparkContext sc, SparkSession sparkSession, int position){

        Dataset<Row> goodsDF = sparkSession.read().format("json").json("/data/app/source.json");

        JavaRDD<Row> dataset = goodsDF.filter(goodsDF.col("goodsCategory").equalTo(goods_category)).select("itemName").toJavaRDD();

        JavaRDD<String> words = dataset.flatMap(s -> {
            String str = s.toString().replaceAll(regexStr, "");
            if(str.length() >= position){
                str = str.substring(position-1, position);
            } else {
                str = "";
            }
            return Arrays.asList(str).iterator();
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        List<Tuple2<String, Integer>> output = counts.collect();

        JavaSparkContext jsc = new JavaSparkContext(sc);
        JavaRDD<Tuple2<String, Integer>> tuple2JavaRDD = jsc.parallelize(output);
        // 排序
        tuple2JavaRDD = tuple2JavaRDD.sortBy(new Function<Tuple2<String, Integer>, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(Tuple2<String, Integer> v )  {
                return v._2();
            }
        }, false, 3);
        output =  tuple2JavaRDD.collect();


        //List<Tuple2<String, Integer>> output = counts.sortByKey().collect();
        List<RDDKeyByCounts> list = new ArrayList<>();
        for (int i=0; i<output.size()&&i<50; i++) {
            Tuple2<?,?> tuple = output.get(i);
            RDDKeyByCounts keyByCounts = new RDDKeyByCounts();
            keyByCounts.setName(tuple._1().toString());
            keyByCounts.setCounts(Long.valueOf(tuple._2().toString()));
            if(!tuple._1().toString().equals("")){
                list.add(keyByCounts);
            }
        }
        Dataset<Row> df = sparkSession.createDataFrame(list, RDDKeyByCounts.class);
        df.write().mode(SaveMode.Overwrite).json("/data/app/"+goods_category + "/" + position +"/position.json");

    }

    public static void main(String[] args) {
        //自定义比较器
        SparkConf conf = new SparkConf().setAppName("my-app");
        SparkContext sc = new SparkContext(conf);

        SparkSession sparkSession = new SparkSession(sc);

        for (int i=1; i<=45; i++){
            for(int j=2; j <=4; j++) {
                save(String.valueOf(i), sc, sparkSession, j);
            }
        }
        sparkSession.stop();
    }
}
