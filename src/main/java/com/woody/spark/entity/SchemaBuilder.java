package com.woody.spark.entity;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by liuwu on 2018/8/1 0001.
 */
public class SchemaBuilder {

    public static StructType nameCountsSchema(){
        List<StructField> inputFields=new ArrayList<>();
        String splitSeq=",";
        String stringType="name";
        String longType="counts";
        for(String stringTmp:stringType.split(splitSeq)){
            //这里添加string类型,可以空.
            inputFields.add(DataTypes.createStructField(stringTmp,DataTypes.StringType,true));
        }
        for(String longTmp:longType.split(splitSeq)){
            inputFields.add(DataTypes.createStructField(longTmp,DataTypes.LongType,false));
        }
        return DataTypes.createStructType(inputFields);
    }

    public static StructType lastNameCountsSchema(){
        List<StructField> inputFields=new ArrayList<>();
        String splitSeq=",";
        String stringType="name";
        String longType="counts,goodsCategory,nameLen";
        for(String stringTmp:stringType.split(splitSeq)){
            //这里添加string类型,可以空.
            inputFields.add(DataTypes.createStructField(stringTmp,DataTypes.StringType,true));
        }
        for(String longTmp:longType.split(splitSeq)){
            inputFields.add(DataTypes.createStructField(longTmp,DataTypes.LongType,false));
        }
        return DataTypes.createStructType(inputFields);
    }
}
