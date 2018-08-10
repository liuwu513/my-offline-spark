package com.woody.spark.entity;

import lombok.Data;

import java.io.Serializable;

@Data
public class RDDKeyByCounts implements Serializable {

    private String name;
    private Long counts;
    private Long goodsCategory;
    private Long nameLen;
    private String pinYin;
}
