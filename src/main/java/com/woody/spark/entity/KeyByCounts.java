package com.woody.spark.entity;

import lombok.Data;

import java.io.Serializable;

/**
 * Created by liuwu on 2018/8/1 0001.
 */
@Data
public class KeyByCounts implements Serializable{

    private String name;
    private Long counts;
}
