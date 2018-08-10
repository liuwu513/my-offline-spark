package com.woody.spark.config;

/**
 * Created by liuwu on 2017/9/15 0015.
 */
public class ESProperties {

    /**
     * 集群名称
     */
    public static String CLUSTER_NAME = "my-cluster";

    /**
     * Elasticsearch服务端 IP地址，开放主节点
     */
    public static String IP = "192.168.1.2";

    /**
     * 端口名称
     */
    public static String PORT = "9200";

    /**
     * 安全用户
     */
    public static String SECURITY_USER = "elastic";

    /**
     * 安全密码
     */
    public static String SECURITY_PWD = "elastic";

    /**
     * 自动嗅探集群节点
     */
    public static boolean TRANSPORT_SNIFF = true;

    /**
     * 执行sql语句时在elasticsearch中执行只返回需要的数据。这个参数在查询时设置比较有用
     */
    public static String PUSH_DOWN = "true";


    /**
     *如果没有这个index自动创建
     */
    public static String INDEX_AUTO_CREATE = "true";

}
