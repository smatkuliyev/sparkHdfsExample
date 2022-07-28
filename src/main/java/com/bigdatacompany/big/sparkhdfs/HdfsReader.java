package com.bigdatacompany.big.sparkhdfs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HdfsReader {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local").appName("Hdfs Reader").getOrCreate();

        //Dataset<Row> rowData = sparkSession.read().option("header", true).csv("hdfs://localhost:8020/data/movies.csv");
        //Dataset<Row> mileniumDS = rowData.filter(rowData.col("title").contains("(2000)"));
        //mileniumDS.show();
        //mileniumDS.write().csv("hdfs://localhost:8020/data/milenium");

        Dataset<Row> rowData = sparkSession.read().option("header", true).csv("hdfs://localhost:8020/data/ratings.csv");
        Dataset<Row> userId = rowData.groupBy("userId").count();
        //userId.write().csv("hdfs://localhost:8020/data/useridcount.csv");
        //userId.write().csv("hdfs://localhost:8020/data/useridcount");

        // to set partitions - coalesce
        //userId.coalesce(1).write().csv("hdfs://localhost:8020/data/useridcount");

        //to set partitons by column - partitionBy
        userId.coalesce(1).write().partitionBy("userId").csv("hdfs://localhost:8020/data/useridcount");

    }
}
