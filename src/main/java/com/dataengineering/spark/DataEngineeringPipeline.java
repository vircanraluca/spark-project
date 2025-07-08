package com.dataengineering.spark;

import org.apache.spark.sql.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import scala.collection.JavaConverters;

public class DataEngineeringPipeline {
    private final SparkSession spark;



    public DataEngineeringPipeline() {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        spark = SparkSession.builder()
                .appName("Data Engineering Pipeline")
                .master("local[*]")
                .config("spark.sql.adaptive.enabled", "true")
                .getOrCreate();
    }

    public Dataset<Row> readCSV(String path, boolean header, String delimiter) {
        return spark.read()
                .option("header", header)
                .option("delimiter", delimiter)
                .csv(path);
    }

    public Dataset<Row> readJSON(String path) {
        return spark.read().json(path);
    }

    public Dataset<Row> readParquet(String path) {
        return spark.read().parquet(path);
    }

    public Dataset<Row> readFromDatabase(String url, String table, Properties props) {
        return spark.read().jdbc(url, table, props);
    }

    public Dataset<Row> filterData(Dataset<Row> df, String condition) {
        return df.filter(condition);
    }

    public Dataset<Row> performAggregations(Dataset<Row> df, String[] groupByCols, Map<String, String> aggregations) {
        RelationalGroupedDataset grouped = df.groupBy(Arrays.toString(groupByCols));
        return grouped.agg(aggregations);
    }

    public Dataset<Row> joinDatasets(Dataset<Row> df1, Dataset<Row> df2, String[] joinCols, String joinType) {
        return df1.join(df2, JavaConverters.asScalaBuffer(Arrays.asList(joinCols)).toList(), joinType);
    }




    public Dataset<Row> cleanAndValidateData(Dataset<Row> df) {
        return df.na().drop().dropDuplicates();
    }

    public void saveAsCSV(Dataset<Row> df, String path, boolean header) {
        df.write().option("header", header).mode(SaveMode.Overwrite).csv(path);
    }

    public void saveAsParquet(Dataset<Row> df, String path) {
        df.write().mode(SaveMode.Overwrite).parquet(path);
    }

    public void saveToDatabase(Dataset<Row> df, String url, String table, Properties props) {
        df.write().mode(SaveMode.Overwrite).jdbc(url, table, props);
    }

    public void logDatasetInfo(Dataset<Row> df, String name) {
        System.out.println("Dataset: " + name);
        df.printSchema();
        df.show(false);
    }

    public void monitorPerformance(String operation, Runnable task) {
        long start = System.currentTimeMillis();
        task.run();
        long end = System.currentTimeMillis();
        System.out.println("Operation '" + operation + "' took " + (end - start) + " ms");
    }

    public Dataset<Row> handleMissingValues(Dataset<Row> df, Map<String, Object> fillValues) {
        return df.na().fill(fillValues);
    }

    public Dataset<Row> selectColumns(Dataset<Row> df, String[] columns) {
        return df.selectExpr(columns);
    }

    public Dataset<Row> optimizeForLargeScale(Dataset<Row> df) {
        return df.repartition(8).persist(); // poți ajusta după caz
    }


    public SparkSession getSparkSession() {
        return spark;
    }
}
