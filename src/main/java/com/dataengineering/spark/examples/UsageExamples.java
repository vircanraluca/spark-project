package com.dataengineering.spark.examples;

import com.dataengineering.spark.DataEngineeringPipeline;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Logger;
import scala.collection.Seq;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.spark.sql.functions.*;

/**
 * Exemple practice de utilizare a pipeline-ului de Data Engineering
 */
public class UsageExamples {
    private static final Logger logger = Logger.getLogger(UsageExamples.class);
    private final SparkSession spark;
    private final DataEngineeringPipeline pipeline;

    public UsageExamples() {
        this.spark = SparkSession.builder()
                .appName("Data Engineering Examples")
                .master("local[*]")
                .getOrCreate();

        this.pipeline = new DataEngineeringPipeline();
    }


    public void missingDataAnalysis() {
        logger.info("=== Exemplu 7: Analiza Valorilor Lipsă ===");

        Dataset<Row> sales = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("C:\\Users\\Raluca PC\\IdeaProjects\\spark-data-engineering\\src\\main\\data\\sales_2024.csv");

        Dataset<Row> nullCounts = sales.selectExpr(
                "sum(case when product_id is null then 1 else 0 end) as null_product_id",
                "sum(case when amount is null then 1 else 0 end) as null_amount",
                "sum(case when quantity is null then 1 else 0 end) as null_quantity"
        );


        nullCounts.show();
    }

    public void orderCycleTimeAnalysis() {
        logger.info("=== Exemplu 8: Analiza Duratei Comenzii ===");

        Dataset<Row> sales = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("C:\\Users\\Raluca PC\\IdeaProjects\\spark-data-engineering\\src\\main\\data\\sales_2024.csv");

        sales = sales.withColumn("order_duration", expr("DATEDIFF(sale_date, order_date)"));

        Dataset<Row> avgDuration = sales.withColumn(
                        "order_duration", expr("DATEDIFF(sale_date, order_date)")
                ).groupBy("store_id")  // Sau "status", "month", etc.
                .agg(avg("order_duration").alias("average_duration"))
                .orderBy(desc("average_duration"));


        avgDuration.show();
    }

    public void detectPriceOutliers() {
        logger.info("=== Exemplu 9: Detectare Prețuri Suspecte ===");

        Dataset<Row> products = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("C:\\Users\\Raluca PC\\IdeaProjects\\spark-data-engineering\\src\\main\\data\\products_full.csv");

        Dataset<Row> outliers = products.filter("price < 10 OR price > 10000");

        outliers.show();
    }

    public void currencyConversionExample() {
        logger.info("=== Exemplu 10: Conversie RON în EUR ===");

        Dataset<Row> sales = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("C:\\Users\\Raluca PC\\IdeaProjects\\spark-data-engineering\\src\\main\\data\\sales_2024.csv");

        double exchangeRate = 0.2; // Exemplu: 1 RON = 0.2 EUR

        Dataset<Row> converted = sales.withColumn("amount_eur", col("amount").multiply(exchangeRate));
        converted.select("order_id", "amount", "amount_eur").show(10);
    }

    public void processCSVExample() {
        logger.info("=== CSV Processing Example ===");

        Dataset<Row> sales = pipeline.readCSV("C:\\Users\\Raluca PC\\IdeaProjects\\spark-data-engineering\\src\\main\\data\\sales_2024.csv", true, ",");
        sales = pipeline.cleanAndValidateData(sales);
        sales = pipeline.filterData(sales, "amount > 50");

        Map<String, String> aggregations = new HashMap<>();
        aggregations.put("amount", "sum");
        aggregations.put("quantity", "sum");
        aggregations.put("order_id", "count");

        Dataset<Row> products = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/data/products_full.csv");

        Dataset<Row> salesByCategory = sales.join(products, "product_id");

        pipeline.saveAsCSV(salesByCategory, "output/sales_by_category", true);
        pipeline.logDatasetInfo(salesByCategory, "Sales by Category");
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("JoinSalesWithProducts")
                .master("local[*]")
                .getOrCreate();

        // 1. Citire fișier sales
        Dataset<Row> sales = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("C:\\Users\\Raluca PC\\IdeaProjects\\spark-data-engineering\\src\\main\\data\\sales_2024.csv");

        // 2. Citire fișier products
        Dataset<Row> products = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("C:\\Users\\Raluca PC\\IdeaProjects\\spark-data-engineering\\src\\main\\data\\products_full.csv");

        Dataset<Row> salesClean = sales.drop("category");

        Dataset<Row> joined = salesClean.join(products, "product_id");

        Dataset<Row> selected = joined.select(
                joined.col("product_id"),
                joined.col("category"),
                joined.col("amount"),
                joined.col("quantity")
        );

        Dataset<Row> aggregated = selected.groupBy("category")
                .agg(
                        sum("amount").alias("total_sales"),
                        sum("quantity").alias("total_quantity")
                )
                .orderBy(desc("total_sales"));

        aggregated.show();


        // 1. Cele mai vândute produse (top 10 după amount)
        Dataset<Row> topProducts = joined.select("product_id", "product_name", "amount")
                .groupBy("product_id", "product_name")
                .agg(sum("amount").alias("total_sales"))
                .orderBy(desc("total_sales"))
                .limit(10);

        System.out.println("Top 10 cele mai vândute produse:");
        topProducts.show();

        // 2. Vânzări pe lună (month)
        Dataset<Row> monthlySales = joined.groupBy("month")
                .agg(sum("amount").alias("total_sales"))
                .orderBy("month");

        System.out.println("Vânzări lunare:");
        monthlySales.show();

        //3. Vânzări per magazin (store_id)
        Dataset<Row> salesByStore = joined.groupBy("store_id")
                .agg(sum("amount").alias("total_sales"))
                .orderBy(desc("total_sales"));

        System.out.println("Vânzări pe magazin:");
        salesByStore.show();

        //4. Vânzări medii per categorie
        Dataset<Row> averageByCategory = selected.groupBy("category")
                .agg(
                        sum("amount").divide(sum("quantity")).alias("average_price")
                )
                .orderBy(desc("average_price"));

        System.out.println("Preț mediu per categorie:");
        averageByCategory.show();

        //5. Număr total de produse vândute per categorie
        Dataset<Row> countByCategory = selected.groupBy("category")
                .agg(sum("quantity").alias("total_units"))
                .orderBy(desc("total_units"));

        System.out.println("Număr total de produse vândute per categorie:");
        countByCategory.show();


        Dataset<Row> salesByStatus = joined.groupBy("status")
                .agg(sum("amount").alias("total_sales"))
                .orderBy(desc("total_sales"));

        System.out.println("Vânzări per status comandă:");
        salesByStatus.show();

        Dataset<Row> salesPerMonthCategory = joined.groupBy("month", "category")
                .agg(sum("amount").alias("monthly_sales"))
                .orderBy(col("month"), desc("monthly_sales"));

        System.out.println("Vânzări lunare per categorie:");
        salesPerMonthCategory.show();

        Dataset<Row> bestSellingByQuantity = selected.groupBy("category")
                .agg(sum("quantity").alias("total_quantity"))
                .orderBy(desc("total_quantity"));

        System.out.println("Categorii cele mai vândute (după cantitate):");
        bestSellingByQuantity.show();

        Dataset<Row> avgRevenuePerProduct = joined.groupBy("product_id", "product_name")
                .agg(
                        sum("amount").alias("total_sales"),
                        sum("quantity").alias("total_qty"),
                        sum("amount").divide(sum("quantity")).alias("avg_price_per_unit")
                )
                .orderBy(desc("avg_price_per_unit"));

        System.out.println("Produse cu încasări medii per unitate cele mai mari:");
        avgRevenuePerProduct.show();


        Dataset<Row> ordersPerCustomer = joined.groupBy("customer_id")
                .count()
                .orderBy(desc("count"));

        System.out.println("Număr comenzi per client:");
        ordersPerCustomer.show();

        UsageExamples examples = new UsageExamples();

        examples.missingDataAnalysis();
        examples.orderCycleTimeAnalysis();
        examples.detectPriceOutliers();
        examples.currencyConversionExample();

        examples.processCSVExample();
        examples.spark.stop();

        // Închide sesiunea
        spark.stop();


    }
}
