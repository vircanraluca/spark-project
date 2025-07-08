package com.dataengineering.spark.examples;

import com.dataengineering.spark.DataEngineeringPipeline;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Logger;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Exemple practice de utilizare a pipeline-ului de Data Engineering
 */
public class UsageExamples {

    private static final Logger logger = Logger.getLogger(UsageExamples.class);
    private DataEngineeringPipeline pipeline;

    public UsageExamples() {
        pipeline = new DataEngineeringPipeline();
    }

    /**
     * Exemplu 1: Procesare fișiere CSV și agregare date
     */
    public void processCSVExample() {
        logger.info("=== Exemplu 1: Procesare CSV ===");

        try {
            // Citire date vânzări
            Dataset<Row> sales = pipeline.readCSV("C:\\Users\\Raluca PC\\IdeaProjects\\spark-data-engineering\\src\\main\\data\\sales_2024.csv", true, ",");

            // Curățare date
            sales = pipeline.cleanAndValidateData(sales);

            // Filtrare doar vânzări peste 1000 RON
            sales = pipeline.filterData(sales, "amount > 1000");

            // Agregare pe categorii
            Map<String, String> aggregations = new HashMap<>();
            aggregations.put("amount", "sum");
            aggregations.put("quantity", "sum");
            aggregations.put("order_id", "count");

            Dataset<Row> salesByCategory = pipeline.performAggregations(
                    sales,
                    new String[]{"category", "month"},
                    aggregations
            );

            // Salvare rezultate
            pipeline.saveAsCSV(salesByCategory, "output/sales_by_category", true);
            pipeline.logDatasetInfo(salesByCategory, "Vânzări pe Categorii");

        } catch (Exception e) {
            logger.error("Eroare în procesare CSV: " + e.getMessage(), e);
        }
    }

    /**
     * Exemplu 2: Join între multiple surse de date
     */
    public void multiSourceJoinExample() {
        logger.info("=== Exemplu 2: Join Multiple Surse ===");

        try {
            // Citire date din diferite surse
            Dataset<Row> customers = pipeline.readCSV("C:\\Users\\Raluca PC\\IdeaProjects\\spark-data-engineering\\src\\main\\data\\customers.csv", true, ",");
            Dataset<Row> orders = pipeline.readJSON("C:\\Users\\Raluca PC\\IdeaProjects\\spark-data-engineering\\src\\main\\data\\orders.json");
            Dataset<Row> products = pipeline.readParquet("data/products.parquet");

            // Join orders cu customers
            Dataset<Row> ordersWithCustomers = pipeline.joinDatasets(
                    orders,
                    customers,
                    new String[]{"customer_id"},
                    "left"
            );

            // Join cu products
            Dataset<Row> completeData = pipeline.joinDatasets(
                    ordersWithCustomers,
                    products,
                    new String[]{"product_id"},
                    "left"
            );

            // Selectare coloane relevante
            Dataset<Row> finalData = pipeline.selectColumns(
                    completeData,
                    new String[]{"order_id", "customer_name", "product_name",
                            "quantity", "price", "order_date"}
            );

            // Salvare în Parquet pentru performanță
            pipeline.saveAsParquet(finalData, "output/complete_orders");

        } catch (Exception e) {
            logger.error("Eroare în join: " + e.getMessage(), e);
        }
    }

    /**
     * Exemplu 3: Procesare date din baza de date
     */
    public void databaseProcessingExample() {
        logger.info("=== Exemplu 3: Procesare Date din DB ===");

        try {
            // Configurare conexiune MySQL
            Properties mysqlProps = new Properties();
            mysqlProps.put("user", "dataeng_user");
            mysqlProps.put("password", "secure_password");
            mysqlProps.put("driver", "com.mysql.jdbc.Driver");

            String mysqlUrl = "jdbc:mysql://localhost:3306/sales_db";

            // Citire tabele
            Dataset<Row> transactions = pipeline.readFromDatabase(
                    mysqlUrl, "transactions", mysqlProps
            );

            Dataset<Row> customers = pipeline.readFromDatabase(
                    mysqlUrl, "customers", mysqlProps
            );

            // Procesare
            transactions = pipeline.filterData(transactions, "status = 'COMPLETED'");

            // Join și agregare
            Dataset<Row> customerStats = pipeline.joinDatasets(
                    transactions,
                    customers,
                    new String[]{"customer_id"},
                    "inner"
            );

            Map<String, String> aggregations = new HashMap<>();
            aggregations.put("amount", "sum");
            aggregations.put("amount", "avg");
            aggregations.put("transaction_id", "count");

            customerStats = pipeline.performAggregations(
                    customerStats,
                    new String[]{"customer_id", "customer_segment"},
                    aggregations
            );

            // Salvare înapoi în DB
            pipeline.saveToDatabase(
                    customerStats,
                    mysqlUrl,
                    "customer_statistics",
                    mysqlProps
            );

        } catch (Exception e) {
            logger.error("Eroare în procesare DB: " + e.getMessage(), e);
        }
    }

    /**
     * Exemplu 4: Utilizare SQL pentru analize complexe
     */
    public void complexSQLAnalysis() {
        logger.info("=== Exemplu 4: Analize SQL Complexe ===");

        try {
            // Citire date
            Dataset<Row> sales = pipeline.readCSV("C:\\Users\\Raluca PC\\IdeaProjects\\spark-data-engineering\\src\\main\\data\\sales_full.csv", true, ",");
            Dataset<Row> products = pipeline.readCSV("C:\\Users\\Raluca PC\\IdeaProjects\\spark-data-engineering\\src\\main\\data\\products_full.csv", true, ",");
            Dataset<Row> stores = pipeline.readCSV("C:\\Users\\Raluca PC\\IdeaProjects\\spark-data-engineering\\src\\main\\data\\stores.csv", true, ",");

            // Creare view-uri temporare
            sales.createOrReplaceTempView("sales");
            products.createOrReplaceTempView("products");
            stores.createOrReplaceTempView("stores");

            // Query 1: Top 10 produse pe regiuni
            String topProductsQuery = """
                WITH regional_sales AS (
                    SELECT
                        s.product_id,
                        p.product_name,
                        st.region,
                        SUM(s.quantity) as total_quantity,
                        SUM(s.amount) as total_revenue
                    FROM sales s
                    JOIN products p ON s.product_id = p.product_id
                    JOIN stores st ON s.store_id = st.store_id
                    WHERE s.sale_date >= '2024-01-01'
                    GROUP BY s.product_id, p.product_name, st.region
                ),
                ranked_products AS (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (PARTITION BY region ORDER BY total_revenue DESC) as rank
                    FROM regional_sales
                )
                SELECT * FROM ranked_products WHERE rank <= 10
                ORDER BY region, rank
            """;

            Dataset<Row> topProducts = pipeline.getSparkSession().sql(topProductsQuery);
            pipeline.saveAsCSV(topProducts, "output/top_products_by_region", true);

            // Query 2: Analiza trend lunar
            String trendQuery = """
                SELECT
                    DATE_FORMAT(sale_date, 'yyyy-MM') as month,
                    p.category,
                    COUNT(DISTINCT s.customer_id) as unique_customers,
                    COUNT(*) as transactions,
                    SUM(s.amount) as revenue,
                    AVG(s.amount) as avg_transaction_value
                FROM sales s
                JOIN products p ON s.product_id = p.product_id
                GROUP BY DATE_FORMAT(sale_date, 'yyyy-MM'), p.category
                ORDER BY month, revenue DESC
            """;

            Dataset<Row> monthlyTrend = pipeline.getSparkSession().sql(trendQuery);
            pipeline.saveAsParquet(monthlyTrend, "output/monthly_trends");

        } catch (Exception e) {
            logger.error("Eroare în analiza SQL: " + e.getMessage(), e);
        }
    }

    /**
     * Exemplu 5: Pipeline complet cu monitorizare performanță
     */
    public void fullPipelineWithMonitoring() {
        logger.info("=== Exemplu 5: Pipeline Complet cu Monitorizare ===");

        SparkSession spark = pipeline.getSparkSession();

        try {
            // Monitorizare citire date
            pipeline.monitorPerformance("Citire date inițiale", () -> {
                Dataset<Row> rawData = pipeline.readCSV("data/large_dataset.csv", true, ",");
                rawData.cache(); // Cache pentru reutilizare
                logger.info("Date citite: " + rawData.count() + " rânduri");
            });

            // Procesare în etape cu monitorizare
            Dataset<Row> processedData = null;

            pipeline.monitorPerformance("Curățare și validare", () -> {
                Dataset<Row> data = spark.table("raw_data");

                // Eliminare valori null
                Map<String, Object> fillValues = new HashMap<>();
                fillValues.put("quantity", 0);
                fillValues.put("price", 0.0);
                fillValues.put("category", "Unknown");

                data = pipeline.handleMissingValues(data, fillValues);
                data = pipeline.cleanAndValidateData(data);

                data.createOrReplaceTempView("cleaned_data");
            });

            pipeline.monitorPerformance("Transformări complexe", () -> {
                String transformQuery = """
                    SELECT
                        *,
                        CASE
                            WHEN amount > 10000 THEN 'HIGH'
                            WHEN amount > 1000 THEN 'MEDIUM'
                            ELSE 'LOW'
                        END as value_category,
                        amount * quantity as total_value,
                        DATEDIFF(current_date(), order_date) as days_since_order
                    FROM cleaned_data
                    WHERE status IN ('COMPLETED', 'SHIPPED')
                """;

                Dataset<Row> transformed = spark.sql(transformQuery);
                transformed.createOrReplaceTempView("transformed_data");
            });

            pipeline.monitorPerformance("Agregări finale", () -> {
                String aggregationQuery = """
                    SELECT
                        value_category,
                        category,
                        COUNT(*) as order_count,
                        SUM(total_value) as total_revenue,
                        AVG(total_value) as avg_order_value,
                        STDDEV(total_value) as stddev_order_value,
                        MIN(days_since_order) as min_days,
                        MAX(days_since_order) as max_days
                    FROM transformed_data
                    GROUP BY value_category, category
                    WITH ROLLUP
                """;

                Dataset<Row> aggregated = spark.sql(aggregationQuery);

                // Optimizare pentru volume mari
                aggregated = pipeline.optimizeForLargeScale(aggregated);

                // Salvare cu partitionare
                aggregated.write()
                        .mode("overwrite")
                        .partitionBy("value_category")
                        .parquet("output/aggregated_results");
            });

            logger.info("Pipeline complet executat cu succes!");

        } catch (Exception e) {
            logger.error("Eroare în pipeline complet: " + e.getMessage(), e);
        }
    }

    /**
     * Exemplu 6: Procesare streaming simulată
     */
    public void batchProcessingExample() {
        logger.info("=== Exemplu 6: Procesare Batch (Simulare Streaming) ===");

        try {
            // Simulare procesare în batch-uri
            for (int batch = 1; batch <= 5; batch++) {
                final int batchNum = batch;

                pipeline.monitorPerformance("Procesare Batch " + batchNum, () -> {
                    // Citire batch de date
                    Dataset<Row> batchData = pipeline.readCSV(
                            "data/batch_" + batchNum + ".csv", true, ","
                    );

                    // Procesare rapidă
                    batchData = pipeline.filterData(batchData, "status = 'NEW'");

                    // Agregare incrementală
                    Map<String, String> aggregations = new HashMap<>();
                    aggregations.put("amount", "sum");
                    aggregations.put("order_id", "count");

                    Dataset<Row> batchAggregated = pipeline.performAggregations(
                            batchData,
                            new String[]{"hour", "category"},
                            aggregations
                    );

                    // Append la rezultate existente
                    batchAggregated.write()
                            .mode("append")
                            .parquet("output/incremental_results");

                    logger.info("Batch " + batchNum + " procesat cu succes");
                });

                // Simulare delay între batch-uri
                Thread.sleep(2000);
            }

        } catch (Exception e) {
            logger.error("Eroare în procesare batch: " + e.getMessage(), e);
        }
    }

    /**
     * Main method pentru rulare exemple
     */
    public static void main(String[] args) {
        UsageExamples examples = new UsageExamples();

        try {
            if (args.length == 0) {
                logger.info("Rulare toate exemplele...");
                examples.processCSVExample();
                examples.multiSourceJoinExample();
                examples.complexSQLAnalysis();
                examples.fullPipelineWithMonitoring();
            } else {
                switch (args[0]) {
                    case "csv":
                        examples.processCSVExample();
                        break;
                    case "join":
                        examples.multiSourceJoinExample();
                        break;
                    case "db":
                        examples.databaseProcessingExample();
                        break;
                    case "sql":
                        examples.complexSQLAnalysis();
                        break;
                    case "full":
                        examples.fullPipelineWithMonitoring();
                        break;
                    case "batch":
                        examples.batchProcessingExample();
                        break;
                    default:
                        logger.error("Exemplu necunoscut: " + args[0]);
                }
            }
        } finally {
            // Închidere Spark Session
            if (examples.pipeline.getSparkSession() != null) {
                examples.pipeline.getSparkSession().stop();
            }
        }
    }
}