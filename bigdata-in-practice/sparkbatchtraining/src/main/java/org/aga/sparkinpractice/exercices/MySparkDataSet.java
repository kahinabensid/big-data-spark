package org.aga.sparkinpractice.exercices;

import static org.apache.spark.sql.functions.col;

import org.aga.sparkinpractice.model.Sale;
import org.aga.sparkinpractice.utils.Util;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MySparkDataSet {
private static Encoder<Sale> encoder = Encoders.bean(Sale.class);
	public static void main(String[] args) throws Exception {
		
		 String salesFilePath = "data/sales.csv";
	        String storeFilePath = "data/stores.csv";
	        String timeByDayFilePath = "data/time_by_day.csv";
	        String customerFilePath = "data/customers.csv";
	        SparkSession sparkSession = buildSparkSession();
	        
		
		readFileAsDataFrameByInferringSchema(sparkSession, "data/sales.csv");	
		computeStoreCAByGroupByAndSum(sparkSession, salesFilePath);
		 
	}

	public static SparkSession buildSparkSession() {
		SparkSession sparkSession = SparkSession
				.builder()
				.appName("Spark dataframe training")
				.master("local[*]")
				.config("spark.sql.warehouse.dir", "warehouseLocation")
				.getOrCreate();

		return sparkSession;
	}

	public static Dataset<Row> readFileAsDataFrameByInferringSchema(SparkSession sparkSession, String filePath) {
		Dataset<Row> salesAsDFWithoutSchemaInferring = sparkSession.read()
				.format("csv")
				.option("sep", ";")
				.option("header", "false")
				.load(filePath);
		salesAsDFWithoutSchemaInferring.printSchema();
		
		salesAsDFWithoutSchemaInferring.show();
		return salesAsDFWithoutSchemaInferring;
	
		
	}
	  public static Dataset<Row> readFileAsDataFrameWithSchema(SparkSession sparkSession, String filePath) throws Exception {

	        Dataset<Row> salesAsDFWithSchemaDefined = sparkSession
	                .read()
	                .format("csv")
	                .option("sep", ";")
	                .option("header", "false")
	                .schema(Util.buildSchema(Sale.class))
	                .load(filePath);
	        return salesAsDFWithSchemaDefined;
	        
	        //4444
}
	  public static Dataset<Row> computeStoreCAByGroupByAndSum(SparkSession sparkSession, String filePath) throws Exception {
	        Dataset<Row> caByStore = readFileAsDataFrameByInferringSchema(sparkSession, filePath)
	        		.as(encoder)
	        		.select(col("storeId"), col("storeSales")
	        		.multiply(col("unitSales"))
	        		.as("rowCA"))
	                .groupBy(col("storeId"))
	                .sum("rowCA").as("ca");
	                 caByStore.show();
	        return caByStore;
	        
	    }
}
