package org.aga.sparkinpractice.exercices;

import org.apache.spark.sql.SparkSession;

public class MySparkDataFrame {

	public static void main(String[] args) {
		
			String salesFilePath = "../data/sales.csv";
	        String storeFilePath = "../data/stores.csv";
	        String timeByDayFilePath = "../data/time_by_day.csv";
	        String customerFilePath = "../data/customers.csv";
	        SparkSession sparkSession = buildSparkSession();

	}
		public static SparkSession buildSparkSession() {
	        SparkSession sparkSession = SparkSession
	                .builder()
	                .appName("Spark dataframe training")
	                .master("local[*]")
	                .config("spark.sql.warehouse.dir", "warehouseLocation") //adding config parameters
	                .getOrCreate();

	        return sparkSession;
}
}