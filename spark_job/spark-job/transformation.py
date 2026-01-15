from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, when, count, avg
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def main(env, bq_project, bq_dataset, transformed_table, route_insights_table, origin_insights_table):
    try:

        spark = SparkSession.builder \
                .appName("FlightBookingAnalysis") \
                .config("spark.sql.catalogImplementation", "hive") \
                .getOrCreate()

        logger.info("Spark Session created successfully.")
        input_path = f"gs://airflow-project-flights/source_{env}"

        logger.info(f"Input path resolved: {input_path}")

        data = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(input_path)
        
        logger.info("Data read successfully from Bucket.")

        #Adding derived columns
        transformed_data = data.withColumn("is_weekend", when(col("flight_day").isin("Saturday", "Sunday"), lit(1)).otherwise(lit(0)))

        transformed_data = transformed_data.withColumn("lead_time_category",
                        when(col("purchase_lead") < 7, lit("short_term"))
                        .when(col("purchase_lead").between(7, 30), lit("medium_term"))
                        .otherwise(lit("long_term")))

        transformed_data = transformed_data.withColumn("booking_success_rate", col("booking_complete") / col("num_passengers")
                                                       )
    
        #Adding aggregate columns

        #Calculating total number of bookings per route, avg flight duration & avg length of stay

        route_agg = transformed_data.groupby("route").agg(count("*").alias("total_bookings"),
                                                          avg("flight_duration").alias("avg_flight_duration"),
                                                          avg("length_of_stay").alias("avg_length_of_stay"))
        

        #Calculate bookings per booking origin

        booking_origin_insights = transformed_data.groupby("booking_origin").agg(count("*").alias("total_bookings_origin"),                                                                               
                                                                                avg("booking_success_rate").alias("avg_booking_success_rate"),
                                                                                avg("purchase_lead").alias("avg_purchase_lead"))   
        logger.info("Data transformation completed successfully.")

        #Write transforms to BigQuery


        transformed_data.write.format("bigquery") \
            .option("table", f"{bq_project}.{bq_dataset}.{transformed_table}") \
            .option("writeMethod", "direct") \
            .mode("overwrite") \
            .save()
        
        logger.info("Data for transformed table written successfully to BigQuery.")

        route_agg.write.format("bigquery") \
            .option("table", f"{bq_project}.{bq_dataset}.{route_insights_table}") \
            .option("writeMethod", "direct") \
            .mode("overwrite") \
            .save()

        logger.info("Data for route insights written successfully to BigQuery.")

        booking_origin_insights.write.format("bigquery") \
            .option("table", f"{bq_project}.{bq_dataset}.{origin_insights_table}") \
            .option("writeMethod", "direct") \
            .mode("overwrite") \
            .save()

        logger.info("Data for booking origin insights written successfully to BigQuery.")

    
    except Exception as e:
        logger.error(f"Error initializing Spark session: {e}")
        sys.exit(1)
    
    finally:
        spark.stop()
        logger.info("Spark Session stopped.")



if __name__ == "__main__":
    # Parse command-line arguments
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", required=True)
    parser.add_argument("--bq_project", required=True)
    parser.add_argument("--bq_dataset", required=True)
    parser.add_argument("--transformed_table", required=True)
    parser.add_argument("--route_insights_table", required=True)
    parser.add_argument("--origin_insights_table", required=True)
    
    args = parser.parse_args()
    main(args.env, args.bq_project, args.bq_dataset, args.transformed_table, args.route_insights_table, args.origin_insights_table)
    parser = argparse.ArgumentParser(description="Process flight booking data and write to BigQuery.")
    parser.add_argument("--env", required=True, help="Environment (e.g., dev, prod)")
    parser.add_argument("--bq_project", required=True, help="BigQuery project ID")
    parser.add_argument("--bq_dataset", required=True, help="BigQuery dataset name")
    parser.add_argument("--transformed_table", required=True, help="BigQuery table for transformed data")
    parser.add_argument("--route_insights_table", required=True, help="BigQuery table for route insights")
    parser.add_argument("--origin_insights_table", required=True, help="BigQuery table for booking origin insights")

    args = parser.parse_args()

    # Call the main function with parsed arguments
    main(
        env=args.env,
        bq_project=args.bq_project,
        bq_dataset=args.bq_dataset,
        transformed_table=args.transformed_table,
        route_insights_table=args.route_insights_table,
        origin_insights_table=args.origin_insights_table
    )