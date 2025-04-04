import logging
import psycopg2
from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import col,lit,when,current_date
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DateType
import pandas

logging.basicConfig(level=logging.INFO,format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

if __name__=="__main__":

    logger.info("Application Start")

    spark = SparkSession.builder.appName("SCD IMPLEMENTATION")\
            .config("spark.ui.port","4041")\
            .config("spark.jars","file:///C://Users/shree/Downloads/postgresql-42.7.4.jar")\
            .config("spark.sql.execution.arrow.pyspark.enabled","true")\
            .getOrCreate()


    logger.info(spark.conf.get("spark.sql.execution.arrow.pyspark.enabled"))



    source_schema  = StructType([
                                    StructField("Customer_Id",IntegerType(),nullable = False),
                                    StructField("Name",StringType(),nullable=False),
                                    StructField("City",StringType(),nullable=False),
                                    StructField("Age",IntegerType(),nullable=False),
                                    StructField("update_date",DateType(),nullable=False)
    ])

    sourcedf = spark.read.option("header",True).option("inferSchema","true").schema(source_schema).csv(r"C:\Users\shree\PycharmProjects\SCD-Implementation\source.csv")

    logger.info("SOURCE DATAFRAME")

    sourcedf.show(5,truncate=False)




    connection = psycopg2.connect(

                    host = "localhost",
                    user = "postgres",
                    password = "Pkts1t4j11@",
                    port = "5432",
                    database = "postgres"
    )


    cursor = connection.cursor()
    cursor.execute("SELECT max(start_date) from target_table where is_current = 'Y';")
    last_update = cursor.fetchone()[0]

    logger.info("last_update : "+str(last_update))


    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    query = """
                (SELECT  *
                FROM
                target_table
                WHERE
                is_current = 'Y') AS subquery 
    """

    connection_properties = {

        "user" : "postgres",
        "password" : "Pkts1t4j11@",
        "driver" : "org.postgresql.Driver"
    }


    target_df = spark.read.jdbc(url=jdbc_url,table=query,properties=connection_properties)

    logger.info("TARGET DATAFRAME")
    target_df.orderBy(col("customer_id")).show(5,truncate=False)


    joined_df = sourcedf.alias("src").join(target_df.alias("tgt"),col("src.Customer_Id") == col("tgt.customer_id"),"left")

    logger.info("JOINED DATAFRAME")
    joined_df.show(truncate=False)


    """
    Finding the records which need to be inserted , that means which are completely new. 
    """

    insertdf = joined_df.filter(col("tgt.customer_id").isNull() & col("tgt.name").isNull()).select(col("src.*"))

    insertdf = insertdf.withColumn("start_date",col("src.update_date"))\
                       .withColumn("end_date",lit(None))\
                       .withColumn("is_current",lit("Y"))\
                       .drop("update_date")

    logger.info("INSERT DATAFRAME")
    insertdf.show(truncate=False)



    """
    Finding the records to be updated ,that means which needs to be updated.
    """

    updated_df = joined_df.filter((col("tgt.customer_id").isNotNull() ) & (

        (col("src.Name") != col("tgt.name")) |
        (col("src.City") != col("tgt.city")) |
        (col("src.Age")  != col("tgt.age"))
    )).select(col("src.*"),col("tgt.end_date"))

    forexpiredf = joined_df.filter((col("tgt.customer_id").isNotNull() ) & (

        (col("src.Name") != col("tgt.name")) |
        (col("src.City") != col("tgt.city")) |
        (col("src.Age")  != col("tgt.age"))
    )).select(col("tgt.*"))
#,col("tgt.end_date")
    expired_records = forexpiredf.withColumn("end_date", current_date()).withColumn("is_current", lit("N")).drop("update_date")
    logger.info("EXPIRED DATAFRAME")
    expired_records.orderBy(col("Customer_Id")).show(truncate=False)
    updated_df = updated_df.withColumn("start_date",current_date())\
                           .withColumn("is_current",lit("Y"))\
                           .withColumn("City",col("src.City"))


    updated_df = updated_df.orderBy(col("Customer_Id")).select(col("Customer_Id"),col("Name"),col("City"),col("Age"),col("start_date"),col("end_date"),col("is_current"))

    logger.info("UPDATE DATAFRAME")
    updated_df.show(truncate=False)

    final_insert_df = insertdf.union(updated_df)
    logger.info("FINAL DATAFRAME TO WRITE")
    final_insert_df.orderBy(col("Customer_Id")).show(truncate=False)

    expired_records_pd = expired_records.select("customer_id", "end_date", "is_current").toPandas()

    logger.info("EXPIRED RECORDS in DATABASE")
    for _, row in expired_records_pd.iterrows():
        cursor.execute("""
            UPDATE target_table
            SET end_date = %s, is_current = 'N'
            WHERE customer_id = %s AND is_current = 'Y';
        """, (row["end_date"], row["customer_id"]))

    connection.commit()
    cursor.close()
    connection.close()
    # expired_records.write.jdbc(url=jdbc_url,table="target_table",mode="append",properties=connection_properties)
    logger.info("UPDATE & INSERT RECORDS in DATABASE")
    final_insert_df.write.jdbc(url=jdbc_url, table="target_table", mode="append", properties=connection_properties)

    logger.info("Application End")

