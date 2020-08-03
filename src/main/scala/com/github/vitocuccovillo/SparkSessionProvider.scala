package com.github.vitocuccovillo

import com.datastax.spark.connector.CassandraSparkExtensions
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionProvider {

  private val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("TestCassandra")
    .set("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:2.5.1")
    .set("spark.sql.catalog.mycatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")
    .set("spark.sql.catalog.mycatalog.spark.cassandra.connection.host", "127.0.0.1")
    .set("spark.sql.catalog.mycatalog.spark.cassandra.connection.port", "32769")
    .set("spark.cassandra.connection.host","127.0.0.1")
    .set("spark.cassandra.connection.port", "32769")
    .set("spark.sql.extensions","com.datastax.spark.connector.CassandraSparkExtensions")

  val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    //.withExtensions(new CassandraSparkExtensions)
    .getOrCreate()

}
