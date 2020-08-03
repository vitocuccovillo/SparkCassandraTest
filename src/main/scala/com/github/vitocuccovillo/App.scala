package com.github.vitocuccovillo

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Encoders, Row, RowFactory, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkContext

import scala.collection.JavaConverters.asJavaIterableConverter

/**
 * Hello world!
 *
 */
object Main extends App {

  main()

  def main(): Unit = {

    println( "Hello World!" )

    val session = SparkSessionProvider.sparkSession

    val context = session.sparkContext

    populateCassandraDB(session, context)

    import org.apache.spark.sql.cassandra._
    //Spark connector
    import com.datastax.spark.connector._
    import com.datastax.spark.connector.cql.CassandraConnector

    val df = session
      .read
      .cassandraFormat("users", "mycatalog")
      .load()
      .filter("count > 5")

    df.explain

    df.show(10, false)

  }

  private def populateCassandraDB(session: SparkSession, sparkContext: SparkContext): Unit = {

    val cassandraConnector = CassandraConnector.apply(session.sparkContext)
    val sessionCassandra = cassandraConnector.openSession()

    sessionCassandra.execute("CREATE KEYSPACE IF NOT EXISTS mycatalog WITH REPLICATION = {'class': 'SimpleStrategy','replication_factor': 1}")
    sessionCassandra.execute("CREATE TABLE IF NOT EXISTS mycatalog.users (user  TEXT, word  TEXT, count INT, PRIMARY KEY (user, word))")

    val cnt = sparkContext
      .cassandraTable("mycatalog","users")
      .cassandraCount()

    println(s"Table has $cnt rows")

    if (cnt == 0) {
      List(
        "INSERT INTO mycatalog.users (user, word, count ) VALUES ( 'Russ', 'dino', 10 )",
        "INSERT INTO mycatalog.users (user, word, count ) VALUES ( 'Russ', 'fad', 5 )",
        "INSERT INTO mycatalog.users (user, word, count ) VALUES ( 'Sam', 'alpha', 3 )",
        "INSERT INTO mycatalog.users (user, word, count ) VALUES ( 'Zebra', 'zed', 100 )"
      ).foreach(q =>
        sessionCassandra.execute(q)
      )
    }
  }

  def createSparkDF(session: SparkSession): Unit = {

    import scala.collection.JavaConverters._

    val rows = (1 to 1000)
      .map(userId =>
        RowFactory.create(s"id_$userId", userId: java.lang.Integer)
      )
      .toList
      .asJava

    case class User(id: String, age: Int)

    val struct = StructType(
      Array(
        StructField("id", DataTypes.StringType, false),
        StructField("age", DataTypes.IntegerType, false)
      )
    )

    import session.implicits._

    val rowsDF = session
      .createDataFrame(rows, struct)
      .as[User]

    rowsDF.show(20, false)
  }

}
