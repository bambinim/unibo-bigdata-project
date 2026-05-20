package project

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.hadoop.fs.{FileSystem, Path}
import java.time.LocalDateTime
import java.sql.Timestamp
import utils.Config

object DataProvider {

  // Explicit schema to handle type mismatches and skip unused problematic columns
  val tripSchema = StructType(Seq(
    StructField("tpep_pickup_datetime", TimestampType, true),
    StructField("tpep_dropoff_datetime", TimestampType, true),
    StructField("trip_distance", DoubleType, true),
    StructField("PULocationID", LongType, true),
    StructField("DOLocationID", LongType, true),
    StructField("fare_amount", DoubleType, true),
    StructField("tip_amount", DoubleType, true),
    StructField("total_amount", DoubleType, true)
  ))

  type TripRdd = RDD[
    (
        java.time.LocalDateTime,
        java.time.LocalDateTime,
        Double,
        Long,
        Long,
        Double,
        Double,
        Double
    )
  ]
  type ZoneRdd = RDD[(Long, String)]

  def fromConfig(sparkSession: SparkSession, conf: Config): DataProvider =
    conf.deploymentMode match {
      case "local" =>
        LocalDataProvider(sparkSession, conf.sampleData, conf.zonesLookupFile)
      case "aws" => S3DataProvider(sparkSession, conf.s3Bucket, conf.awsRegion)
      case _     =>
        throw new RuntimeException(
          s"Could not create data provider for deployment mode ${conf.deploymentMode}"
        )
    }

  trait DataProvider {

    def readTripData(): DataFrame
    def readZoneData(): DataFrame

    protected def preprocessTripDataFrame(df: DataFrame): TripRdd = df
      .select(
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "fare_amount",
        "tip_amount",
        "total_amount"
      )
      .rdd
      .map(row =>
        (
          row.getAs[Timestamp](0).toLocalDateTime, // Convert java.sql.Timestamp to java.time.LocalDateTime
          row.getAs[Timestamp](1).toLocalDateTime, // Convert java.sql.Timestamp to java.time.LocalDateTime
          row.getDouble(2), // trip_distance
          row.getLong(3), // PULocationID
          row.getLong(4), // DOLocationID
          row.getDouble(5), // fare_amount
          row.getDouble(6), // tip_amount
          row.getDouble(7) // total_amount
        )
      )

    protected def preprocessZonesDataFrame(df: DataFrame): ZoneRdd = df
      .drop("service_zone", "Zone")
      .rdd
      .map[(Long, String)](row =>
        (
          row.getInt(0), // LocationID
          row.getString(1) // Borough
        )
      )
    def loadData(): (TripRdd, ZoneRdd) = {
      return (
        preprocessTripDataFrame(readTripData()),
        preprocessZonesDataFrame(readZoneData())
      )
    }
  }

  case class LocalDataProvider(
      sparkSession: SparkSession,
      dataSample: String,
      zonesLookupFiles: String
  ) extends DataProvider {

    override def readTripData(): DataFrame =
      sparkSession.read
        .option("mergeSchema", "true")
        .schema(DataProvider.tripSchema)
        .parquet(dataSample)

    override def readZoneData(): DataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(zonesLookupFiles)

  }

  case class S3DataProvider(
      sparkSession: SparkSession,
      bucketName: String,
      awsZone: String
  ) extends DataProvider {

    override def readTripData(): DataFrame = {
      val dataPath = s"s3a://$bucketName/data"
      sparkSession.sparkContext.hadoopConfiguration
        .set("fs.s3a.endpoint", s"s3.$awsZone.amazonaws.com")
      
      val fs = FileSystem.get(
        new java.net.URI(dataPath),
        sparkSession.sparkContext.hadoopConfiguration
      )

      // List all parquet files in the directory
      val files = fs.listStatus(new Path(dataPath))
        .filter(_.getPath.getName.endsWith(".parquet"))
        .map(_.getPath.toString)

      if (files.isEmpty) {
        throw new RuntimeException(s"No Parquet files found in $dataPath")
      }

      // Read each file individually to allow vectorized reader to use local schemas,
      // then explicitly cast to our target schema.
      // Wrapped in a job group for better Spark UI readability.
      val dataframes = try {
        sparkSession.sparkContext.setJobGroup("load_trip_data", "Loading individual Parquet files from S3")
        files.map { file =>
          sparkSession.read.parquet(file)
            .select(DataProvider.tripSchema.fields.map(f => col(f.name).cast(f.dataType)): _*)
        }
      } finally {
        sparkSession.sparkContext.clearJobGroup()
      }

      // Union all individual dataframes
      dataframes.reduce(_ union _)
    }

    override def readZoneData(): DataFrame = {
      sparkSession.sparkContext.hadoopConfiguration
        .set("fs.s3a.endpoint", s"s3.$awsZone.amazonaws.com")
      sparkSession.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(s"s3a://$bucketName/zones.csv")
    }

  }

}
