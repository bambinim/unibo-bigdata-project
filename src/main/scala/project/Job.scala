package project

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import utils.Config
import utils.JobResult
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import DataProvider.{TripRdd, ZoneRdd}

trait Job {

  val sparkSession =
    SparkSession.builder.appName("BigDataProject").getOrCreate()

  implicit val localDateTimeOrdering: Ordering[LocalDateTime] =
    Ordering.fromLessThan(_ isBefore _)

  def initializeSparkContext(config: Config): Unit = {}

  def loadData(config: Config): (TripRdd, ZoneRdd) =
    DataProvider.fromConfig(sparkSession, config).loadData()

  def writeResults(results: DataFrame, jobName: String): Unit

  def runStandardJob(tripRdd: TripRdd, zonesRdd: ZoneRdd): Unit = {
    import sparkSession.implicits._

    // prepare min and max dates to calculate deltas
    val tripDepTimes = tripRdd.map(_._1)
    val tripDataMinDate = tripDepTimes.min()
    val tripDataMaxDate = tripDepTimes.max()

    // Job 1: Average trips per borough by day
    val daysDelta = ChronoUnit.DAYS.between(tripDataMinDate, tripDataMaxDate)
  
    val tripsPerBorough = tripRdd
      .map(t => (t._4, 1))
      .join(zonesRdd)
      .map(t => (t._2._2, 1))
      .reduceByKey(_ + _)
      .mapValues(_ / daysDelta)
    writeResults(
      tripsPerBorough.toDF("borough", "trips"),
      "1_trips_per_borough"
    )

    // Job 2: Average trips number by hour
    val tripsPerTimeSlot = tripRdd
      .map(t => (t._1.getHour, 1))
      .reduceByKey(_ + _)
      .mapValues(_ / daysDelta)
    writeResults(tripsPerTimeSlot.toDF("hour", "trips"), "2_trips_per_hour")

    // Job 3: Average trips by month
    val yearsDelta = ChronoUnit.YEARS.between(tripDataMinDate, tripDataMaxDate)
    val tripsPerMonth = tripRdd
      .map(t => (t._1.getMonth.toString, 1))
      .reduceByKey(_ + _)
      .mapValues(_ / yearsDelta)
    writeResults(tripsPerMonth.toDF("month", "trips"), "3_trips_per_month")

    // Job 4: Average speed by departing hour
    val speedPerTimeSlot = tripRdd
      .map(t =>
        (
          t._1.getHour,
          (
            t._3,
            ChronoUnit.SECONDS
              .between(t._1, t._2)
              .doubleValue / 3600.doubleValue
          )
        )
      )
      .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
      .mapValues(v => v._1 / v._2)
    writeResults(speedPerTimeSlot.toDF("hour", "avg_speed"), "4_average_speed")

    // Job 5: Average tip by destination borough
    val tipsByDestination = tripRdd
      .map(t => (t._5, (t._7, 1)))
      .join(zonesRdd)
      .map(t => (t._2._2, t._2._1))
      .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
      .mapValues(v => v._1 / v._2)
    writeResults(
      tipsByDestination.toDF("destination_borough", "avg_tips"),
      "5_average_tips_by_borough"
    )
  }

  def runOptimizedJob(tripRdd: TripRdd, zonesRdd: ZoneRdd): Unit = {
    import sparkSession.implicits._

    // Using persist with StorageLevel for more control over caching behavior.
    // val tripRdd = uncachedTripRdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    // Broadcast the zones lookup table to avoid joins
    val broadcastZones =
      sparkSession.sparkContext.broadcast(zonesRdd.collectAsMap())

    // prepare min and max dates to calculate deltas
    val tripDepTimes = tripRdd.map(_._1)
    val tripDataMinDate = tripDepTimes.min()
    val tripDataMaxDate = tripDepTimes.max()

    // Job 1: Average trips per borough by day
    // Optimized using broadcast ad flatmap instead of join
    val daysDelta = ChronoUnit.DAYS.between(tripDataMinDate, tripDataMaxDate)
    val tripsPerBorough = tripRdd
      .flatMap(t => broadcastZones.value.get(t._4).map(borough => (borough, 1)))
      .reduceByKey(_ + _)
      .mapValues(_ / daysDelta)
    writeResults(
      tripsPerBorough.toDF("borough", "trips"),
      "1_trips_per_borough"
    )

    // Job 2 + 4: Average trips number by hour
    // Optimized merging the 2 jobs in only one reduce call since
    // both are reduced by the same key
    val tripsPerTimeSlot = tripRdd
      .map(t =>
        (
          t._1.getHour,
          (
            1,
            t._3,
            ChronoUnit.SECONDS
              .between(t._1, t._2)
              .doubleValue / 3600.doubleValue
          )
        )
      )
      .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2, v1._3 + v2._3))
      .map(v => (v._1, v._2._1 / daysDelta, v._2._2 / v._2._3))
    writeResults(
      tripsPerTimeSlot.toDF("hour", "trips", "avg_speed"),
      "2-4_trips_and_speed_per_hour"
    )

    // Job 3: Average trips by month
    val yearsDelta = ChronoUnit.YEARS.between(tripDataMinDate, tripDataMaxDate)
    val tripsPerMonth = tripRdd
      .map(t => (t._1.getMonth.toString, 1))
      .reduceByKey(_ + _)
      .mapValues(_ / yearsDelta)
    writeResults(tripsPerMonth.toDF("month", "trips"), "3_trips_per_month")

    // Job 5: Average tip by destination borough
    // Optimized using broadcast ad flatmap instead of join
    val tipsByDestination = tripRdd
      .flatMap(t =>
        broadcastZones.value.get(t._5).map(borough => (borough, (t._7, 1)))
      )
      .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
      .mapValues(v => v._1 / v._2.toDouble)
    writeResults(
      tipsByDestination.toDF("destination_borough", "avg_tips"),
      "5_average_tips_by_borough"
    )

    // tripRdd.unpersist()
    broadcastZones.destroy()
  }

  def run(config: Config): Unit = {
    initializeSparkContext(config)
    val dataset = loadData(config)
    config.runOptimizedJob match {
      case true       => runOptimizedJob(dataset._1, dataset._2)
      case _: Boolean => runStandardJob(dataset._1, dataset._2)
    }
  }
}

object LocalJob extends Job {

  private var outputDir: String = ""

  override def initializeSparkContext(config: Config): Unit = {
    outputDir = config.outputDir
  }

  override def writeResults(results: DataFrame, jobName: String): Unit = {
    sparkSession.sparkContext.setJobDescription(jobName)

    val finalPath =
      if (outputDir.nonEmpty) s"$outputDir/$jobName.csv" else s"$jobName.csv"

    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    val tempPath = new Path(finalPath + "_temp")
    val destPath = new Path(finalPath)

    results
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(tempPath.toString)

    val file = fs
      .listStatus(tempPath)
      .find(_.getPath.getName.endsWith(".csv"))
      .get
      .getPath
    if (fs.exists(destPath)) fs.delete(destPath, true)
    fs.rename(file, destPath)
    fs.delete(tempPath, true)
  }

}

object RemoteJob extends Job {

  private var bucketName: String = ""
  private var outputDir: String = ""

  override def writeResults(results: DataFrame, jobName: String): Unit = {
    sparkSession.sparkContext.setJobDescription(jobName)

    val finalPath = s"s3a://$bucketName/$outputDir/$jobName.csv"

    val fs = FileSystem.get(
      new java.net.URI(finalPath),
      sparkSession.sparkContext.hadoopConfiguration
    )
    val tempPath = new Path(finalPath + "_temp")
    val destPath = new Path(finalPath)

    results
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(tempPath.toString)

    val file = fs
      .listStatus(tempPath)
      .find(_.getPath.getName.endsWith(".csv"))
      .get
      .getPath
    if (fs.exists(destPath)) fs.delete(destPath, true)
    fs.rename(file, destPath)
    fs.delete(tempPath, true)
  }

  override def initializeSparkContext(config: Config): Unit = {
    bucketName = config.s3Bucket
    outputDir = config.s3OutputDir

    sparkSession.sparkContext.hadoopConfiguration
      .set("fs.s3a.fast.upload", "true")
    sparkSession.sparkContext.hadoopConfiguration
      .set("fs.s3a.fast.upload.buffer", "bytebuffer")

    val accessKey = config.awsAccessKey
    val secretKey = config.awsSecretKey

    sparkSession.sparkContext.hadoopConfiguration
      .set("fs.s3n.awsAccessKeyId", accessKey)
    sparkSession.sparkContext.hadoopConfiguration
      .set("fs.s3n.awsSecretAccessKey", secretKey)

    if (config.awsRegion.nonEmpty) {
      sparkSession.sparkContext.hadoopConfiguration
        .set("fs.s3a.endpoint", s"s3.${config.awsRegion}.amazonaws.com")
    }
  }
}
