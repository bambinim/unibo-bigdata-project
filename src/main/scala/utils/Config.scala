package utils

import org.apache.spark.sql.DataFrame

case class Config(
    deploymentMode: String = "",

    //common parameters
    runOptimizedJob: Boolean = false,

    // local deployment parameters
    sampleData: String = "",
    zonesLookupFile: String = "",
    outputDir: String = "",

    // aws deployment parameters
    s3Bucket: String = "",
    awsRegion: String = "",
    s3OutputDir: String = "",
    awsAccessKey: String = "",
    awsSecretKey: String = ""
)

case class JobResult(
    name: String,
    result: DataFrame
)
