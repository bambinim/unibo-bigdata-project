package project

import scopt.OParser
import utils.Config
import java.nio.file.Paths

object Main {

  def ensureAbsolutePath(path: String): String = {
    Paths.get(path).toAbsolutePath().toString
  }

  def parseArgs(args: Array[String]): Option[Config] = {
    val builder = OParser.builder[Config]
    val parser = {
      import builder._
      OParser.sequence(
        cmd("local")
          .action((_, c) => c.copy(deploymentMode = "local"))
          .children(
            opt[String]("sample-data")
              .abbr("s")
              .required()
              .action((v, c) => c.copy(sampleData = ensureAbsolutePath(v))),
            opt[String]("zones-lookup-file")
              .abbr("z")
              .required()
              .action((v, c) => c.copy(zonesLookupFile = ensureAbsolutePath(v))),
            opt[String]("output-dir")
              .abbr("o")
              .required()
              .action((v, c) => c.copy(outputDir = ensureAbsolutePath(v))),
            opt[Unit]("optimized")
              .action((_, c) => c.copy(runOptimizedJob = true))
          ),
        cmd("aws")
          .action((_, c) => c.copy(deploymentMode = "aws"))
          .children(
            opt[String]("bucket")
              .abbr("b")
              .required()
              .action((v, c) => c.copy(s3Bucket = v)),
            opt[String]("region")
              .abbr("r")
              .withFallback(() => "us-east-1")
              .action((v, c) => c.copy(awsRegion = v)),
            opt[String]("output-dir")
              .abbr("o")
              .withFallback(() => "out")
              .action((v, c) => c.copy(s3OutputDir = v)),
            opt[String]("access-key")
              .action((v, c) => c.copy(awsAccessKey = v)),
            opt[String]("secret-key")
              .action((v, c) => c.copy(awsSecretKey = v)),
            opt[Unit]("optimized")
              .action((_, c) => c.copy(runOptimizedJob = true))
          ),
        help("help")
      )
    }
    OParser.parse(parser, args, Config())
  }

  def getJobInstance(config: Config): Job = config.deploymentMode match {
    case "local" => LocalJob
    case "aws" => RemoteJob
    case _ => throw new RuntimeException(s"Invalid deployment mode ${config.deploymentMode}")
  }

  def runJob(config: Config): Unit = {
    val job = getJobInstance(config)
    job.run(config)
  }

  def main(args: Array[String]): Unit = {
    parseArgs(args) match {
      case Some(config) => runJob(config)
      case None         =>
    }
  }
}
