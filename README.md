# NYC Taxi Spark Analysis - University Project Delivery

This repository contains the Big Data university project for analyzing the New York City Yellow Taxi dataset using Apache Spark (Scala & RDDs). 

Below is the directory structure mapping of the project artifacts required for the delivery, followed by the execution guidelines and the command-line arguments supported by the Spark application.

## 📦 Project Delivery Structure

When delivered as a ZIP file, the project is structured as follows:

| Delivery Requirement | File / Directory Location in the ZIP | Description |
| :--- | :--- | :--- |
| **The project application** | [src/main/scala/](./src/main/scala/) | Spark application Scala source code containing: <ul><li>[Main.scala](./src/main/scala/project/Main.scala): CLI entry point & parsing</li><li>[Job.scala](./src/main/scala/project/Job.scala): Standard/Optimized job implementations</li><li>[DataProvider.scala](./src/main/scala/project/DataProvider.scala): Spark data loading</li></ul> |
| **The executed notebooks** | [src/main/python/](./src/main/python/) | Jupyter notebooks containing all cells executed:<ul><li>[jobs.ipynb](./src/main/python/jobs.ipynb): Interactive Spark job development</li><li>[results.ipynb](./src/main/python/results.ipynb): Plots, verification, & speedup comparison</li></ul> |
| **The dataset sample** | [data/sample.parquet](./data/sample.parquet) | Parquet sample of the dataset used for local execution. |
| **Zone lookup reference** | [data/taxi_zone_lookup.csv](./data/taxi_zone_lookup.csv) | CSV mapping Zone IDs to Borough names. |
| **The history of executed jobs** | [data/results/histories/](./data/results/histories/) | Gzipped Spark UI event log histories:<ul><li>[standard.gz](./data/results/histories/standard.gz): Event log for standard execution</li><li>[optimized.gz](./data/results/histories/optimized.gz): Event log for optimized execution</li></ul> |
| **Additional Material (Outputs)** | [data/results/](./data/results/) | Output CSV files containing the computed metrics:<ul><li>`out_standard/`: Standard jobs output</li><li>`out_optimized/`: Optimized jobs output</li></ul> |
| **Application Jar** | [BigDataProject.jar](./BigDataProject.jar) | Executable Fat JAR containing the Spark application and all dependencies. |

---

## 💾 Full Dataset Download Link

The full dataset utilized for this project is the official **New York City Taxi & Limousine Commission (TLC) Trip Record Data** spanning from `2009-01-01` to `2020-07-01`.

*   **Official TLC Website Page**: [NYC TLC Trip Record Data Page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
*   **Direct Download S3 Bucket URI**: `s3://nyc-tlc/` (Hosted in the `us-east-1` region)
    *   *Note*: The full dataset is publicly available on AWS and can be accessed/listed using:
        ```bash
        aws s3 ls --no-sign-request s3://nyc-tlc/
        ```

---

## 🚀 Building the Application

To build the executable Fat JAR, run the following Gradle task from the project root:

```bash
./gradlew shadowJar
```

This will produce the executable JAR file at:
`build/libs/BigDataProject.jar`

---

## 💻 Command Line Interface (CLI) Arguments

The Spark application executable supports two deployment modes: `local` and `aws`.

### General Usage:
```bash
spark-submit --class project.Main build/libs/BigDataProject.jar <command> [options]
```

### Positional Argument / Commands:
*   `local`: Executes Spark jobs locally on a single machine.
*   `aws`: Executes Spark jobs on AWS EMR cluster or reading/writing directly to/from S3.
*   `--help`: Displays the CLI help menu.

---

### Options for `local` command:

| Option | Abbreviation | Type | Description |
| :--- | :--- | :--- | :--- |
| `--sample-data` | `-s` | String | **Required.** Path to the Parquet dataset sample (e.g., `data/sample.parquet`). |
| `--zones-lookup-file` | `-z` | String | **Required.** Path to the taxi zones CSV file (e.g., `data/taxi_zone_lookup.csv`). |
| `--output-dir` | `-o` | String | **Required.** Local path directory to save results CSVs (e.g., `out` or `data/results/out_standard`). |
| `--optimized` | - | Flag / Unit | **Optional.** If present, runs the optimized implementation of the Spark jobs. |

*Example execution:*
```bash
spark-submit --class project.Main --master local[*] build/libs/BigDataProject.jar local \
  -s data/sample.parquet \
  -z data/taxi_zone_lookup.csv \
  -o out \
  --optimized
```

---

### Options for `aws` command:

| Option | Abbreviation | Type | Description |
| :--- | :--- | :--- | :--- |
| `--bucket` | `-b` | String | **Required.** The target AWS S3 bucket name. |
| `--region` | `-r` | String | **Optional.** The AWS region (defaults to `us-east-1`). |
| `--output-dir` | `-o` | String | **Optional.** S3 folder/prefix path where results are written (defaults to `out`). |
| `--access-key` | - | String | **Optional.** AWS Access Key ID for S3 storage configuration. |
| `--secret-key` | - | String | **Optional.** AWS Secret Access Key for S3 storage configuration. |
| `--optimized` | - | Flag / Unit | **Optional.** If present, runs the optimized implementation of the Spark jobs. |

*Example execution:*
```bash
spark-submit --class project.Main build/libs/BigDataProject.jar aws \
  -b my-spark-bucket \
  -o results \
  --access-key YOUR_ACCESS_KEY \
  --secret-key YOUR_SECRET_KEY \
  --optimized
```
