from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("SampleConnectApp")
        .remote("sc://localhost")
        .getOrCreate()
    )

    # See https://issues.apache.org/jira/browse/SPARK-46032
    spark.addArtifacts("/stackable/spark/connect/spark-connect_2.12-3.5.5.jar")

    logFile = "/stackable/spark/README.md"
    logData = spark.read.text(logFile).cache()

    numAs = logData.filter(logData.value.contains("a")).count()
    numBs = logData.filter(logData.value.contains("b")).count()

    print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

    spark.stop()
