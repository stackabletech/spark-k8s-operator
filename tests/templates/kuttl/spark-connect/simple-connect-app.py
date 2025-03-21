from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("SampleConnectApp")
        .remote("sc://localhost")
        .getOrCreate()
    )

    logFile = "/stackable/spark/README.md"
    logData = spark.read.text(logFile).cache()

    numAs = logData.filter(logData.value.contains("a")).count()
    numBs = logData.filter(logData.value.contains("b")).count()

    print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

    spark.stop()
