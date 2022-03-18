
## Build job image

    docker build -t docker.stackable.tech/stackable/ny-tlc-report:0.1.0 -t docker.stackable.tech/stackable/ny-tlc-report:latest -f apps/docker/Dockerfile apps/

## Generate report from the public data set

    spark-submit --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider --packages org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-s3:1.12.180,com.amazonaws:aws-java-sdk-core:1.12.180 ny_tlc_report.py --input 's3a://nyc-tlc/trip data/yellow_tripdata_2021-07.csv'

Notes:

* The `AnonymousAWSCredentialsProvider` is required because the `nyc-tlc` is a public S3 bucket.
* The `--packages` list contains the job dependencies for the AWS S3 connection.
* Only one file is used for reporting in this example `yellow_tripdata_2021-07.csv`
* This example works with `spark-3.1.3-bin-hadoop3.2` and Python 3.9.7. Other versions may require different dependency versions or even complete different dependencies altogether.

### Links

[0] TLC trip data set https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
[1] AWS Java SDK https://github.com/aws/aws-sdk-java
[2] Hadoop AWS module https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html
