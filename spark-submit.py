# with Workflow(
#         generate_name="csv2parquet-iceberg-",
#         entrypoint="steps",
#         namespace="argo",
#         pod_gc=PodGC(strategy="OnWorkflowCompletion"),
# ) as w:
#     Container(name="container-template", image="ghcr.io/sbylemans/hera-deploy-spark-sedona:v0.0.44", command=[
#         "/bin/sh",
#         "-c",
#         "/opt/spark/bin/spark-submit \
#         --master k8s://https://kubernetes.default.svc:443 \
#         --deploy-mode cluster \
#         --name sparkapp \
#         --conf spark.kubernetes.namespace=argo \
#         --conf spark.kubernetes.container.image=ghcr.io/sbylemans/hera-deploy-spark-sedona:v0.0.44 \
#         --conf spark.executor.memory=2G \
#         --conf spark.executor.cores=2 \
#         --conf spark.hadoop.fs.s3.connection.timeout=6000 \
#         --conf spark.hadoop.fs.s3a.endpoint=http://minio:11000 \
#         --conf spark.hadoop.fs.s3a.access.key=local-access \
#         --conf spark.hadoop.fs.s3a.secret.key=local-secret \
#         --conf spark.hadoop.fs.s3a.path.style.access=true \
#         --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
#         --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
#         --conf spark.executor.extraJavaOptions=\"-Dcom.amazonaws.services.s3.enableV4=true -Daws.region=us-east-1\" \
#         --conf spark.driver.extraJavaOptions=\"-Dcom.amazonaws.services.s3.enableV4=true -Daws.region=us-east-1\" \
#         --conf spark.hadoop.fs.s3a.connection.establish.timeout=30000 \
#         --conf spark.hadoop.fs.s3a.threads.keepalivetime=60000 \
#         --conf spark.hadoop.fs.s3a.multipart.purge.age=86400000 \
#         --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
#         --conf spark.sql.catalog.test=org.apache.iceberg.spark.SparkCatalog \
#         --conf spark.sql.catalog.test.uri=http://polaris:8181/api/catalog \
#         --conf spark.sql.catalog.test.type=rest \
#         --conf spark.sql.catalog.test.warehouse=test \
#         --conf spark.sql.catalog.test.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
#         --conf spark.sql.catalog.test.credential=root:s3cr3t \
#         --conf spark.sql.catalog.test.scope=PRINCIPAL_ROLE:ALL \
#         --conf spark.sql.catalog.test.header.X-Iceberg-Access-Delegation=vended-credentials \
#         --conf spark.sql.catalog.test.token-refresh-enabled=true \
#         --conf spark.jars.ivy=/tmp/.ivy \
#         --conf spark.jars.packages=org.apache.hadoop:hadoop-aws:3.3.4,software.amazon.awssdk:bundle:2.29.52,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,org.apache.sedona:sedona-spark-3.5_2.12:1.8.0,org.datasyslab:geotools-wrapper:1.8.0-33.1 \
#         --conf spark.jars.excludes=org.slf4j:slf4j-api,org.slf4j:slf4j-log4j12 \
#         --conf spark.executorEnv.AWS_REGION=us-east-1 \
#         --conf spark.executorEnv.AWS_ACCESS_KEY_ID=local-access \
#         --conf spark.executorEnv.AWS_SECRET_ACCESS_KEY=local-secret \
#         --conf spark.executorEnv.AWS_ENDPOINT_URL_S3=http://minio:11000 \
#         --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
#         --conf spark.kryo.registrator=org.apache.sedona.core.serde.SedonaKryoRegistrator \
#             local:///opt/spark-app/spark-sedona.py"
#
#     ])
#
#     with DAG(name="steps") as s:
#         t1 = Task(name="transform", template="container-template")
#
#         t1