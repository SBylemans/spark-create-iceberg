package be.bylemans;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;

public class Main {

  public static void main(String[] args) throws Exception {
    SparkSession spark = SparkSession.builder()
        .appName("YAML Schema Reader")
        .getOrCreate();
    try (Git g = Git.cloneRepository()
        .setURI(args[1])
        .setDirectory(new File("/tmp/data"))
        .call()) {
      Schema schema = Schema.fromYaml("/tmp/data/%s".formatted(args[2]));
      StructType sparkSchema = Objects.requireNonNull(schema).toSparkSchema();
      Dataset<Row> dataFrame = spark.createDataFrame(spark.emptyDataFrame().javaRDD(), sparkSchema);
      dataFrame.writeTo(schema.name).tableProperty("format-version", "2").createOrReplace();
    } finally {
      spark.stop();
    }
  }
}
