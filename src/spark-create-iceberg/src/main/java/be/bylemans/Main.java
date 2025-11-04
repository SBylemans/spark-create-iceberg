package be.bylemans;

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.EntryStatus.dir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {


  public static void main(String[] args) throws Exception {
    Logger logger = LoggerFactory.getLogger(Main.class);

    logger.info("Getting schema from {} - {}", args[0], args[1]);

    SparkSession spark = SparkSession.builder()
        .appName("YAML Schema Reader")
        .getOrCreate();
    try (Git g = Git.cloneRepository()
        .setURI(args[0])
        .setDirectory(new File("/tmp/data"))
        .call()) {

      try (Stream<Path> stream = Files.walk(Path.of("/tmp/data"))) {
        stream.filter(Files::isRegularFile)
            .forEach(f -> logger.info("Found file: {}", f));
      } catch (IOException e) {
        e.printStackTrace();
      }

      Schema schema = Schema.fromYaml("/tmp/data/%s".formatted(args[1]));
      StructType sparkSchema = Objects.requireNonNull(schema).toSparkSchema();
      Dataset<Row> dataFrame = spark.createDataFrame(spark.emptyDataFrame().javaRDD(), sparkSchema);
      dataFrame.writeTo(schema.name).tableProperty("format-version", "2").createOrReplace();
    } finally {
      spark.stop();
    }
  }
}
