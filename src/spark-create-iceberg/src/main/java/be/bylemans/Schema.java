package be.bylemans;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.yaml.snakeyaml.Yaml;

public class Schema {

  public String name;
  public List<Field> fields;

  public static class Field {
    public String name;
    public String type;
    public boolean nullable;
    public String metadata;
  }

  public static Schema fromYaml(String yamlPath) {
    Yaml yaml = new Yaml();
    try (InputStream in = new FileInputStream(yamlPath)) {
      return yaml.loadAs(in, Schema.class);
    } catch (IOException e) {
      return null;
    }
  }

  public StructType toSparkSchema() {
    return new StructType(this.fields.stream()
        .map(f -> new StructField(f.name, toDataType(f.type), f.nullable,
            Metadata.fromJson(f.metadata)))
        .toArray(StructField[]::new));
  }

  private static DataType toDataType(String type) {
    return switch (type.toLowerCase()) {
      case "string" -> DataTypes.StringType;
      case "integer", "int" -> DataTypes.IntegerType;
      case "long" -> DataTypes.LongType;
      case "double" -> DataTypes.DoubleType;
      case "float" -> DataTypes.FloatType;
      case "boolean", "bool" -> DataTypes.BooleanType;
      case "binary" -> DataTypes.BinaryType;
      default -> throw new IllegalArgumentException("Unsupported type: " + type);
    };
  }
}
