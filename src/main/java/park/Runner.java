package park;

import park.avro.Organization;
import org.apache.avro.Schema;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.LocalOutputFile;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class Runner {
  public static void main(String[] args) throws IOException {
    saveParquet(List.of(
        new Org("A", "A1", "USA", Type.FOO, List.of()),
        new Org("B", "B1", "BSA", Type.FOO, List.of()),
        new Org("C", "C1", "CSA", Type.FOO, List.of()),
        new Org("D", "D1", "DSA", Type.FOO, List.of()),
        new Org("E", "E1", "ESA", Type.FOO, List.of()),
        new Org("F", "F1", "FSA", Type.FOO, List.of())
    ));
  }

  public static void saveParquet(List<Org> os) throws IOException {
    final var schema = new Organization().getSchema();

    /*try (final var writer = AvroParquetWriter
        .builder(new LocalOutputFile(Path.of("out.parquet")))
        .withSchema(schema)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .config(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "false")
        .build()) {*/
    try (final var writer = avroParquetWriter(schema, "out.parquet")) {
      Org
          .toAvro(os)
          .forEach(org -> {
            try {
              writer.write(org);
            } catch (IOException e) {
              throw new RuntimeException("Failed to write org: " + org.getName(), e);
            }
          });
    }
  }

  public static <T> ParquetWriter<T> avroParquetWriter(Schema schema, String outFilePath) throws IOException {
    final var out = new LocalOutputFile(Path.of(outFilePath));
    return AvroParquetWriter
        .<T>builder(out)
        .withSchema(schema)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .config(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "false")
        .build();
  }
}
