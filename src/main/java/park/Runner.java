package park;

import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.LocalInputFile;
import park.avro.Organization;
import org.apache.avro.Schema;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.LocalOutputFile;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.apache.parquet.avro.AvroReadSupport.AVRO_REQUESTED_PROJECTION;

public class Runner {
  private static final byte BYTE_5 = 5;
  private static final byte BYTE_10 = 10;
  private static final short SHORT_25 = 25;

  public static void main(String[] args) throws IOException {
    final var parquetFilePath = "build/out.parquet";
    final var organizations = getOrgs();

    saveParquet(organizations, parquetFilePath);

    System.err.println("Parquet file " + parquetFilePath + " saved successfully");

    readOrganizations(parquetFilePath).forEach(System.out::println);
    parseOrgs(parquetFilePath).forEach(System.out::println);
  }

  private static List<Org> getOrgs() {
    final var attr = new Attr(
      "123",
      BYTE_5,
      BYTE_10,
      true,
      12.34,
      SHORT_25
    );

    return List.of(
      new Org("A", "A1", "USA", Type.FOO, List.of(attr)),
      new Org("B", "B1", "BSA", Type.FOO, List.of(attr)),
      new Org("C", "C1", "CSA", Type.FOO, List.of(attr)),
      new Org("D", "D1", "DSA", Type.FOO, List.of(attr)),
      new Org("E", "E1", "ESA", Type.FOO, List.of(attr)),
      new Org("F", "F1", "FSA", Type.FOO, List.of(attr))
    );
  }

  public static void saveParquet(List<Org> os, String outFilePath) throws IOException {
    final var schema = new Organization().getSchema();

    try (final var writer = avroParquetWriter(schema, outFilePath)) {
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

  // region Many different ways of reading the parquet file

  public static List<Organization> readOrganizations(String parquetFilePath) throws IOException {
    final var in = new LocalInputFile(Path.of(parquetFilePath));
    try (
      final var reader = AvroParquetReader
        .<Organization>builder(in)
        .withConf(new PlainParquetConfiguration())
        .build()
    ) {
      final var organizations = new ArrayList<Organization>();
      Organization next;
      while ((next = reader.read()) != null) {
        organizations.add(next);
      }
      return organizations;
    }
  }

  @SuppressWarnings("unchecked")
  public static List<Org> readOrgList(String parquetFilePath) throws IOException {
    try (
      final ParquetReader<GenericRecord> reader =
        AvroParquetReader
          .<GenericRecord>builder(new LocalInputFile(Path.of(parquetFilePath)))
          .withConf(new PlainParquetConfiguration())
          .build()
    ) {
      final List<Org> organizations = new ArrayList<>();
      GenericRecord record;
      while ((record = reader.read()) != null) {
        final var attrsRecords = (List<GenericRecord>) record.get("attributes");

        final var attrs =
          attrsRecords
            .stream()
            .map(attr -> new Attr(
              attr.get("id").toString(),
              ((Integer) attr.get("quantity")).byteValue(),
              ((Integer) attr.get("amount")).byteValue(),
              (boolean) attr.get("active"),
              (double) attr.get("percent"),
              ((Integer) attr.get("size")).shortValue())
            )
            .toList();

        final Utf8 name = (Utf8) record.get("name");
        final Utf8 category = (Utf8) record.get("category");
        final Utf8 country = (Utf8) record.get("country");
        final Type type = Type.valueOf(record.get("organizationType").toString());

        organizations.add(
          new Org(
            name.toString()
            , category.toString()
            , country.toString()
            , type
            , attrs
          )
        );
      }

      return organizations;
    }
  }

  public static <T> List<T> readObjects(String parquetFilePath) throws IOException {
    final var in = new LocalInputFile(Path.of(parquetFilePath));
    try (final var reader = AvroParquetReader.<T>builder(in).build()) {
      final var objects = new ArrayList<T>();
      T next;
      while ((next = reader.read()) != null) {
        objects.add(next);
      }
      return objects;
    }
  }

  public static List<Org> parseOrgs(String parquetFilePath) throws IOException {
    final var orgsSchema =
      SchemaBuilder
        .record("Organizations")
        .fields()
        .requiredString("name")
        .requiredString("category")
        .requiredString("country")
        .requiredString("organizationType")
        .endRecord();

    final var configuration = new Configuration();
    configuration.set(AVRO_REQUESTED_PROJECTION, orgsSchema.toString());

    final var inputFile = new LocalInputFile(Path.of(parquetFilePath));

    try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile)
      .withConf(configuration)
      .build()) {
      final List<Org> organizations = new ArrayList<>();
      GenericRecord record;

      while ((record = reader.read()) != null) {
        final var name = (Utf8) record.get("name");
        final var category = (Utf8) record.get("category");
        final var country = (Utf8) record.get("country");
        final var type = Type.valueOf(record.get("organizationType").toString());

        organizations.add(
          new Org(
            name.toString(),
            category.toString(),
            country.toString(),
            type,
            null
          )
        );
      }

      return organizations;
    }
  }

  // endregion
}
