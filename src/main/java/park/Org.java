package park;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import park.avro.Attribute;
import park.avro.Organization;
import park.avro.OrganizationType;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public record Org(
    String name
    , String category
    , String country
    , Type type
    , List<Attr> attributes
) {
  public Organization toAvro() {
    return Organization.newBuilder()
        .setName(name())
        .setCategory(category())
        .setCountry(country())
        .setOrganizationType(OrganizationType.valueOf(type().name()))
        .setAttributes(Attr.toAvro(attributes()))
        .build();
  }

  public static List<Organization> toAvro(List<Org> organizations) {
    return organizations
        .stream()
        .map(Org::toAvro)
        .toList();
  }

  public static void serialize(List<Org> organizations, OutputStream os) throws IOException {
    final var datumWriter = new SpecificDatumWriter<>(Organization.class);

    try (final var dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.create(new Organization().getSchema(), os);

      for (var org : organizations) {
        final var attrs =
            org
                .attributes()
                .stream()
                .map(a -> Attribute.newBuilder()
                    .setId(a.id())
                    .setQuantity(a.quantity())
                    .setAmount(a.amount())
                    .setSize(a.size())
                    .setPercent(a.percent())
                    .setActive(a.active())
                    .build())
                .toList();

        final var organization = Organization.newBuilder()
            .setName(org.name())
            .setCategory(org.category())
            .setCountry(org.country())
            .setOrganizationType(OrganizationType.valueOf(org.type().name()))
            .setAttributes(attrs)
            .build();

        dataFileWriter.append(organization);
      }
    }
  }

  public static List<Organization> deserialize(String avroFilePath) throws IOException {
    final var file = new File(avroFilePath);
    final var reader = new SpecificDatumReader<>(Organization.class);
    final var organizations = new ArrayList<Organization>();

    try (final var dataFileReader = new DataFileReader<>(file, reader)) {
      while (dataFileReader.hasNext()) {
        organizations.add(dataFileReader.next());
      }
    }

    return organizations;
  }
}

