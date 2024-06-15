package park;

import park.avro.Attribute;

import java.util.List;

public record Attr(
    String id,
    byte quantity,
    byte amount,
    boolean active,
    double percent,
    short size
) {
  public Attribute toAvro() {
    return Attribute.newBuilder()
        .setId(id())
        .setQuantity(quantity())
        .setAmount(amount())
        .setSize(size())
        .setPercent(percent())
        .setActive(active())
        .build();
  }

  public static List<Attribute> toAvro(List<Attr> attributes) {
    return attributes
        .stream()
        .map(Attr::toAvro)
        .toList();
  }
}
