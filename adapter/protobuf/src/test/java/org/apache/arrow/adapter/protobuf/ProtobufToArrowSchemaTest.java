/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.adapter.protobuf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.Descriptors.Descriptor;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.adapter.protobuf.TestEditionsProtos.EditionsMessage;
import org.apache.arrow.adapter.protobuf.TestProto2Protos.Proto2Message;
import org.apache.arrow.adapter.protobuf.TestProtos.Containers;
import org.apache.arrow.adapter.protobuf.TestProtos.Enums;
import org.apache.arrow.adapter.protobuf.TestProtos.Envelope;
import org.apache.arrow.adapter.protobuf.TestProtos.Node;
import org.apache.arrow.adapter.protobuf.TestProtos.Presence;
import org.apache.arrow.adapter.protobuf.TestProtos.Scalars;
import org.apache.arrow.adapter.protobuf.TestProtos.WellKnownTypes;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

public class ProtobufToArrowSchemaTest {

  private static final ArrowType.Int INT32 = new ArrowType.Int(32, true);
  private static final ArrowType.Int INT64 = new ArrowType.Int(64, true);
  private static final ArrowType.Int UINT32 = new ArrowType.Int(32, false);
  private static final ArrowType.Int UINT64 = new ArrowType.Int(64, false);
  private static final ArrowType.FloatingPoint DOUBLE =
      new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);

  private static Schema convert(Descriptor descriptor, ProtobufToArrowConfigBuilder builder) {
    return ProtobufToArrow.protobufToArrowSchema(descriptor, builder.build());
  }

  private static Schema convert(Descriptor descriptor) {
    return convert(descriptor, new ProtobufToArrowConfigBuilder());
  }

  private static Field field(String name, boolean nullable, ArrowType type, Field... children) {
    return new Field(name, new FieldType(nullable, type, null), Arrays.asList(children));
  }

  private static Field dictionaryField(String name, boolean nullable, long dictionaryId) {
    DictionaryEncoding encoding = new DictionaryEncoding(dictionaryId, false, INT32);
    return new Field(name, new FieldType(nullable, INT32, encoding), null);
  }

  private static Field list(String name, Field element) {
    return field(name, false, ArrowType.List.INSTANCE, element);
  }

  private static Field map(String name, Field key, Field value) {
    return field(
        name,
        false,
        new ArrowType.Map(false),
        field("entries", false, ArrowType.Struct.INSTANCE, key, value));
  }

  private static Field item(String name, boolean nullable) {
    return field(
        name,
        nullable,
        ArrowType.Struct.INSTANCE,
        field("name", false, ArrowType.Utf8.INSTANCE),
        field("quantity", false, INT64));
  }

  @Test
  public void testScalarTypes() {
    List<Field> expected =
        Arrays.asList(
            field("bool_field", false, ArrowType.Bool.INSTANCE),
            field("int32_field", false, INT32),
            field("sint32_field", false, INT32),
            field("sfixed32_field", false, INT32),
            field("uint32_field", false, UINT32),
            field("fixed32_field", false, UINT32),
            field("int64_field", false, INT64),
            field("sint64_field", false, INT64),
            field("sfixed64_field", false, INT64),
            field("uint64_field", false, UINT64),
            field("fixed64_field", false, UINT64),
            field("float_field", false, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
            field("double_field", false, DOUBLE),
            field("string_field", false, ArrowType.Utf8.INSTANCE),
            field("bytes_field", false, ArrowType.Binary.INSTANCE));

    assertEquals(expected, convert(Scalars.getDescriptor()).getFields());
  }

  @Test
  public void testUnsignedAsSigned() {
    Schema schema =
        convert(
            Scalars.getDescriptor(), new ProtobufToArrowConfigBuilder().setUnsignedAsSigned(true));

    assertEquals(INT64, schema.findField("uint32_field").getType());
    assertEquals(INT64, schema.findField("fixed32_field").getType());
    assertEquals(INT64, schema.findField("uint64_field").getType());
    assertEquals(INT64, schema.findField("fixed64_field").getType());
    assertEquals(INT32, schema.findField("int32_field").getType());
  }

  @Test
  public void testFieldsWithPresenceAreNullable() {
    List<Field> expected =
        Arrays.asList(
            field("implicit_field", false, INT32),
            field("optional_field", true, INT32),
            item("message_field", true),
            field("text", true, ArrowType.Utf8.INSTANCE),
            item("item", true));
    assertEquals(expected, convert(Presence.getDescriptor()).getFields());

    // Every singular proto2 field tracks presence, even with a default value
    List<Field> expectedProto2 =
        Arrays.asList(
            field("required_field", true, ArrowType.Utf8.INSTANCE),
            field("optional_field", true, INT32),
            list("repeated_field", field("$data$", false, INT32)),
            field(
                "point",
                true,
                ArrowType.Struct.INSTANCE,
                field("x", true, INT32),
                list("y", field("$data$", false, INT32))),
            dictionaryField("level", true, 0),
            list("levels", dictionaryField("$data$", false, 0)));
    assertEquals(expectedProto2, convert(Proto2Message.getDescriptor()).getFields());

    List<Field> expectedEditions =
        Arrays.asList(
            field("explicit_field", true, INT32),
            field("implicit_field", false, INT32),
            field("required_field", true, INT32),
            field("delimited", true, ArrowType.Struct.INSTANCE, field("value", true, INT32)),
            list("expanded", field("$data$", false, INT32)));
    assertEquals(expectedEditions, convert(EditionsMessage.getDescriptor()).getFields());
  }

  @Test
  public void testRepeatedAndMapFields() {
    List<Field> expected =
        Arrays.asList(
            list("ints", field("$data$", false, INT32)),
            list("items", item("$data$", false)),
            map(
                "counts",
                field("key", false, ArrowType.Utf8.INSTANCE),
                field("value", false, INT64)),
            map("items_by_id", field("key", false, INT32), item("value", false)),
            map(
                "flags",
                field("key", false, ArrowType.Bool.INSTANCE),
                field("value", false, DOUBLE)),
            map(
                "blobs",
                field("key", false, UINT64),
                field("value", false, ArrowType.Binary.INSTANCE)));

    assertEquals(expected, convert(Containers.getDescriptor()).getFields());
  }

  @Test
  public void testEnumMappings() {
    // Fields of the same enum type share a dictionary
    List<Field> expected =
        Arrays.asList(
            dictionaryField("color", false, 0),
            dictionaryField("shape", false, 1),
            list("colors", dictionaryField("$data$", false, 0)),
            map(
                "colors_by_name",
                field("key", false, ArrowType.Utf8.INSTANCE),
                dictionaryField("value", false, 0)),
            dictionaryField("optional_color", true, 0));
    assertEquals(expected, convert(Enums.getDescriptor()).getFields());

    Schema nameSchema =
        convert(
            Enums.getDescriptor(),
            new ProtobufToArrowConfigBuilder()
                .setEnumMapping(ProtobufToArrowConfig.EnumMapping.NAME));
    assertEquals(field("color", false, ArrowType.Utf8.INSTANCE), nameSchema.findField("color"));

    Schema numberSchema =
        convert(
            Enums.getDescriptor(),
            new ProtobufToArrowConfigBuilder()
                .setEnumMapping(ProtobufToArrowConfig.EnumMapping.NUMBER));
    assertEquals(field("color", false, INT32), numberSchema.findField("color"));
  }

  @Test
  public void testUnknownEnumValuesAsNull() {
    ProtobufToArrowConfigBuilder builder =
        new ProtobufToArrowConfigBuilder()
            .setUnknownEnumValues(ProtobufToArrowConfig.UnknownEnumValues.NULL);

    // Values of open enums can be null, even in lists and maps
    List<Field> expected =
        Arrays.asList(
            dictionaryField("color", true, 0),
            dictionaryField("shape", true, 1),
            list("colors", dictionaryField("$data$", true, 0)),
            map(
                "colors_by_name",
                field("key", false, ArrowType.Utf8.INSTANCE),
                dictionaryField("value", true, 0)),
            dictionaryField("optional_color", true, 0));
    assertEquals(expected, convert(Enums.getDescriptor(), builder).getFields());

    // Closed enums and numbers do not have unknown values
    assertEquals(
        list("levels", dictionaryField("$data$", false, 0)),
        convert(Proto2Message.getDescriptor(), builder).findField("levels"));
    assertEquals(
        field("color", false, INT32),
        convert(
                Enums.getDescriptor(),
                builder.setEnumMapping(ProtobufToArrowConfig.EnumMapping.NUMBER))
            .findField("color"));
  }

  @Test
  public void testWellKnownTypes() {
    ArrowType timestamp = new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
    List<Field> expected =
        Arrays.asList(
            field("timestamp", true, timestamp),
            field("struct", true, ArrowType.Utf8.INSTANCE),
            field("value", true, ArrowType.Utf8.INSTANCE),
            field("list_value", true, ArrowType.Utf8.INSTANCE),
            list("timestamps", field("$data$", false, timestamp)),
            map(
                "values",
                field("key", false, ArrowType.Utf8.INSTANCE),
                field("value", false, ArrowType.Utf8.INSTANCE)));
    assertEquals(expected, convert(WellKnownTypes.getDescriptor()).getFields());

    Schema nanoSchema =
        convert(
            WellKnownTypes.getDescriptor(),
            new ProtobufToArrowConfigBuilder().setTimestampUnit(TimeUnit.NANOSECOND));
    assertEquals(
        new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC"),
        nanoSchema.findField("timestamp").getType());
  }

  @Test
  public void testRecursiveMessages() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> convert(Envelope.getDescriptor()));
    assertEquals(
        "Recursive message types are not supported: arrow.adapter.protobuf.Envelope"
            + " -> arrow.adapter.protobuf.Ping -> arrow.adapter.protobuf.Pong"
            + " -> arrow.adapter.protobuf.Ping",
        e.getMessage());

    // Map entries are not part of the cycle
    e = assertThrows(IllegalArgumentException.class, () -> convert(Node.getDescriptor()));
    assertEquals(
        "Recursive message types are not supported: arrow.adapter.protobuf.Node"
            + " -> arrow.adapter.protobuf.Node",
        e.getMessage());
  }
}
