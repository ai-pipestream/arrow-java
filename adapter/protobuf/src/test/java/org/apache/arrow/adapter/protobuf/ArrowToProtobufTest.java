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

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Empty;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ListValue;
import com.google.protobuf.Message;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.apache.arrow.adapter.protobuf.TestEditionsProtos.EditionsMessage;
import org.apache.arrow.adapter.protobuf.TestProto2Protos.Level;
import org.apache.arrow.adapter.protobuf.TestProto2Protos.Proto2Message;
import org.apache.arrow.adapter.protobuf.TestProtos.Color;
import org.apache.arrow.adapter.protobuf.TestProtos.Containers;
import org.apache.arrow.adapter.protobuf.TestProtos.Drawing;
import org.apache.arrow.adapter.protobuf.TestProtos.Enums;
import org.apache.arrow.adapter.protobuf.TestProtos.Inner;
import org.apache.arrow.adapter.protobuf.TestProtos.Item;
import org.apache.arrow.adapter.protobuf.TestProtos.Outer;
import org.apache.arrow.adapter.protobuf.TestProtos.Presence;
import org.apache.arrow.adapter.protobuf.TestProtos.Scalars;
import org.apache.arrow.adapter.protobuf.TestProtos.Shape;
import org.apache.arrow.adapter.protobuf.TestProtos.Size;
import org.apache.arrow.adapter.protobuf.TestProtos.WellKnownTypes;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ViewVarBinaryVector;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class ArrowToProtobufTest {

  private static final Scalars SCALARS =
      Scalars.newBuilder()
          .setBoolField(true)
          .setInt32Field(-1)
          .setSint32Field(Integer.MIN_VALUE)
          .setSfixed32Field(Integer.MAX_VALUE)
          .setUint32Field(-1)
          .setFixed32Field(1)
          .setInt64Field(-1L)
          .setSint64Field(Long.MIN_VALUE)
          .setSfixed64Field(Long.MAX_VALUE)
          .setUint64Field(-1L)
          .setFixed64Field(1L)
          .setFloatField(Float.NaN)
          .setDoubleField(-0.5)
          .setStringField("héllo")
          .setBytesField(ByteString.copyFrom(new byte[] {0, -1}))
          .build();

  private static final Presence[] PRESENCE = {
    Presence.getDefaultInstance(),
    Presence.newBuilder()
        .setOptionalField(0)
        .setMessageField(Item.getDefaultInstance())
        .setText("")
        .build(),
    Presence.newBuilder().setItem(Item.newBuilder().setName("a").setQuantity(2)).build()
  };

  private static final Enums ENUMS =
      Enums.newBuilder()
          .setColor(Color.GREEN)
          .setShape(Shape.CIRCLE)
          .addColors(Color.RED)
          .addColors(Color.COLOR_UNSPECIFIED)
          .putColorsByName("g", Color.GREEN)
          .setOptionalColor(Color.COLOR_UNSPECIFIED)
          .build();

  private static final Containers CONTAINERS =
      Containers.newBuilder()
          .addInts(1)
          .addInts(2)
          .addItems(Item.newBuilder().setName("a").setQuantity(1))
          .addItems(Item.getDefaultInstance())
          .putCounts("x", 1L)
          .putCounts("y", 2L)
          .putItemsById(3, Item.newBuilder().setName("c").build())
          .putItemsById(4, Item.getDefaultInstance())
          .putFlags(true, -0.0)
          .putBlobs(-1L, ByteString.copyFromUtf8("b"))
          .build();

  private static final WellKnownTypes WELL_KNOWN_TYPES =
      WellKnownTypes.newBuilder()
          .setTimestamp(Timestamp.newBuilder().setSeconds(-1).setNanos(999_000_000))
          .setStruct(
              Struct.newBuilder()
                  .putFields("a", Value.newBuilder().setNumberValue(1e300).build())
                  .putFields(
                      "b",
                      Value.newBuilder()
                          .setStructValue(
                              Struct.newBuilder()
                                  .putFields(
                                      "c", Value.newBuilder().setStringValue("\"\\\u0001").build()))
                          .build()))
          .setValue(Value.newBuilder().setNumberValue(-0.0))
          .setListValue(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setBoolValue(true))
                  .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE))
                  .addValues(Value.newBuilder().setListValue(ListValue.getDefaultInstance())))
          .addTimestamps(Timestamp.newBuilder().setSeconds(253402300799L))
          .putValues("k", Value.newBuilder().setStringValue("v").build())
          .build();

  // The deepest value that can be converted
  private static final WellKnownTypes DEEP_VALUE = createDeepValue(100);

  private static final Proto2Message[] PROTO2 = {
    Proto2Message.newBuilder().setRequiredField("a").build(),
    Proto2Message.newBuilder()
        .setRequiredField("")
        .setOptionalField(7)
        .setPoint(Proto2Message.Point.newBuilder().setX(1).addY(2))
        .setLevel(Level.HIGH)
        .addLevels(Level.LOW)
        .build()
  };

  private static final EditionsMessage EDITIONS =
      EditionsMessage.newBuilder()
          .setExplicitField(0)
          .setRequiredField(5)
          .setDelimited(EditionsMessage.Inner.newBuilder().setValue(1))
          .addExpanded(2)
          .build();

  private BufferAllocator allocator;

  @BeforeEach
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void tearDown() {
    allocator.close();
  }

  private static WellKnownTypes createDeepValue(int depth) {
    Value value = Value.newBuilder().setBoolValue(true).build();
    for (int i = 0; i < depth; i++) {
      value = Value.newBuilder().setListValue(ListValue.newBuilder().addValues(value)).build();
    }
    return WellKnownTypes.newBuilder().setValue(value).build();
  }

  static Stream<Arguments> roundTrips() {
    List<Arguments> configs =
        Arrays.asList(
            Arguments.of("default", (UnaryOperator<ProtobufToArrowConfigBuilder>) b -> b),
            Arguments.of(
                "signed, milliseconds",
                (UnaryOperator<ProtobufToArrowConfigBuilder>)
                    b -> b.setUnsignedAsSigned(true).setTimestampUnit(TimeUnit.MILLISECOND)),
            Arguments.of(
                "enum names, unknown enum values as null",
                (UnaryOperator<ProtobufToArrowConfigBuilder>)
                    b ->
                        b.setEnumMapping(ProtobufToArrowConfig.EnumMapping.NAME)
                            .setUnknownEnumValues(ProtobufToArrowConfig.UnknownEnumValues.NULL)),
            Arguments.of(
                "enum numbers",
                (UnaryOperator<ProtobufToArrowConfigBuilder>)
                    b -> b.setEnumMapping(ProtobufToArrowConfig.EnumMapping.NUMBER)));
    List<List<Message>> messageLists =
        Arrays.asList(
            List.of(SCALARS, Scalars.getDefaultInstance()),
            Arrays.asList(PRESENCE),
            List.of(ENUMS, Enums.getDefaultInstance()),
            List.of(Drawing.newBuilder().setShape(Shape.SQUARE).setSize(Size.BIG).build()),
            List.of(CONTAINERS, Containers.getDefaultInstance()),
            List.of(WELL_KNOWN_TYPES, WellKnownTypes.getDefaultInstance(), DEEP_VALUE),
            Arrays.asList(PROTO2),
            List.of(EDITIONS));
    return configs.stream()
        .flatMap(
            config ->
                messageLists.stream()
                    .map(
                        messages ->
                            Arguments.of(
                                config.get()[0],
                                config.get()[1],
                                messages.get(0).getDescriptorForType().getName(),
                                messages)));
  }

  /** Converts the messages to a single batch and back. */
  private List<Message> roundTrip(ProtobufToArrowConfigBuilder builder, List<Message> messages) {
    Message prototype = messages.get(0);
    try (ProtobufToArrowVectorIterator iterator =
            ProtobufToArrow.protobufToArrowIterator(
                prototype.getDescriptorForType(), messages.iterator(), builder.build());
        VectorSchemaRoot root = iterator.next()) {
      return produce(
          root,
          prototype.getDescriptorForType(),
          iterator.getDictionaryProvider(),
          prototype::newBuilderForType);
    }
  }

  private static List<Message> produce(
      VectorSchemaRoot root,
      Descriptor descriptor,
      DictionaryProvider provider,
      Supplier<Message.Builder> builders) {
    CompositeProtobufProducer producer =
        ArrowToProtobufUtils.createCompositeProducer(root.getFieldVectors(), descriptor, provider);
    List<Message> messages = new ArrayList<>();
    for (int i = 0; i < root.getRowCount(); i++) {
      Message.Builder builder = builders.get();
      producer.produce(i, builder);
      messages.add(builder.build());
    }
    return messages;
  }

  private static List<Message> produce(VectorSchemaRoot root, Message prototype) {
    return produce(root, prototype.getDescriptorForType(), null, prototype::newBuilderForType);
  }

  private VectorSchemaRoot convert(Message... messages) {
    try (ProtobufToArrowVectorIterator iterator =
        ProtobufToArrow.protobufToArrowIterator(
            messages[0].getDescriptorForType(),
            Arrays.asList(messages).iterator(),
            new ProtobufToArrowConfigBuilder(allocator)
                .setEnumMapping(ProtobufToArrowConfig.EnumMapping.NAME)
                .build())) {
      return iterator.next();
    }
  }

  private VectorSchemaRoot createRoot(Field... fields) {
    return VectorSchemaRoot.create(new Schema(Arrays.asList(fields)), allocator);
  }

  @ParameterizedTest(name = "{0}: {2}")
  @MethodSource("roundTrips")
  public void testRoundTrip(
      String configName,
      UnaryOperator<ProtobufToArrowConfigBuilder> config,
      String typeName,
      List<Message> messages) {
    assertEquals(
        messages, roundTrip(config.apply(new ProtobufToArrowConfigBuilder(allocator)), messages));
  }

  @ParameterizedTest
  @ValueSource(
      ints = {
        1,
        2,
        ProtobufToArrowVectorIterator.DEFAULT_BATCH_SIZE,
        ProtobufToArrowVectorIterator.NO_LIMIT_BATCH_SIZE
      })
  public void testIpcRoundTrip(int targetBatchSize) throws Exception {
    // Lists, maps and dictionaries under null structs, written and read with Arrow IPC
    Inner inner =
        Inner.newBuilder()
            .setName("x")
            .addInts(1)
            .addInts(2)
            .putColorsByName("k", Color.GREEN)
            .addColors(Color.RED)
            .setTimestamp(Timestamp.newBuilder().setSeconds(5))
            .build();
    List<Message> messages =
        List.of(
            Outer.newBuilder().setId(1).build(),
            Outer.newBuilder()
                .setId(2)
                .setInner(inner)
                .addInners(inner)
                .setEmpty(Empty.getDefaultInstance())
                .putCounts("a", 1)
                .build(),
            Outer.newBuilder().setId(3).build(),
            Outer.newBuilder().setId(4).setInner(inner.toBuilder().setName("z")).build());
    ProtobufToArrowConfig config =
        new ProtobufToArrowConfigBuilder(allocator).setTargetBatchSize(targetBatchSize).build();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (ProtobufToArrowVectorIterator iterator =
            ProtobufToArrow.protobufToArrowIterator(
                Outer.getDescriptor(), messages.iterator(), config);
        VectorSchemaRoot sink =
            VectorSchemaRoot.create(
                ProtobufToArrow.protobufToArrowSchema(Outer.getDescriptor(), config), allocator);
        ArrowStreamWriter writer =
            new ArrowStreamWriter(
                sink, iterator.getDictionaryProvider(), Channels.newChannel(out))) {
      writer.start();
      while (iterator.hasNext()) {
        try (VectorSchemaRoot root = iterator.next();
            ArrowRecordBatch batch = new VectorUnloader(root).getRecordBatch()) {
          new VectorLoader(sink).load(batch);
          writer.writeBatch();
        }
      }
      writer.end();
    }

    List<Message> read = new ArrayList<>();
    try (ArrowStreamReader reader =
        new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator)) {
      while (reader.loadNextBatch()) {
        read.addAll(
            produce(
                reader.getVectorSchemaRoot(), Outer.getDescriptor(), reader, Outer::newBuilder));
      }
    }
    assertEquals(messages, read);
  }

  @Test
  public void testUnknownEnumValues() {
    // Numbers can represent the values of open enums that are not defined
    Enums unknown = Enums.newBuilder().setColorValue(7).addColorsValue(8).build();
    ProtobufToArrowConfigBuilder builder =
        new ProtobufToArrowConfigBuilder(allocator)
            .setEnumMapping(ProtobufToArrowConfig.EnumMapping.NUMBER);
    assertEquals(List.of(unknown), roundTrip(builder, List.of(unknown)));

    // Unknown values converted to null clear singular fields, and cannot be converted back in lists
    // and maps
    ProtobufToArrowConfigBuilder nullBuilder =
        new ProtobufToArrowConfigBuilder(allocator)
            .setUnknownEnumValues(ProtobufToArrowConfig.UnknownEnumValues.NULL);
    assertEquals(
        List.of(Enums.getDefaultInstance()),
        roundTrip(nullBuilder, List.of(Enums.newBuilder().setColorValue(7).build())));
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> roundTrip(nullBuilder, List.of(Enums.newBuilder().addColorsValue(8).build())));
    assertEquals(
        "Repeated field arrow.adapter.protobuf.Enums.colors cannot have null elements",
        e.getMessage());
    e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                roundTrip(
                    nullBuilder, List.of(Enums.newBuilder().putColorsByNameValue("k", 9).build())));
    assertEquals(
        "Map field arrow.adapter.protobuf.Enums.colors_by_name cannot have null values",
        e.getMessage());

    // Closed enums cannot have such values
    try (VectorSchemaRoot root =
        createRoot(Field.notNullable("level", new ArrowType.Int(32, true)))) {
      ((IntVector) root.getVector("level")).setSafe(0, 5);
      root.setRowCount(1);
      e =
          assertThrows(
              IllegalArgumentException.class,
              () -> produce(root, Proto2Message.getDefaultInstance()));
      assertEquals(
          "Enum value 5 is not defined in closed enum arrow.adapter.protobuf.Level",
          e.getMessage());
    }
  }

  @Test
  public void testDynamicMessages() throws Exception {
    for (Message message :
        Arrays.asList(SCALARS, PRESENCE[2], ENUMS, CONTAINERS, WELL_KNOWN_TYPES, PROTO2[1])) {
      Descriptor descriptor = message.getDescriptorForType();
      try (ProtobufToArrowVectorIterator iterator =
              ProtobufToArrow.protobufToArrowIterator(
                  descriptor,
                  List.of(message).iterator(),
                  new ProtobufToArrowConfigBuilder(allocator).build());
          VectorSchemaRoot root = iterator.next()) {
        Message actual =
            produce(
                    root,
                    descriptor,
                    iterator.getDictionaryProvider(),
                    () -> DynamicMessage.newBuilder(descriptor))
                .get(0);
        assertEquals(message, message.getParserForType().parseFrom(actual.toByteString()));
      }
    }
  }

  @Test
  public void testNullValues() {
    // Null values clear fields, so that builders can be reused
    try (VectorSchemaRoot root = convert(PRESENCE[1])) {
      for (FieldVector vector : root.getFieldVectors()) {
        vector.setNull(0);
      }
      CompositeProtobufProducer producer =
          ArrowToProtobufUtils.createCompositeProducer(
              root.getFieldVectors(), Presence.getDescriptor());
      Presence.Builder builder = PRESENCE[1].toBuilder().setImplicitField(1);
      producer.produce(0, builder);
      assertEquals(Presence.getDefaultInstance(), builder.build());
    }

    try (VectorSchemaRoot root = convert(CONTAINERS)) {
      // Protobuf maps have no null entries, keys or values
      StructVector entries =
          (StructVector) ((MapVector) root.getVector("items_by_id")).getDataVector();
      entries.getChild(MapVector.VALUE_NAME).setNull(0);
      IllegalArgumentException e =
          assertThrows(IllegalArgumentException.class, () -> produce(root, CONTAINERS));
      assertEquals(
          "Map field arrow.adapter.protobuf.Containers.items_by_id cannot have null values",
          e.getMessage());

      entries.getChild(MapVector.KEY_NAME).setNull(0);
      e = assertThrows(IllegalArgumentException.class, () -> produce(root, CONTAINERS));
      assertEquals(
          "Map field arrow.adapter.protobuf.Containers.items_by_id cannot have null keys",
          e.getMessage());

      entries.setNull(0);
      e = assertThrows(IllegalArgumentException.class, () -> produce(root, CONTAINERS));
      assertEquals(
          "Map field arrow.adapter.protobuf.Containers.items_by_id cannot have null entries",
          e.getMessage());

      ((ListVector) root.getVector("ints")).getDataVector().setNull(1);
      e = assertThrows(IllegalArgumentException.class, () -> produce(root, CONTAINERS));
      assertEquals(
          "Repeated field arrow.adapter.protobuf.Containers.ints cannot have null elements",
          e.getMessage());
    }
  }

  @Test
  public void testOneofConflict() {
    try (VectorSchemaRoot root = convert(PRESENCE[2])) {
      ((VarCharVector) root.getVector("text")).setSafe(0, "t".getBytes(StandardCharsets.UTF_8));
      IllegalArgumentException e =
          assertThrows(IllegalArgumentException.class, () -> produce(root, PRESENCE[2]));
      assertEquals(
          "Fields text and item of oneof arrow.adapter.protobuf.Presence.choice are both set at"
              + " index 0",
          e.getMessage());
    }
  }

  @Test
  public void testEnumDictionary() {
    // The dictionary can hold the value names in any order
    DictionaryEncoding encoding = new DictionaryEncoding(5, false, new ArrowType.Int(8, true));
    try (DictionaryProvider.MapDictionaryProvider provider =
            new DictionaryProvider.MapDictionaryProvider();
        VectorSchemaRoot root =
            createRoot(
                new Field(
                    "color", new FieldType(false, encoding.getIndexType(), encoding), null))) {
      VarCharVector names = new VarCharVector("names", allocator);
      provider.put(new Dictionary(names, encoding));
      setNames(names, "GREEN", "PURPLE", "RED");
      TinyIntVector indexes = (TinyIntVector) root.getVector("color");
      indexes.setSafe(0, 2);
      indexes.setSafe(1, 0);
      root.setRowCount(2);
      CompositeProtobufProducer producer =
          ArrowToProtobufUtils.createCompositeProducer(
              root.getFieldVectors(), Enums.getDescriptor(), provider);
      assertEquals(
          List.of(
              Enums.newBuilder().setColor(Color.RED).build(),
              Enums.newBuilder().setColor(Color.GREEN).build()),
          produce(root, Enums.getDescriptor(), provider, Enums::newBuilder));

      Enums.Builder builder = Enums.newBuilder();
      indexes.setSafe(0, 1);
      IllegalArgumentException e =
          assertThrows(IllegalArgumentException.class, () -> producer.produce(0, builder));
      assertEquals(
          "Enum value PURPLE is not defined in arrow.adapter.protobuf.Color", e.getMessage());

      // Dictionaries reloaded in place, as by IPC readers, are read again
      setNames(names, "RED", "GREEN");
      producer.produce(1, builder);
      assertEquals(Color.RED, builder.getColor());

      indexes.setSafe(0, 99);
      e = assertThrows(IllegalArgumentException.class, () -> producer.produce(0, builder));
      assertEquals(
          "Dictionary index 99 of color does not refer to a value of dictionary 5", e.getMessage());
    }
  }

  private static void setNames(VarCharVector names, String... values) {
    names.allocateNew();
    for (int i = 0; i < values.length; i++) {
      names.setSafe(i, values[i].getBytes(StandardCharsets.UTF_8));
    }
    names.setValueCount(values.length);
  }

  @Test
  public void testInvalidValues() {
    try (VectorSchemaRoot root =
        createRoot(Field.notNullable("uint32_field", new ArrowType.Int(64, true)))) {
      ((BigIntVector) root.getVector("uint32_field")).setSafe(0, 1L << 32);
      root.setRowCount(1);
      IllegalArgumentException e =
          assertThrows(IllegalArgumentException.class, () -> produce(root, SCALARS));
      assertEquals("Value 4294967296 of uint32_field is out of range for uint32", e.getMessage());
    }

    try (TimeStampSecTZVector timestamp = new TimeStampSecTZVector("timestamp", allocator, "UTC")) {
      timestamp.setSafe(0, Long.MAX_VALUE);
      timestamp.setValueCount(1);
      CompositeProtobufProducer producer =
          ArrowToProtobufUtils.createCompositeProducer(
              List.of(timestamp), WellKnownTypes.getDescriptor());
      IllegalArgumentException e =
          assertThrows(
              IllegalArgumentException.class,
              () -> producer.produce(0, WellKnownTypes.newBuilder()));
      assertEquals(
          "Timestamp 9223372036854775807 in unit SECOND is out of the range of protobuf timestamps",
          e.getMessage());
    }

    // JSON is parsed strictly, since JsonFormat's parser reads malformed JSON as other values
    WellKnownTypes parsed =
        parseJson("value", " [ 1E2 , \"\\u0041\\/\\n\\ud83d\\ude00\" , {} , null ] ");
    assertEquals(
        ListValue.newBuilder()
            .addValues(Value.newBuilder().setNumberValue(100))
            .addValues(Value.newBuilder().setStringValue("A/\n😀"))
            .addValues(Value.newBuilder().setStructValue(Struct.getDefaultInstance()))
            .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE))
            .build(),
        parsed.getValue().getListValue());
    for (String json :
        List.of(
            "",
            " ",
            "tru",
            "NaN",
            "01",
            "0x10",
            "1e400",
            "1 2",
            "[1] x",
            "{\"a\":1};",
            "[1,]",
            "[1,,2]",
            "{a:1}",
            "{'a':1}",
            "// c\n1",
            "{\"a\":1,\"a\":2}",
            "\"\u0001\"",
            "\"\\'\"",
            "\"\\ud800\"",
            "\"\\u00zz\"",
            "TRUE",
            "Null",
            "[".repeat(101) + "]".repeat(101),
            "[".repeat(100_000))) {
      // The cause says what is wrong, rather than Gson's advice to parse leniently
      assertEquals(IllegalArgumentException.class, parseInvalidJson(json).getClass());
    }
    assertEquals("Malformed JSON at path $[1]", parseInvalidJson("[1,,2]").getMessage());
    assertEquals("Duplicate key at path $.a", parseInvalidJson("{\"a\":1,\"a\":2}").getMessage());
    assertEquals("Unpaired surrogate at path $[0]", parseInvalidJson("[\"\\ud800\"]").getMessage());
    assertEquals(
        "Number out of the range of doubles at path $.a",
        parseInvalidJson("{\"a\":-1e400}").getMessage());
    assertEquals(
        "JSON value is nested more than 100 levels deep",
        parseInvalidJson("[".repeat(101) + "]".repeat(101)).getMessage());

    // JSON of the wrong kind for the field
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> parseJson("struct", "[1]"));
    assertEquals(
        "Invalid JSON for field arrow.adapter.protobuf.WellKnownTypes.struct", e.getMessage());
    assertEquals(InvalidProtocolBufferException.class, e.getCause().getClass());
  }

  private Throwable parseInvalidJson(String json) {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> parseJson("value", json));
    assertEquals(
        "Invalid JSON for field arrow.adapter.protobuf.WellKnownTypes.value", e.getMessage());
    return e.getCause();
  }

  private WellKnownTypes parseJson(String fieldName, String json) {
    try (VarCharVector vector = new VarCharVector(fieldName, allocator)) {
      vector.setSafe(0, json.getBytes(StandardCharsets.UTF_8));
      vector.setValueCount(1);
      WellKnownTypes.Builder builder = WellKnownTypes.newBuilder();
      ArrowToProtobufUtils.createCompositeProducer(List.of(vector), WellKnownTypes.getDescriptor())
          .produce(0, builder);
      return builder.build();
    }
  }

  @Test
  public void testLargeAndViewVectors() {
    Scalars expected =
        Scalars.newBuilder()
            .setStringField("s")
            .setBytesField(ByteString.copyFromUtf8("b"))
            .build();
    try (LargeVarCharVector largeString = new LargeVarCharVector("string_field", allocator);
        LargeVarBinaryVector largeBytes = new LargeVarBinaryVector("bytes_field", allocator);
        ViewVarCharVector viewString = new ViewVarCharVector("string_field", allocator);
        ViewVarBinaryVector viewBytes = new ViewVarBinaryVector("bytes_field", allocator)) {
      largeString.setSafe(0, "s".getBytes(StandardCharsets.UTF_8));
      largeBytes.setSafe(0, "b".getBytes(StandardCharsets.UTF_8));
      viewString.setSafe(0, "s".getBytes(StandardCharsets.UTF_8));
      viewBytes.setSafe(0, "b".getBytes(StandardCharsets.UTF_8));
      for (List<FieldVector> vectors :
          List.of(
              List.<FieldVector>of(largeString, largeBytes),
              List.<FieldVector>of(viewString, viewBytes))) {
        Scalars.Builder builder = Scalars.newBuilder();
        ArrowToProtobufUtils.createCompositeProducer(vectors, Scalars.getDescriptor())
            .produce(0, builder);
        assertEquals(expected, builder.build());
      }
    }
  }

  @Test
  public void testUnsupportedVectors() {
    DictionaryEncoding encoding = new DictionaryEncoding(0, false, new ArrowType.Int(32, true));
    try (IntVector stringField = new IntVector("string_field", allocator);
        FixedSizeBinaryVector bytesField = new FixedSizeBinaryVector("bytes_field", allocator, 16);
        IntVector encodedField =
            new IntVector(
                "int32_field", new FieldType(false, encoding.getIndexType(), encoding), allocator);
        TimeStampMicroVector timestamp = new TimeStampMicroVector("timestamp", allocator);
        IntVector missing = new IntVector("missing", allocator);
        VarCharVector otherStringField = new VarCharVector("string_field", allocator);
        MapVector emptyMap = MapVector.empty("counts", allocator, false);
        MapVector childlessEntriesMap = MapVector.empty("counts", allocator, false)) {
      IllegalArgumentException e =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  ArrowToProtobufUtils.createCompositeProducer(
                      List.of(stringField), Scalars.getDescriptor()));
      assertEquals(
          "Cannot convert Int(32, true) vector string_field to protobuf field"
              + " arrow.adapter.protobuf.Scalars.string_field",
          e.getMessage());

      e =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  ArrowToProtobufUtils.createCompositeProducer(
                      List.of(bytesField), Scalars.getDescriptor()));
      assertEquals(
          "Cannot convert FixedSizeBinary(16) vector bytes_field to protobuf field"
              + " arrow.adapter.protobuf.Scalars.bytes_field",
          e.getMessage());

      e =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  ArrowToProtobufUtils.createCompositeProducer(
                      List.of(encodedField), Scalars.getDescriptor()));
      assertEquals(
          "Cannot convert dictionary-encoded Int(32, true) vector int32_field to protobuf field"
              + " arrow.adapter.protobuf.Scalars.int32_field",
          e.getMessage());

      // Timestamps without a time zone do not identify an instant
      assertThrows(
          IllegalArgumentException.class,
          () ->
              ArrowToProtobufUtils.createCompositeProducer(
                  List.of(timestamp), WellKnownTypes.getDescriptor()));

      e =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  ArrowToProtobufUtils.createCompositeProducer(
                      List.of(missing), Scalars.getDescriptor()));
      assertEquals(
          "Field missing does not exist in arrow.adapter.protobuf.Scalars", e.getMessage());

      e =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  ArrowToProtobufUtils.createCompositeProducer(
                      List.of(otherStringField, stringField), Scalars.getDescriptor()));
      assertEquals("More than one vector is named string_field", e.getMessage());

      // Map vectors without an entries struct of two children
      e =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  ArrowToProtobufUtils.createCompositeProducer(
                      List.of(emptyMap), Containers.getDescriptor()));
      assertEquals(
          "Cannot convert Null vector $data$ to protobuf field"
              + " arrow.adapter.protobuf.Containers.counts",
          e.getMessage());
      childlessEntriesMap.addOrGetVector(FieldType.notNullable(ArrowType.Struct.INSTANCE));
      e =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  ArrowToProtobufUtils.createCompositeProducer(
                      List.of(childlessEntriesMap), Containers.getDescriptor()));
      assertEquals(
          "Cannot convert Struct vector entries to protobuf field"
              + " arrow.adapter.protobuf.Containers.counts",
          e.getMessage());
    }
  }
}
