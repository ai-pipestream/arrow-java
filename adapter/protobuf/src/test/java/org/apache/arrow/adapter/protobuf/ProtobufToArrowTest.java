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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.arrow.adapter.protobuf.TestEditionsProtos.EditionsMessage;
import org.apache.arrow.adapter.protobuf.TestProto2Protos.Proto2Message;
import org.apache.arrow.adapter.protobuf.TestProtos.Containers;
import org.apache.arrow.adapter.protobuf.TestProtos.Item;
import org.apache.arrow.adapter.protobuf.TestProtos.Presence;
import org.apache.arrow.adapter.protobuf.TestProtos.Scalars;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.util.ValueVectorUtility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ProtobufToArrowTest {

  private BufferAllocator allocator;

  @BeforeEach
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void tearDown() {
    allocator.close();
  }

  private ProtobufToArrowConfigBuilder configBuilder() {
    return new ProtobufToArrowConfigBuilder(allocator);
  }

  /** Converts the messages into a single batch. */
  private VectorSchemaRoot convert(
      Descriptor descriptor, ProtobufToArrowConfigBuilder builder, Message... messages) {
    ProtobufToArrowConfig config = builder.build();
    try (ProtobufToArrowVectorIterator iterator =
        ProtobufToArrow.protobufToArrowIterator(
            descriptor, Arrays.asList(messages).iterator(), config)) {
      VectorSchemaRoot root = iterator.next();
      assertFalse(iterator.hasNext());
      assertEquals(ProtobufToArrow.protobufToArrowSchema(descriptor, config), root.getSchema());
      return root;
    }
  }

  private VectorSchemaRoot convert(Descriptor descriptor, Message... messages) {
    return convert(descriptor, configBuilder(), messages);
  }

  private static Map<String, Object> item(String name, long quantity) {
    return Map.of("name", new Text(name), "quantity", quantity);
  }

  private static List<Object> getValues(FieldVector vector) {
    List<Object> values = new ArrayList<>();
    for (int i = 0; i < vector.getValueCount(); i++) {
      values.add(vector.getObject(i));
    }
    return values;
  }

  @Test
  public void testScalars() {
    Scalars message =
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
            .setFloatField(1.5f)
            .setDoubleField(-2.5)
            .setStringField("héllo")
            .setBytesField(ByteString.copyFrom(new byte[] {0, -1}))
            .build();

    try (VectorSchemaRoot root =
        convert(Scalars.getDescriptor(), message, Scalars.getDefaultInstance())) {
      assertEquals(2, root.getRowCount());
      assertEquals(true, root.getVector("bool_field").getObject(0));
      assertEquals(-1, root.getVector("int32_field").getObject(0));
      assertEquals(Integer.MIN_VALUE, root.getVector("sint32_field").getObject(0));
      assertEquals(Integer.MAX_VALUE, root.getVector("sfixed32_field").getObject(0));
      assertEquals(
          4294967295L, ((UInt4Vector) root.getVector("uint32_field")).getObjectNoOverflow(0));
      assertEquals(1L, ((UInt4Vector) root.getVector("fixed32_field")).getObjectNoOverflow(0));
      assertEquals(-1L, root.getVector("int64_field").getObject(0));
      assertEquals(Long.MIN_VALUE, root.getVector("sint64_field").getObject(0));
      assertEquals(Long.MAX_VALUE, root.getVector("sfixed64_field").getObject(0));
      assertEquals(
          new BigInteger("18446744073709551615"),
          ((UInt8Vector) root.getVector("uint64_field")).getObjectNoOverflow(0));
      assertEquals(
          BigInteger.ONE, ((UInt8Vector) root.getVector("fixed64_field")).getObjectNoOverflow(0));
      assertEquals(1.5f, root.getVector("float_field").getObject(0));
      assertEquals(-2.5, root.getVector("double_field").getObject(0));
      assertEquals(new Text("héllo"), root.getVector("string_field").getObject(0));
      assertArrayEquals(new byte[] {0, -1}, (byte[]) root.getVector("bytes_field").getObject(0));

      // Fields without presence are never null
      for (FieldVector vector : root.getFieldVectors()) {
        assertFalse(vector.isNull(1), vector.getName());
      }
      assertEquals(0, root.getVector("int32_field").getObject(1));
      assertEquals(new Text(""), root.getVector("string_field").getObject(1));
    }
  }

  @Test
  public void testUnsignedAsSigned() {
    Scalars message = Scalars.newBuilder().setUint32Field(-1).setUint64Field(-1L).build();

    try (VectorSchemaRoot root =
        convert(Scalars.getDescriptor(), configBuilder().setUnsignedAsSigned(true), message)) {
      assertEquals(4294967295L, root.getVector("uint32_field").getObject(0));
      assertEquals(-1L, root.getVector("uint64_field").getObject(0));
    }
  }

  @Test
  public void testPresence() {
    Presence unset = Presence.getDefaultInstance();
    Presence defaults =
        Presence.newBuilder()
            .setOptionalField(0)
            .setMessageField(Item.getDefaultInstance())
            .setText("")
            .build();
    Presence item =
        Presence.newBuilder().setItem(Item.newBuilder().setName("a").setQuantity(2)).build();

    try (VectorSchemaRoot root = convert(Presence.getDescriptor(), unset, defaults, item)) {
      assertEquals(Arrays.asList(0, 0, 0), getValues(root.getVector("implicit_field")));
      assertEquals(Arrays.asList(null, 0, null), getValues(root.getVector("optional_field")));
      assertEquals(
          Arrays.asList(null, item("", 0), null), getValues(root.getVector("message_field")));
      assertEquals(Arrays.asList(null, new Text(""), null), getValues(root.getVector("text")));
      assertEquals(Arrays.asList(null, null, item("a", 2)), getValues(root.getVector("item")));
    }

    // Unset fields are null rather than their default value
    Proto2Message proto2 =
        Proto2Message.newBuilder()
            .setRequiredField("a")
            .setPoint(Proto2Message.Point.newBuilder().addY(1))
            .build();
    try (VectorSchemaRoot root = convert(Proto2Message.getDescriptor(), proto2)) {
      assertEquals(new Text("a"), root.getVector("required_field").getObject(0));
      assertTrue(root.getVector("optional_field").isNull(0));
      assertEquals(List.of(), root.getVector("repeated_field").getObject(0));
      Map<?, ?> point = (Map<?, ?>) root.getVector("point").getObject(0);
      assertFalse(point.containsKey("x"));
      assertEquals(List.of(1), point.get("y"));
    }

    EditionsMessage editions =
        EditionsMessage.newBuilder()
            .setRequiredField(1)
            .setDelimited(EditionsMessage.Inner.getDefaultInstance())
            .build();
    try (VectorSchemaRoot root = convert(EditionsMessage.getDescriptor(), editions)) {
      assertTrue(root.getVector("explicit_field").isNull(0));
      assertEquals(0, root.getVector("implicit_field").getObject(0));
      assertEquals(1, root.getVector("required_field").getObject(0));
      assertEquals(Map.of(), root.getVector("delimited").getObject(0));
    }
  }

  @Test
  public void testContainers() {
    Containers message =
        Containers.newBuilder()
            .addInts(1)
            .addInts(2)
            .addItems(Item.newBuilder().setName("a").setQuantity(1))
            .addItems(Item.newBuilder().setName("b"))
            .putCounts("x", 1L)
            .putCounts("y", 2L)
            .putItemsById(3, Item.newBuilder().setName("c").setQuantity(3).build())
            .putFlags(true, 0.5)
            .putBlobs(-1L, ByteString.EMPTY)
            .build();

    try (VectorSchemaRoot root =
        convert(Containers.getDescriptor(), message, Containers.getDefaultInstance())) {
      assertEquals(List.of(1, 2), root.getVector("ints").getObject(0));
      assertEquals(List.of(item("a", 1), item("b", 0)), root.getVector("items").getObject(0));
      assertEquals(
          List.of(
              Map.of("key", new Text("x"), "value", 1L), Map.of("key", new Text("y"), "value", 2L)),
          root.getVector("counts").getObject(0));
      assertEquals(
          List.of(Map.of("key", 3, "value", item("c", 3))),
          root.getVector("items_by_id").getObject(0));
      assertEquals(
          List.of(Map.of("key", true, "value", 0.5)), root.getVector("flags").getObject(0));
      List<?> blobs = (List<?>) root.getVector("blobs").getObject(0);
      assertEquals(1, blobs.size());
      Map<?, ?> blob = (Map<?, ?>) blobs.get(0);
      assertEquals(-1L, blob.get("key"));
      assertArrayEquals(new byte[0], (byte[]) blob.get("value"));

      // Empty repeated fields and maps are empty rather than null
      for (FieldVector vector : root.getFieldVectors()) {
        assertEquals(List.of(), vector.getObject(1), vector.getName());
      }
    }
  }

  @Test
  public void testDynamicMessages() throws Exception {
    Containers containers =
        Containers.newBuilder()
            .addItems(Item.newBuilder().setName("a"))
            .putItemsById(1, Item.newBuilder().setQuantity(2).build())
            .build();
    Presence presence = Presence.newBuilder().setItem(Item.getDefaultInstance()).build();

    for (Message message : List.of(containers, presence)) {
      Descriptor descriptor = message.getDescriptorForType();
      DynamicMessage dynamicMessage = DynamicMessage.parseFrom(descriptor, message.toByteString());
      try (VectorSchemaRoot expected = convert(descriptor, message);
          VectorSchemaRoot actual = convert(descriptor, dynamicMessage)) {
        assertTrue(expected.equals(actual), descriptor.getFullName());
      }
    }

    // Parsing can give a map key more than once, and generated messages keep its last entry
    ByteString duplicateKeys =
        Containers.newBuilder()
            .putCounts("a", 1L)
            .putCounts("b", 2L)
            .build()
            .toByteString()
            .concat(Containers.newBuilder().putCounts("a", 3L).build().toByteString());
    try (VectorSchemaRoot expected =
            convert(Containers.getDescriptor(), Containers.parseFrom(duplicateKeys));
        VectorSchemaRoot actual =
            convert(
                Containers.getDescriptor(),
                DynamicMessage.parseFrom(Containers.getDescriptor(), duplicateKeys))) {
      assertEquals(
          List.of(
              Map.of("key", new Text("a"), "value", 3L), Map.of("key", new Text("b"), "value", 2L)),
          actual.getVector("counts").getObject(0));
      assertTrue(expected.equals(actual));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {ProtobufToArrowVectorIterator.NO_LIMIT_BATCH_SIZE, 1, 3})
  public void testBatches(int targetBatchSize) {
    List<Containers> messages = new ArrayList<>();
    List<Object> expected = new ArrayList<>();
    for (int i = 0; i < 7; i++) {
      Item item = Item.newBuilder().setName("item" + i).setQuantity(i).build();
      messages.add(
          i % 2 == 0
              ? Containers.getDefaultInstance()
              : Containers.newBuilder().addItems(item).putItemsById(i, item).build());
      expected.add(
          i % 2 == 0 ? List.of() : List.of(Map.of("key", i, "value", item(item.getName(), i))));
    }

    List<Integer> rowCounts = new ArrayList<>();
    List<Object> values = new ArrayList<>();
    try (ProtobufToArrowVectorIterator iterator =
        ProtobufToArrow.protobufToArrowIterator(
            Containers.getDescriptor(),
            messages.iterator(),
            configBuilder().setTargetBatchSize(targetBatchSize).build())) {
      while (iterator.hasNext()) {
        try (VectorSchemaRoot root = iterator.next()) {
          for (FieldVector vector : root.getFieldVectors()) {
            // validateFull does not support map vectors
            if (!(vector instanceof MapVector)) {
              ValueVectorUtility.validateFull(vector);
            }
          }
          rowCounts.add(root.getRowCount());
          values.addAll(getValues(root.getVector("items_by_id")));
        }
      }
    }
    List<Integer> expectedRowCounts =
        targetBatchSize == 1
            ? List.of(1, 1, 1, 1, 1, 1, 1)
            : targetBatchSize == 3 ? List.of(3, 3, 1) : List.of(7);
    assertEquals(expectedRowCounts, rowCounts);
    assertEquals(expected, values);
  }

  @Test
  public void testDescriptorMismatch() throws Exception {
    ProtobufToArrowConfig config = configBuilder().build();
    try (ProtobufToArrowVectorIterator iterator =
        ProtobufToArrow.protobufToArrowIterator(
            Item.getDescriptor(), List.of(Presence.getDefaultInstance()).iterator(), config)) {
      IllegalArgumentException e = assertThrows(IllegalArgumentException.class, iterator::next);
      assertEquals(
          "Expected a message of type arrow.adapter.protobuf.Item,"
              + " but got arrow.adapter.protobuf.Presence",
          e.getMessage());
    }

    // The same type loaded separately, as from a schema registry
    FileDescriptor file = Item.getDescriptor().getFile();
    Descriptor rebuilt =
        FileDescriptor.buildFrom(
                file.toProto(), file.getDependencies().toArray(new FileDescriptor[0]))
            .findMessageTypeByName("Item");
    try (ProtobufToArrowVectorIterator iterator =
        ProtobufToArrow.protobufToArrowIterator(
            Item.getDescriptor(),
            List.of(DynamicMessage.getDefaultInstance(rebuilt)).iterator(),
            config)) {
      IllegalArgumentException e = assertThrows(IllegalArgumentException.class, iterator::next);
      assertEquals(
          "Message of type arrow.adapter.protobuf.Item has a different descriptor instance than"
              + " the iterator. Messages must use the descriptor that the iterator was created"
              + " with.",
          e.getMessage());
    }
  }

  @Test
  public void testFailure() {
    OutOfMemoryError error = new OutOfMemoryError();
    Iterator<Item> messages =
        new Iterator<Item>() {
          private int count;

          @Override
          public boolean hasNext() {
            return true;
          }

          @Override
          public Item next() {
            if (count++ == 2) {
              throw error;
            }
            return Item.newBuilder().setName("item").build();
          }
        };

    try (ProtobufToArrowVectorIterator iterator =
        ProtobufToArrow.protobufToArrowIterator(
            Item.getDescriptor(), messages, configBuilder().setTargetBatchSize(4).build())) {
      assertSame(error, assertThrows(OutOfMemoryError.class, iterator::next));
      // The batch is released, and the iterator cannot be used anymore
      assertEquals(0, allocator.getAllocatedMemory());
      assertSame(error, assertThrows(IllegalStateException.class, iterator::hasNext).getCause());
      assertSame(error, assertThrows(IllegalStateException.class, iterator::next).getCause());
    }
  }
}
