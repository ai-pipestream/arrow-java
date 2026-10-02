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

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;

/** Writes the fields of protobuf messages to the vectors of the schema of their descriptor. */
final class MessageConsumer {

  /** Writes a field value, or an element of a repeated field, to a vector. */
  @FunctionalInterface
  private interface ValueConsumer {
    void consume(int index, Object value);
  }

  private final FieldDescriptor[] fields;

  private final FieldVector[] vectors;

  private final ValueConsumer[] consumers;

  // Whether the field is null rather than its default value when it is not set
  private final boolean[] nullIfUnset;

  private MessageConsumer(
      List<FieldDescriptor> fields, List<FieldVector> vectors, ProtobufToArrowConfig config) {
    this.fields = fields.toArray(new FieldDescriptor[0]);
    this.vectors = vectors.toArray(new FieldVector[0]);
    this.consumers = new ValueConsumer[this.fields.length];
    this.nullIfUnset = new boolean[this.fields.length];
    for (int i = 0; i < this.fields.length; i++) {
      FieldDescriptor field = this.fields[i];
      consumers[i] = createConsumer(field, this.vectors[i], config);
      // Map keys and values are never null, although map entry fields can track presence
      nullIfUnset[i] = field.hasPresence() && !field.getContainingType().getOptions().getMapEntry();
    }
  }

  static MessageConsumer create(
      Descriptor descriptor, VectorSchemaRoot root, ProtobufToArrowConfig config) {
    return new MessageConsumer(descriptor.getFields(), root.getFieldVectors(), config);
  }

  void consume(int index, Message message) {
    for (int i = 0; i < fields.length; i++) {
      if (nullIfUnset[i] && !message.hasField(fields[i])) {
        vectors[i].setNull(index);
      } else {
        consumers[i].consume(index, message.getField(fields[i]));
      }
    }
  }

  private static ValueConsumer createConsumer(
      FieldDescriptor field, FieldVector vector, ProtobufToArrowConfig config) {
    if (field.isMapField()) {
      MapVector mapVector = (MapVector) vector;
      StructVector entriesVector = (StructVector) mapVector.getDataVector();
      MessageConsumer entryConsumer =
          new MessageConsumer(
              field.getMessageType().getFields(), entriesVector.getChildrenFromFields(), config);
      ValueConsumer entriesConsumer =
          createListConsumer(mapVector, createMessageConsumer(entriesVector, entryConsumer));
      FieldDescriptor keyField = field.getMessageType().findFieldByNumber(1);
      return (index, value) ->
          entriesConsumer.consume(index, getLastEntryPerKey((List<?>) value, keyField));
    }
    if (field.isRepeated()) {
      ListVector listVector = (ListVector) vector;
      return createListConsumer(
          listVector, createValueConsumer(field, listVector.getDataVector(), config));
    }
    return createValueConsumer(field, vector, config);
  }

  /**
   * Returns the last entry of each key of a map field, in the order of the first entry of each key.
   * DynamicMessage keeps every entry that was parsed, while generated messages keep the last entry
   * of each key.
   */
  private static List<?> getLastEntryPerKey(List<?> entries, FieldDescriptor keyField) {
    if (entries.size() < 2) {
      return entries;
    }
    Map<Object, Object> lastEntries = new LinkedHashMap<>();
    for (Object entry : entries) {
      lastEntries.put(((Message) entry).getField(keyField), entry);
    }
    return lastEntries.size() == entries.size() ? entries : new ArrayList<>(lastEntries.values());
  }

  private static ValueConsumer createListConsumer(
      ListVector vector, ValueConsumer elementConsumer) {
    return (index, value) -> {
      List<?> elements = (List<?>) value;
      int offset = vector.startNewValue(index);
      for (int i = 0; i < elements.size(); i++) {
        elementConsumer.consume(offset + i, elements.get(i));
      }
      vector.endValue(index, elements.size());
    };
  }

  private static ValueConsumer createMessageConsumer(
      StructVector vector, MessageConsumer fieldsConsumer) {
    return (index, value) -> {
      vector.setIndexDefined(index);
      fieldsConsumer.consume(index, (Message) value);
    };
  }

  private static ValueConsumer createValueConsumer(
      FieldDescriptor field, FieldVector vector, ProtobufToArrowConfig config) {
    boolean unsigned = SchemaConverter.isUnsigned(field);
    switch (field.getJavaType()) {
      case BOOLEAN:
        BitVector bitVector = (BitVector) vector;
        return (index, value) -> bitVector.setSafe(index, (Boolean) value ? 1 : 0);
      case INT:
        if (!unsigned) {
          IntVector intVector = (IntVector) vector;
          return (index, value) -> intVector.setSafe(index, (Integer) value);
        }
        if (config.isUnsignedAsSigned()) {
          BigIntVector widenedVector = (BigIntVector) vector;
          return (index, value) ->
              widenedVector.setSafe(index, Integer.toUnsignedLong((Integer) value));
        }
        UInt4Vector uintVector = (UInt4Vector) vector;
        return (index, value) -> uintVector.setSafe(index, (Integer) value);
      case LONG:
        if (unsigned && !config.isUnsignedAsSigned()) {
          UInt8Vector ulongVector = (UInt8Vector) vector;
          return (index, value) -> ulongVector.setSafe(index, (Long) value);
        }
        BigIntVector longVector = (BigIntVector) vector;
        return (index, value) -> longVector.setSafe(index, (Long) value);
      case FLOAT:
        Float4Vector floatVector = (Float4Vector) vector;
        return (index, value) -> floatVector.setSafe(index, (Float) value);
      case DOUBLE:
        Float8Vector doubleVector = (Float8Vector) vector;
        return (index, value) -> doubleVector.setSafe(index, (Double) value);
      case STRING:
        VarCharVector stringVector = (VarCharVector) vector;
        return (index, value) ->
            stringVector.setSafe(index, ((String) value).getBytes(StandardCharsets.UTF_8));
      case BYTE_STRING:
        VarBinaryVector bytesVector = (VarBinaryVector) vector;
        return (index, value) -> bytesVector.setSafe(index, ((ByteString) value).toByteArray());
      case MESSAGE:
        StructVector structVector = (StructVector) vector;
        MessageConsumer fieldsConsumer =
            new MessageConsumer(
                field.getMessageType().getFields(), structVector.getChildrenFromFields(), config);
        return createMessageConsumer(structVector, fieldsConsumer);
      default:
        throw new UnsupportedOperationException(
            "Unsupported type " + field.getType() + " of field " + field.getFullName());
    }
  }
}
