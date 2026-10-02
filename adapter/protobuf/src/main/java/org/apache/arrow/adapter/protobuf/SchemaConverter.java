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

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/** Converts protobuf message descriptors to Arrow schemas. */
final class SchemaConverter {

  // Well-known types that are not converted like other messages
  private static final Set<String> UNSUPPORTED_MESSAGE_TYPES =
      Set.of(
          "google.protobuf.Timestamp",
          "google.protobuf.Struct",
          "google.protobuf.Value",
          "google.protobuf.ListValue");

  private final ProtobufToArrowConfig config;

  // Full names of the message types enclosing the current field, outermost first
  private final Deque<String> messageTypes = new ArrayDeque<>();

  SchemaConverter(ProtobufToArrowConfig config) {
    this.config = config;
  }

  Schema convert(Descriptor descriptor) {
    return new Schema(convertFields(descriptor));
  }

  static boolean isUnsigned(FieldDescriptor field) {
    switch (field.getType()) {
      case UINT32:
      case FIXED32:
      case UINT64:
      case FIXED64:
        return true;
      default:
        return false;
    }
  }

  private List<Field> convertFields(Descriptor messageType) {
    if (messageTypes.contains(messageType.getFullName())) {
      throw new IllegalArgumentException(
          "Recursive message types are not supported: "
              + String.join(" -> ", messageTypes)
              + " -> "
              + messageType.getFullName());
    }
    messageTypes.addLast(messageType.getFullName());
    List<Field> fields = new ArrayList<>(messageType.getFields().size());
    for (FieldDescriptor field : messageType.getFields()) {
      fields.add(convertField(field));
    }
    messageTypes.removeLast();
    return fields;
  }

  private Field convertField(FieldDescriptor field) {
    if (field.isMapField()) {
      Descriptor entryType = field.getMessageType();
      Field keyField = convertValue(entryType.findFieldByNumber(1), MapVector.KEY_NAME, false);
      Field valueField = convertValue(entryType.findFieldByNumber(2), MapVector.VALUE_NAME, false);
      Field entriesField =
          new Field(
              MapVector.DATA_VECTOR_NAME,
              FieldType.notNullable(ArrowType.Struct.INSTANCE),
              Arrays.asList(keyField, valueField));
      return new Field(
          field.getName(),
          FieldType.notNullable(new ArrowType.Map(/* keysSorted= */ false)),
          Collections.singletonList(entriesField));
    }
    if (field.isRepeated()) {
      Field elementField = convertValue(field, ListVector.DATA_VECTOR_NAME, false);
      return new Field(
          field.getName(),
          FieldType.notNullable(ArrowType.List.INSTANCE),
          Collections.singletonList(elementField));
    }
    return convertValue(field, field.getName(), field.hasPresence());
  }

  private Field convertValue(FieldDescriptor field, String name, boolean nullable) {
    final ArrowType arrowType;
    List<Field> children = null;

    switch (field.getJavaType()) {
      case BOOLEAN:
        arrowType = ArrowType.Bool.INSTANCE;
        break;
      case INT:
        if (!isUnsigned(field)) {
          arrowType = new ArrowType.Int(32, /* isSigned= */ true);
        } else if (config.isUnsignedAsSigned()) {
          arrowType = new ArrowType.Int(64, /* isSigned= */ true);
        } else {
          arrowType = new ArrowType.Int(32, /* isSigned= */ false);
        }
        break;
      case LONG:
        arrowType = new ArrowType.Int(64, !isUnsigned(field) || config.isUnsignedAsSigned());
        break;
      case FLOAT:
        arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        break;
      case DOUBLE:
        arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        break;
      case STRING:
        arrowType = ArrowType.Utf8.INSTANCE;
        break;
      case BYTE_STRING:
        arrowType = ArrowType.Binary.INSTANCE;
        break;
      case MESSAGE:
        if (UNSUPPORTED_MESSAGE_TYPES.contains(field.getMessageType().getFullName())) {
          throw new UnsupportedOperationException(
              "Unsupported type "
                  + field.getMessageType().getFullName()
                  + " of field "
                  + field.getFullName());
        }
        arrowType = ArrowType.Struct.INSTANCE;
        children = convertFields(field.getMessageType());
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported type " + field.getType() + " of field " + field.getFullName());
    }

    return new Field(name, new FieldType(nullable, arrowType, null), children);
  }
}
