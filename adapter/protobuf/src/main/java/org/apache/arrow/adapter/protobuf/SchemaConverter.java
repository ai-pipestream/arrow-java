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
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Converts protobuf message descriptors to Arrow schemas, and creates the dictionaries of enum
 * fields that are not in the dictionary provider yet.
 */
final class SchemaConverter {

  // Well-known types that are not converted like other messages
  private static final Set<String> UNSUPPORTED_MESSAGE_TYPES =
      Set.of(
          "google.protobuf.Timestamp",
          "google.protobuf.Struct",
          "google.protobuf.Value",
          "google.protobuf.ListValue");

  // Not sized to the enum, so that adding enum values does not change the schema
  private static final ArrowType.Int DICTIONARY_INDEX_TYPE =
      new ArrowType.Int(32, /* isSigned= */ true);

  private final ProtobufToArrowConfig config;

  private final DictionaryProvider.MapDictionaryProvider provider;

  // Full names of the message types enclosing the current field, outermost first
  private final Deque<String> messageTypes = new ArrayDeque<>();

  // Fields of the same enum type share a dictionary
  private final Map<String, Long> dictionaryIds = new HashMap<>();

  // The enum types of the dictionaries that are not in the provider, by dictionary id
  private final Map<Long, EnumDescriptor> newDictionaries = new LinkedHashMap<>();

  private long nextDictionaryId;

  /**
   * Creates a converter that assigns dictionary ids relative to a provider, which can be null: an
   * enum type gets the id of a dictionary of the provider that holds its values, or else an unused
   * id.
   */
  SchemaConverter(ProtobufToArrowConfig config, DictionaryProvider.MapDictionaryProvider provider) {
    this.config = config;
    this.provider = provider;
    this.nextDictionaryId =
        provider == null
            ? 0
            : provider.getDictionaryIds().stream().mapToLong(Long::longValue).max().orElse(-1) + 1;
  }

  Schema convert(Descriptor descriptor) {
    return new Schema(convertFields(descriptor));
  }

  /** Adds the dictionaries that the converted schemas use and the provider does not have. */
  void addDictionaries(BufferAllocator allocator) {
    for (Map.Entry<Long, EnumDescriptor> entry : newDictionaries.entrySet()) {
      VarCharVector vector = createDictionaryVector(entry.getValue(), allocator);
      provider.put(new Dictionary(vector, createEncoding(entry.getKey())));
    }
    newDictionaries.clear();
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
    DictionaryEncoding dictionary = null;
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
      case ENUM:
        switch (config.getEnumMapping()) {
          case DICTIONARY:
            arrowType = DICTIONARY_INDEX_TYPE;
            dictionary = createEncoding(getDictionaryId(field.getEnumType()));
            break;
          case NAME:
            arrowType = ArrowType.Utf8.INSTANCE;
            break;
          case NUMBER:
            arrowType = new ArrowType.Int(32, /* isSigned= */ true);
            break;
          default:
            throw new UnsupportedOperationException(
                "Unsupported enum mapping: " + config.getEnumMapping());
        }
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

    FieldType fieldType =
        new FieldType(nullable || isUnknownEnumNull(field, config), arrowType, dictionary);
    return new Field(name, fieldType, children);
  }

  /** Returns whether the values of the field can be unknown enum values that become null. */
  static boolean isUnknownEnumNull(FieldDescriptor field, ProtobufToArrowConfig config) {
    return field.getJavaType() == FieldDescriptor.JavaType.ENUM
        && !field.getEnumType().isClosed()
        && config.getEnumMapping() != ProtobufToArrowConfig.EnumMapping.NUMBER
        && config.getUnknownEnumValues() == ProtobufToArrowConfig.UnknownEnumValues.NULL;
  }

  private long getDictionaryId(EnumDescriptor enumType) {
    Long id = dictionaryIds.get(enumType.getFullName());
    if (id == null) {
      id = findDictionary(enumType);
      if (id == null) {
        id = nextDictionaryId++;
        newDictionaries.put(id, enumType);
      }
      dictionaryIds.put(enumType.getFullName(), id);
    }
    return id;
  }

  private Long findDictionary(EnumDescriptor enumType) {
    if (provider == null) {
      return null;
    }
    for (long id : provider.getDictionaryIds()) {
      Dictionary dictionary = provider.lookup(id);
      if (dictionary.getEncoding().equals(createEncoding(id))
          && isDictionaryOf(dictionary.getVector(), enumType)) {
        return id;
      }
    }
    return null;
  }

  private static boolean isDictionaryOf(FieldVector vector, EnumDescriptor enumType) {
    List<EnumValueDescriptor> values = enumType.getValues();
    if (!(vector instanceof VarCharVector)
        || !vector.getName().equals(enumType.getFullName())
        || vector.getValueCount() != values.size()) {
      return false;
    }
    for (int i = 0; i < values.size(); i++) {
      byte[] name = values.get(i).getName().getBytes(StandardCharsets.UTF_8);
      if (!Arrays.equals(((VarCharVector) vector).get(i), name)) {
        return false;
      }
    }
    return true;
  }

  private static DictionaryEncoding createEncoding(long id) {
    return new DictionaryEncoding(id, /* ordered= */ false, DICTIONARY_INDEX_TYPE);
  }

  /** Creates a vector of the enum value names, indexed by the value index. */
  private static VarCharVector createDictionaryVector(
      EnumDescriptor enumType, BufferAllocator allocator) {
    List<EnumValueDescriptor> values = enumType.getValues();
    byte[][] names = new byte[values.size()][];
    long totalBytes = 0;
    for (int i = 0; i < names.length; i++) {
      names[i] = values.get(i).getName().getBytes(StandardCharsets.UTF_8);
      totalBytes += names[i].length;
    }
    VarCharVector vector = new VarCharVector(enumType.getFullName(), allocator);
    try {
      // Sized to the names, since the default capacity is much larger than most enums need
      vector.allocateNew(totalBytes, names.length);
      for (int i = 0; i < names.length; i++) {
        vector.set(i, names[i]);
      }
      vector.setValueCount(names.length);
      return vector;
    } catch (Throwable t) {
      vector.close();
      throw t;
    }
  }
}
