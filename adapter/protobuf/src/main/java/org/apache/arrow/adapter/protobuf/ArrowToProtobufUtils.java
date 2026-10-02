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
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.arrow.adapter.protobuf.CompositeProtobufProducer.ValueProducer;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VariableWidthFieldVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;

/** Utility methods to convert Arrow data to protobuf messages. */
public final class ArrowToProtobufUtils {

  private static final Set<ArrowTypeID> STRING_TYPES =
      EnumSet.of(ArrowTypeID.Utf8, ArrowTypeID.LargeUtf8, ArrowTypeID.Utf8View);

  private static final Set<ArrowTypeID> BINARY_TYPES =
      EnumSet.of(ArrowTypeID.Binary, ArrowTypeID.LargeBinary, ArrowTypeID.BinaryView);

  private static final long MAX_UINT32 = 0xFFFFFFFFL;

  private ArrowToProtobufUtils() {}

  /**
   * Create a composite producer that sets the fields of messages from a set of vectors, typically
   * the vectors of a VectorSchemaRoot. Each vector sets the field with the same name.
   *
   * <p>The vectors can have the types that {@link ProtobufToArrow#protobufToArrowSchema} maps the
   * fields to, with any configuration. In addition, strings and enum names can be LargeUtf8 or
   * Utf8View, bytes can be LargeBinary or BinaryView, and dictionaries of enum value names can hold
   * the names in any order. Timestamps must have a time zone, since timestamps without one do not
   * identify an instant.
   *
   * <p>Dictionaries are read when each value is produced, so a dictionary vector that is reloaded
   * in place, as {@link org.apache.arrow.vector.ipc.ArrowReader} does, can be used across batches.
   *
   * <p>Strings are decoded as UTF-8, and invalid UTF-8, which Arrow does not allow, is replaced
   * with U+FFFD rather than rejected. The JSON of google.protobuf.Struct, Value and ListValue
   * fields must be a single JSON value as defined by RFC 8259, without duplicate keys, unpaired
   * surrogates or numbers out of the range of doubles, and nested at most 100 levels deep.
   *
   * @param vectors The vectors that will be used to produce protobuf messages
   * @param descriptor The descriptor of the messages
   * @param dictionaries The dictionaries of dictionary-encoded enum fields, or null if there are
   *     none
   * @return The resulting composite producer
   * @throws IllegalArgumentException if a vector has no matching field, or a type that cannot be
   *     converted to the field, or if several vectors have the same name
   */
  public static CompositeProtobufProducer createCompositeProducer(
      List<FieldVector> vectors, Descriptor descriptor, DictionaryProvider dictionaries) {
    Preconditions.checkNotNull(vectors, "vectors cannot be null");
    Preconditions.checkNotNull(descriptor, "Protobuf descriptor cannot be null");

    List<FieldDescriptor> fields = new ArrayList<>(vectors.size());
    Set<String> names = new HashSet<>();
    for (FieldVector vector : vectors) {
      if (!names.add(vector.getName())) {
        throw new IllegalArgumentException("More than one vector is named " + vector.getName());
      }
      FieldDescriptor field = descriptor.findFieldByName(vector.getName());
      if (field == null) {
        throw new IllegalArgumentException(
            "Field " + vector.getName() + " does not exist in " + descriptor.getFullName());
      }
      fields.add(field);
    }
    return createCompositeProducer(fields, vectors, dictionaries);
  }

  /**
   * Overload provided for convenience, sets dictionaries = null.
   *
   * @param vectors The vectors that will be used to produce protobuf messages
   * @param descriptor The descriptor of the messages
   * @return The resulting composite producer
   */
  public static CompositeProtobufProducer createCompositeProducer(
      List<FieldVector> vectors, Descriptor descriptor) {
    return createCompositeProducer(vectors, descriptor, null);
  }

  private static CompositeProtobufProducer createCompositeProducer(
      List<FieldDescriptor> fields, List<FieldVector> vectors, DictionaryProvider dictionaries) {
    List<ValueProducer> producers = new ArrayList<>(fields.size());
    for (int i = 0; i < fields.size(); i++) {
      producers.add(createProducer(fields.get(i), vectors.get(i), dictionaries));
    }
    return new CompositeProtobufProducer(fields, vectors, producers);
  }

  private static ValueProducer createProducer(
      FieldDescriptor field, FieldVector vector, DictionaryProvider dictionaries) {
    if (field.isMapField()) {
      MapVector mapVector = cast(vector, MapVector.class, field);
      StructVector entriesVector = cast(mapVector.getDataVector(), StructVector.class, field);
      List<FieldVector> entryVectors = entriesVector.getChildrenFromFields();
      if (entryVectors.size() != 2) {
        throw createTypeMismatchException(field, entriesVector);
      }
      FieldVector keyVector = entryVectors.get(0);
      FieldVector valueVector = entryVectors.get(1);
      ValueProducer entryProducer =
          createMessageProducer(
              field,
              createCompositeProducer(
                  field.getMessageType().getFields(), entryVectors, dictionaries));
      // Protobuf has no null map values, and a default value would be a different value
      String message = "Map field " + field.getFullName() + " cannot have null ";
      return createListProducer(
          mapVector,
          (index, builder) -> {
            if (entriesVector.isNull(index)) {
              throw new IllegalArgumentException(message + "entries");
            }
            if (keyVector.isNull(index)) {
              throw new IllegalArgumentException(message + "keys");
            }
            if (valueVector.isNull(index)) {
              throw new IllegalArgumentException(message + "values");
            }
            return entryProducer.produce(index, builder);
          });
    }
    if (field.isRepeated()) {
      ListVector listVector = cast(vector, ListVector.class, field);
      FieldVector elementVector = listVector.getDataVector();
      ValueProducer elementProducer = createValueProducer(field, elementVector, dictionaries);
      String message = "Repeated field " + field.getFullName() + " cannot have null elements";
      return createListProducer(
          listVector,
          (index, builder) -> {
            if (elementVector.isNull(index)) {
              throw new IllegalArgumentException(message);
            }
            return elementProducer.produce(index, builder);
          });
    }
    return createValueProducer(field, vector, dictionaries);
  }

  private static ValueProducer createListProducer(
      ListVector vector, ValueProducer elementProducer) {
    return (index, builder) -> {
      int start = vector.getElementStartIndex(index);
      int end = vector.getElementEndIndex(index);
      List<Object> elements = new ArrayList<>(end - start);
      for (int i = start; i < end; i++) {
        elements.add(elementProducer.produce(i, builder));
      }
      return elements;
    };
  }

  private static ValueProducer createMessageProducer(
      FieldDescriptor field, CompositeProtobufProducer fieldsProducer) {
    return (index, builder) -> {
      Message.Builder messageBuilder = builder.newBuilderForField(field);
      fieldsProducer.produce(index, messageBuilder);
      return messageBuilder.buildPartial();
    };
  }

  private static ValueProducer createValueProducer(
      FieldDescriptor field, FieldVector vector, DictionaryProvider dictionaries) {
    if (field.getJavaType() == FieldDescriptor.JavaType.ENUM) {
      return createEnumProducer(field, vector, dictionaries);
    }
    if (vector.getField().getDictionary() != null) {
      throw createTypeMismatchException(field, vector);
    }
    boolean unsigned = SchemaConverter.isUnsigned(field);
    switch (field.getJavaType()) {
      case BOOLEAN:
        BitVector bitVector = cast(vector, BitVector.class, field);
        return (index, builder) -> bitVector.get(index) != 0;
      case INT:
        if (!unsigned) {
          IntVector intVector = cast(vector, IntVector.class, field);
          return (index, builder) -> intVector.get(index);
        }
        if (vector instanceof BigIntVector) {
          BigIntVector widenedVector = (BigIntVector) vector;
          return (index, builder) -> {
            long value = widenedVector.get(index);
            if (value < 0 || value > MAX_UINT32) {
              throw new IllegalArgumentException(
                  "Value " + value + " of " + vector.getName() + " is out of range for uint32");
            }
            return (int) value;
          };
        }
        UInt4Vector uintVector = cast(vector, UInt4Vector.class, field);
        return (index, builder) -> uintVector.get(index);
      case LONG:
        if (unsigned && vector instanceof UInt8Vector) {
          UInt8Vector ulongVector = (UInt8Vector) vector;
          return (index, builder) -> ulongVector.get(index);
        }
        BigIntVector longVector = cast(vector, BigIntVector.class, field);
        return (index, builder) -> longVector.get(index);
      case FLOAT:
        Float4Vector floatVector = cast(vector, Float4Vector.class, field);
        return (index, builder) -> floatVector.get(index);
      case DOUBLE:
        Float8Vector doubleVector = cast(vector, Float8Vector.class, field);
        return (index, builder) -> doubleVector.get(index);
      case STRING:
        VariableWidthFieldVector stringVector = castVariableWidth(vector, STRING_TYPES, field);
        return (index, builder) -> getString(stringVector, index);
      case BYTE_STRING:
        VariableWidthFieldVector bytesVector = castVariableWidth(vector, BINARY_TYPES, field);
        return (index, builder) -> ByteString.copyFrom(bytesVector.get(index));
      case MESSAGE:
        return createMessageValueProducer(field, vector, dictionaries);
      default:
        throw new UnsupportedOperationException(
            "Unsupported type " + field.getType() + " of field " + field.getFullName());
    }
  }

  private static ValueProducer createMessageValueProducer(
      FieldDescriptor field, FieldVector vector, DictionaryProvider dictionaries) {
    Descriptor messageType = field.getMessageType();
    if (WellKnownTypeUtils.isTimestamp(messageType)) {
      TimeStampVector timestampVector = cast(vector, TimeStampVector.class, field);
      ArrowType.Timestamp type = (ArrowType.Timestamp) vector.getField().getType();
      if (type.getTimezone() == null) {
        throw createTypeMismatchException(field, vector);
      }
      return (index, builder) ->
          WellKnownTypeUtils.toProtobufTimestamp(
              timestampVector.get(index), type.getUnit(), builder.newBuilderForField(field));
    }
    if (WellKnownTypeUtils.isJson(messageType)) {
      VariableWidthFieldVector jsonVector = castVariableWidth(vector, STRING_TYPES, field);
      return (index, builder) ->
          WellKnownTypeUtils.fromJson(
              getString(jsonVector, index), field, builder.newBuilderForField(field));
    }
    StructVector structVector = cast(vector, StructVector.class, field);
    return createMessageProducer(
        field,
        createCompositeProducer(structVector.getChildrenFromFields(), messageType, dictionaries));
  }

  private static ValueProducer createEnumProducer(
      FieldDescriptor field, FieldVector vector, DictionaryProvider dictionaries) {
    EnumDescriptor enumType = field.getEnumType();
    DictionaryEncoding encoding = vector.getField().getDictionary();
    if (encoding != null) {
      Dictionary dictionary = dictionaries == null ? null : dictionaries.lookup(encoding.getId());
      if (dictionary == null) {
        throw new IllegalArgumentException(
            "Dictionary "
                + encoding.getId()
                + " of "
                + vector.getName()
                + " is not in the provider");
      }
      BaseIntVector indexVector = cast(vector, BaseIntVector.class, field);
      VariableWidthFieldVector names =
          castVariableWidth(dictionary.getVector(), STRING_TYPES, field);
      return (index, builder) -> {
        long dictionaryIndex = indexVector.getValueAsLong(index);
        if (dictionaryIndex < 0
            || dictionaryIndex >= names.getValueCount()
            || names.isNull((int) dictionaryIndex)) {
          throw new IllegalArgumentException(
              "Dictionary index "
                  + dictionaryIndex
                  + " of "
                  + vector.getName()
                  + " does not refer to a value of dictionary "
                  + encoding.getId());
        }
        return findEnumValue(enumType, getString(names, (int) dictionaryIndex));
      };
    }
    if (STRING_TYPES.contains(vector.getField().getType().getTypeID())) {
      VariableWidthFieldVector names = castVariableWidth(vector, STRING_TYPES, field);
      return (index, builder) -> findEnumValue(enumType, getString(names, index));
    }
    IntVector numbers = cast(vector, IntVector.class, field);
    if (!enumType.isClosed()) {
      return (index, builder) -> enumType.findValueByNumberCreatingIfUnknown(numbers.get(index));
    }
    return (index, builder) -> {
      EnumValueDescriptor enumValue = enumType.findValueByNumber(numbers.get(index));
      if (enumValue == null) {
        throw new IllegalArgumentException(
            "Enum value "
                + numbers.get(index)
                + " is not defined in closed enum "
                + enumType.getFullName());
      }
      return enumValue;
    };
  }

  private static EnumValueDescriptor findEnumValue(EnumDescriptor enumType, String name) {
    EnumValueDescriptor enumValue = enumType.findValueByName(name);
    if (enumValue == null) {
      throw new IllegalArgumentException(
          "Enum value " + name + " is not defined in " + enumType.getFullName());
    }
    return enumValue;
  }

  private static String getString(VariableWidthFieldVector vector, int index) {
    return new String(vector.get(index), StandardCharsets.UTF_8);
  }

  private static VariableWidthFieldVector castVariableWidth(
      FieldVector vector, Set<ArrowTypeID> types, FieldDescriptor field) {
    if (!types.contains(vector.getField().getType().getTypeID())
        || vector.getField().getDictionary() != null) {
      throw createTypeMismatchException(field, vector);
    }
    return cast(vector, VariableWidthFieldVector.class, field);
  }

  private static <T> T cast(FieldVector vector, Class<T> vectorClass, FieldDescriptor field) {
    if (!vectorClass.isInstance(vector)) {
      throw createTypeMismatchException(field, vector);
    }
    return vectorClass.cast(vector);
  }

  private static IllegalArgumentException createTypeMismatchException(
      FieldDescriptor field, FieldVector vector) {
    String encoding = vector.getField().getDictionary() == null ? "" : "dictionary-encoded ";
    return new IllegalArgumentException(
        "Cannot convert "
            + encoding
            + vector.getField().getType()
            + " vector "
            + vector.getName()
            + " to protobuf field "
            + field.getFullName());
  }
}
