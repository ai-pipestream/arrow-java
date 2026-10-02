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

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.FieldVector;

/**
 * Sets the fields of protobuf messages from the values of a set of vectors, each with its own
 * producer. Create instances with {@link ArrowToProtobufUtils#createCompositeProducer}.
 */
public final class CompositeProtobufProducer {

  /** Reads a field value, or an element of a repeated field, from a non-null vector entry. */
  @FunctionalInterface
  interface ValueProducer {
    Object produce(int index, Message.Builder builder);
  }

  private final FieldDescriptor[] fields;

  private final FieldVector[] vectors;

  private final ValueProducer[] producers;

  // The positions of the preceding fields of the same oneof as each field
  private final int[][] precedingOneofFields;

  CompositeProtobufProducer(
      List<FieldDescriptor> fields, List<FieldVector> vectors, List<ValueProducer> producers) {
    this.fields = fields.toArray(new FieldDescriptor[0]);
    this.vectors = vectors.toArray(new FieldVector[0]);
    this.producers = producers.toArray(new ValueProducer[0]);
    this.precedingOneofFields = new int[this.fields.length][];
    for (int i = 0; i < this.fields.length; i++) {
      OneofDescriptor oneof = this.fields[i].getRealContainingOneof();
      List<Integer> preceding = new ArrayList<>();
      for (int j = 0; oneof != null && j < i; j++) {
        if (oneof.equals(this.fields[j].getRealContainingOneof())) {
          preceding.add(j);
        }
      }
      precedingOneofFields[i] = preceding.stream().mapToInt(Integer::intValue).toArray();
    }
  }

  /**
   * Sets the fields of a message that have a vector from the values at the given index. Fields
   * whose value is null are cleared, and fields without a vector are left unchanged, so a builder
   * that is reused for several messages must be cleared first unless the vectors cover every field.
   *
   * @param index the index of the values
   * @param builder a builder of messages with the descriptor that the producer was created for
   * @throws IllegalArgumentException if a value cannot be converted, a list has null elements, a
   *     map has null entries, keys or values, or several fields of a oneof are not null
   */
  public void produce(int index, Message.Builder builder) {
    for (int i = 0; i < fields.length; i++) {
      if (vectors[i].isNull(index)) {
        builder.clearField(fields[i]);
        continue;
      }
      for (int j : precedingOneofFields[i]) {
        if (!vectors[j].isNull(index)) {
          throw new IllegalArgumentException(
              "Fields "
                  + fields[j].getName()
                  + " and "
                  + fields[i].getName()
                  + " of oneof "
                  + fields[i].getRealContainingOneof().getFullName()
                  + " are both set at index "
                  + index);
        }
      }
      builder.setField(fields[i], producers[i].produce(index, builder));
    }
  }
}
