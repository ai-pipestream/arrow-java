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
import com.google.protobuf.Message;
import java.util.Iterator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.Schema;

/** Utility class to convert protobuf messages to columnar Arrow format objects. */
public final class ProtobufToArrow {

  private ProtobufToArrow() {}

  /**
   * Convert a protobuf message descriptor to its Arrow equivalent.
   *
   * <p>Each field of the message becomes a field of the schema, with the following type mapping.
   *
   * <table>
   *   <caption>Protobuf to Arrow type mapping</caption>
   *   <thead><tr><th>Protobuf type</th><th>Arrow type</th></tr></thead>
   *   <tbody>
   *     <tr><td>bool</td><td>Bool</td></tr>
   *     <tr><td>int32, sint32, sfixed32</td><td>Int(32, signed)</td></tr>
   *     <tr><td>uint32, fixed32</td><td>Int(32, unsigned), or Int(64, signed)</td></tr>
   *     <tr><td>int64, sint64, sfixed64</td><td>Int(64, signed)</td></tr>
   *     <tr><td>uint64, fixed64</td><td>Int(64, unsigned), or Int(64, signed)</td></tr>
   *     <tr><td>float</td><td>FloatingPoint(SINGLE)</td></tr>
   *     <tr><td>double</td><td>FloatingPoint(DOUBLE)</td></tr>
   *     <tr><td>string</td><td>Utf8</td></tr>
   *     <tr><td>bytes</td><td>Binary</td></tr>
   *     <tr><td>enum</td><td>Dictionary-encoded Int(32, signed), Utf8 or Int(32, signed)</td></tr>
   *     <tr><td>message, group</td><td>Struct</td></tr>
   *     <tr><td>repeated</td><td>List</td></tr>
   *     <tr><td>map</td><td>Map</td></tr>
   *   </tbody>
   * </table>
   *
   * <p>A field is nullable if it tracks presence, as reported by {@link
   * com.google.protobuf.Descriptors.FieldDescriptor#hasPresence()}: message fields, oneof members,
   * proto2 optional and required fields, proto3 {@code optional} fields, and fields with explicit
   * presence in editions. Other fields always have a value, since protobuf does not distinguish an
   * unset field from its default value. Lists, list elements, maps, map keys and map values are
   * never null, except for open enum values if unknown enum values are converted to null.
   *
   * <p>If enums are mapped to dictionaries, enum fields have the ids of the dictionaries that an
   * iterator created now with the same config would use. Creating iterators for other message types
   * can add dictionaries to the provider of the config and change these ids, so with a shared
   * provider, use the schema of the batches instead.
   *
   * <p>Recursive message types cannot be converted, and neither can fields of the well-known types
   * google.protobuf.Timestamp, Struct, Value and ListValue.
   *
   * @param descriptor The protobuf message descriptor to convert
   * @param config Configuration options for conversion
   * @return The equivalent Arrow schema
   * @throws IllegalArgumentException if the message type is recursive
   */
  public static Schema protobufToArrowSchema(Descriptor descriptor, ProtobufToArrowConfig config) {
    Preconditions.checkNotNull(descriptor, "Protobuf descriptor cannot be null");
    Preconditions.checkNotNull(config, "config cannot be null");

    return new SchemaConverter(config, config.getProvider()).convert(descriptor);
  }

  /**
   * Convert protobuf messages to Arrow vectors in batches, with the schema returned by {@link
   * #protobufToArrowSchema}. Fields that track presence and are not set are null. If enums are
   * mapped to dictionaries, the dictionaries are added to the provider of the config, or to a
   * provider that the iterator creates and closes if the config has none.
   *
   * <p>The messages can be generated messages or {@link com.google.protobuf.DynamicMessage}s, but
   * their descriptor must be the given descriptor instance. Messages of the same type whose
   * descriptor was built separately, for example from a {@code FileDescriptorSet}, are rejected.
   *
   * @param descriptor The descriptor of the messages
   * @param messages The messages to convert
   * @param config Configuration options for conversion, which must have an allocator
   * @return An iterator over batches of the converted messages, which must be closed
   * @throws IllegalArgumentException if the message type is recursive
   */
  public static ProtobufToArrowVectorIterator protobufToArrowIterator(
      Descriptor descriptor, Iterator<? extends Message> messages, ProtobufToArrowConfig config) {
    Preconditions.checkNotNull(descriptor, "Protobuf descriptor cannot be null");
    Preconditions.checkNotNull(messages, "messages cannot be null");
    Preconditions.checkNotNull(config, "config cannot be null");

    return ProtobufToArrowVectorIterator.create(descriptor, messages, config);
  }
}
