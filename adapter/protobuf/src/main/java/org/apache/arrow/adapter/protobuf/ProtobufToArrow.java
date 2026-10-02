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
   * never null.
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

    return new SchemaConverter(config).convert(descriptor);
  }
}
