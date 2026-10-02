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

import com.google.gson.Strictness;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.arrow.vector.types.TimeUnit;

/** Conversions of the protobuf well-known types that have a dedicated Arrow mapping. */
final class WellKnownTypeUtils {

  private static final String TIMESTAMP_TYPE_NAME = "google.protobuf.Timestamp";

  private static final String STRUCT_TYPE_NAME = "google.protobuf.Struct";

  private static final String VALUE_TYPE_NAME = "google.protobuf.Value";

  private static final String LIST_VALUE_TYPE_NAME = "google.protobuf.ListValue";

  // The range of google.protobuf.Timestamp, 0001-01-01T00:00:00Z to 9999-12-31T23:59:59Z
  private static final long MIN_TIMESTAMP_SECONDS = -62_135_596_800L;
  private static final long MAX_TIMESTAMP_SECONDS = 253_402_300_799L;

  private static final long NANOS_PER_SECOND = 1_000_000_000L;

  // JsonFormat's parser accepts this depth, and its printer has no limit
  private static final int MAX_JSON_DEPTH = 100;

  private static final JsonFormat.Printer JSON_PRINTER =
      JsonFormat.printer().omittingInsignificantWhitespace();

  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser();

  private WellKnownTypeUtils() {}

  static boolean isTimestamp(Descriptor messageType) {
    return TIMESTAMP_TYPE_NAME.equals(messageType.getFullName());
  }

  /** Returns whether the type is one of the recursive types that are converted to JSON. */
  static boolean isJson(Descriptor messageType) {
    String name = messageType.getFullName();
    return STRUCT_TYPE_NAME.equals(name)
        || VALUE_TYPE_NAME.equals(name)
        || LIST_VALUE_TYPE_NAME.equals(name);
  }

  /**
   * Converts the fields of a google.protobuf.Timestamp to a timestamp in the given unit, rounded
   * down.
   */
  static long toArrowTimestamp(long seconds, int nanos, TimeUnit unit) {
    if (seconds < MIN_TIMESTAMP_SECONDS
        || seconds > MAX_TIMESTAMP_SECONDS
        || nanos < 0
        || nanos >= NANOS_PER_SECOND) {
      throw new IllegalArgumentException(
          "Invalid google.protobuf.Timestamp with " + seconds + " seconds and " + nanos + " nanos");
    }
    long unitsPerSecond = getUnitsPerSecond(unit);
    long units = nanos / (NANOS_PER_SECOND / unitsPerSecond);
    // Negative seconds are offset by one, so that the product does not overflow for the earliest
    // timestamps of the unit before the positive nanos are added
    long wholeSeconds = seconds < 0 ? seconds + 1 : seconds;
    long fraction = seconds < 0 ? units - unitsPerSecond : units;
    try {
      return Math.addExact(Math.multiplyExact(wholeSeconds, unitsPerSecond), fraction);
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException(
          "Timestamp of " + seconds + " seconds is out of range for unit " + unit, e);
    }
  }

  /** Builds a google.protobuf.Timestamp from a timestamp in the given unit. */
  static Message toProtobufTimestamp(long value, TimeUnit unit, Message.Builder builder) {
    long unitsPerSecond = getUnitsPerSecond(unit);
    long seconds = Math.floorDiv(value, unitsPerSecond);
    if (seconds < MIN_TIMESTAMP_SECONDS || seconds > MAX_TIMESTAMP_SECONDS) {
      throw new IllegalArgumentException(
          "Timestamp "
              + value
              + " in unit "
              + unit
              + " is out of the range of protobuf timestamps");
    }
    long nanos = Math.floorMod(value, unitsPerSecond) * (NANOS_PER_SECOND / unitsPerSecond);
    Descriptor timestampType = builder.getDescriptorForType();
    return builder
        .setField(timestampType.findFieldByNumber(1), seconds)
        .setField(timestampType.findFieldByNumber(2), (int) nanos)
        .buildPartial();
  }

  private static long getUnitsPerSecond(TimeUnit unit) {
    switch (unit) {
      case SECOND:
        return 1L;
      case MILLISECOND:
        return 1_000L;
      case MICROSECOND:
        return 1_000_000L;
      case NANOSECOND:
        return NANOS_PER_SECOND;
      default:
        throw new UnsupportedOperationException("Unsupported time unit: " + unit);
    }
  }

  /** Prints a google.protobuf.Struct, Value or ListValue as JSON. */
  static String toJson(Message message) {
    checkJsonDepth(message, 0);
    try {
      return JSON_PRINTER.print(message);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(
          "Cannot print " + message.getDescriptorForType().getFullName() + " as JSON", e);
    }
  }

  /** Parses JSON into a google.protobuf.Struct, Value or ListValue of the given field. */
  static Message fromJson(String json, FieldDescriptor field, Message.Builder builder) {
    try {
      // JsonFormat's parser is lenient, and reads malformed JSON as some other value
      checkJson(json);
      JSON_PARSER.merge(json, builder);
    } catch (IllegalArgumentException | InvalidProtocolBufferException e) {
      throw new IllegalArgumentException("Invalid JSON for field " + field.getFullName(), e);
    }
    return builder.buildPartial();
  }

  /**
   * Checks that the text is a single JSON value as defined by RFC 8259, without duplicate keys,
   * unpaired surrogates or numbers out of the range of doubles, and nested at most MAX_JSON_DEPTH
   * levels deep.
   */
  private static void checkJson(String json) {
    JsonReader reader = new JsonReader(new StringReader(json));
    reader.setStrictness(Strictness.STRICT);
    Deque<Set<String>> objectKeys = new ArrayDeque<>();
    int depth = 0;
    try {
      do {
        switch (reader.peek()) {
          case BEGIN_ARRAY:
            reader.beginArray();
            depth++;
            break;
          case END_ARRAY:
            reader.endArray();
            depth--;
            break;
          case BEGIN_OBJECT:
            reader.beginObject();
            objectKeys.push(new HashSet<>());
            depth++;
            break;
          case END_OBJECT:
            reader.endObject();
            objectKeys.pop();
            depth--;
            break;
          case NAME:
            String name = reader.nextName();
            checkSurrogates(name, reader.getPath());
            if (!objectKeys.peek().add(name)) {
              throw new IllegalArgumentException("Duplicate key at path " + reader.getPath());
            }
            break;
          case STRING:
            checkSurrogates(reader.nextString(), reader.getPreviousPath());
            break;
          case NUMBER:
            if (Double.isInfinite(Double.parseDouble(reader.nextString()))) {
              throw new IllegalArgumentException(
                  "Number out of the range of doubles at path " + reader.getPreviousPath());
            }
            break;
          default:
            reader.skipValue();
            break;
        }
        if (depth > MAX_JSON_DEPTH) {
          throw new IllegalArgumentException(
              "JSON value is nested more than " + MAX_JSON_DEPTH + " levels deep");
        }
      } while (depth > 0);
      if (reader.peek() != JsonToken.END_DOCUMENT) {
        throw new IllegalArgumentException("More than one JSON value");
      }
    } catch (IOException e) {
      // Gson's messages suggest a lenient mode, which would accept the malformed JSON
      throw new IllegalArgumentException("Malformed JSON at path " + reader.getPath(), e);
    }
  }

  /** Checks that a string has no unpaired surrogates, which protobuf strings cannot hold. */
  private static void checkSurrogates(String string, String path) {
    if (string
        .codePoints()
        .anyMatch(c -> c >= Character.MIN_SURROGATE && c <= Character.MAX_SURROGATE)) {
      throw new IllegalArgumentException("Unpaired surrogate at path " + path);
    }
  }

  /** Checks that Struct and ListValue messages are not nested more than MAX_JSON_DEPTH deep. */
  private static void checkJsonDepth(Message message, int depth) {
    Descriptor type = message.getDescriptorForType();
    int nestedDepth = depth;
    if (STRUCT_TYPE_NAME.equals(type.getFullName())
        || LIST_VALUE_TYPE_NAME.equals(type.getFullName())) {
      nestedDepth++;
      if (nestedDepth > MAX_JSON_DEPTH) {
        throw new IllegalArgumentException(
            "JSON value is nested more than " + MAX_JSON_DEPTH + " levels deep");
      }
    }
    for (FieldDescriptor field : type.getFields()) {
      if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
        continue;
      }
      if (field.isRepeated()) {
        for (Object element : (List<?>) message.getField(field)) {
          checkJsonDepth((Message) element, nestedDepth);
        }
      } else if (message.hasField(field)) {
        checkJsonDepth((Message) message.getField(field), nestedDepth);
      }
    }
  }
}
