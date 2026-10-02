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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.TimeUnit;

/**
 * This class configures the Protobuf-to-Arrow conversion process.
 *
 * <p>A config must not be used by several threads at once, since creating an iterator can add
 * dictionaries to its provider.
 */
public final class ProtobufToArrowConfig {

  /** The Arrow representation of protobuf enum fields. */
  public enum EnumMapping {
    /** Int32 indexes into a dictionary of the enum value names. */
    DICTIONARY,
    /** The enum value name, as Utf8. */
    NAME,
    /** The enum value number, as Int32. */
    NUMBER
  }

  /**
   * The conversion of values of open enums that are not defined in the enum type, which messages
   * can hold when they were written with a newer version of the enum. Only {@link
   * EnumMapping#NUMBER} can represent these values, so this does not apply to it.
   */
  public enum UnknownEnumValues {
    /** Converting a message with an unknown enum value fails. */
    FAIL,
    /** Unknown enum values are converted to null, so fields of open enum types are nullable. */
    NULL
  }

  private final BufferAllocator allocator;

  private final int targetBatchSize;

  private final DictionaryProvider.MapDictionaryProvider provider;

  private final EnumMapping enumMapping;

  private final UnknownEnumValues unknownEnumValues;

  private final boolean unsignedAsSigned;

  private final TimeUnit timestampUnit;

  ProtobufToArrowConfig(
      BufferAllocator allocator,
      int targetBatchSize,
      DictionaryProvider.MapDictionaryProvider provider,
      EnumMapping enumMapping,
      UnknownEnumValues unknownEnumValues,
      boolean unsignedAsSigned,
      TimeUnit timestampUnit) {
    Preconditions.checkArgument(
        targetBatchSize == ProtobufToArrowVectorIterator.NO_LIMIT_BATCH_SIZE || targetBatchSize > 0,
        "invalid targetBatchSize: %s",
        targetBatchSize);
    Preconditions.checkNotNull(enumMapping, "enumMapping cannot be null");
    Preconditions.checkNotNull(unknownEnumValues, "unknownEnumValues cannot be null");
    Preconditions.checkNotNull(timestampUnit, "timestampUnit cannot be null");

    this.allocator = allocator;
    this.targetBatchSize = targetBatchSize;
    this.provider = provider;
    this.enumMapping = enumMapping;
    this.unknownEnumValues = unknownEnumValues;
    this.unsignedAsSigned = unsignedAsSigned;
    this.timestampUnit = timestampUnit;
  }

  /**
   * Returns the allocator that vectors are allocated with.
   *
   * @return the allocator, or null if the config can only convert descriptors
   */
  public BufferAllocator getAllocator() {
    return allocator;
  }

  /**
   * Returns the maximum number of messages in each batch.
   *
   * @return the maximum number of messages in each batch, or {@link
   *     ProtobufToArrowVectorIterator#NO_LIMIT_BATCH_SIZE} to convert all messages into one batch
   */
  public int getTargetBatchSize() {
    return targetBatchSize;
  }

  /**
   * Returns the provider that the dictionaries of enum fields are added to.
   *
   * @return the provider, or null if each iterator creates its own provider
   */
  public DictionaryProvider.MapDictionaryProvider getProvider() {
    return provider;
  }

  /**
   * Returns the Arrow representation of enum fields.
   *
   * @return the Arrow representation of enum fields
   */
  public EnumMapping getEnumMapping() {
    return enumMapping;
  }

  /**
   * Returns the conversion of enum values that are not defined in the enum type.
   *
   * @return the conversion of enum values that are not defined in the enum type
   */
  public UnknownEnumValues getUnknownEnumValues() {
    return unknownEnumValues;
  }

  /**
   * Returns whether unsigned integer fields are mapped to signed Arrow types.
   *
   * @return whether unsigned integer fields are mapped to signed Arrow types
   */
  public boolean isUnsignedAsSigned() {
    return unsignedAsSigned;
  }

  /**
   * Returns the unit of the timestamps that google.protobuf.Timestamp fields are mapped to.
   *
   * @return the unit of the timestamps that google.protobuf.Timestamp fields are mapped to
   */
  public TimeUnit getTimestampUnit() {
    return timestampUnit;
  }
}
