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
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.TimeUnit;

/** This class builds {@link ProtobufToArrowConfig}s. */
public final class ProtobufToArrowConfigBuilder {

  private final BufferAllocator allocator;

  private int targetBatchSize;

  private DictionaryProvider.MapDictionaryProvider provider;

  private ProtobufToArrowConfig.EnumMapping enumMapping;

  private ProtobufToArrowConfig.UnknownEnumValues unknownEnumValues;

  private boolean unsignedAsSigned;

  private TimeUnit timestampUnit;

  /**
   * Constructs a builder for converting descriptors to schemas. Converting messages requires an
   * allocator, see {@link #ProtobufToArrowConfigBuilder(BufferAllocator)}.
   */
  public ProtobufToArrowConfigBuilder() {
    this(null);
  }

  /**
   * Constructs a builder for converting messages to vectors.
   *
   * @param allocator The memory allocator to construct the Arrow vectors with.
   */
  public ProtobufToArrowConfigBuilder(BufferAllocator allocator) {
    this.allocator = allocator;
    this.targetBatchSize = ProtobufToArrowVectorIterator.DEFAULT_BATCH_SIZE;
    this.provider = null;
    this.enumMapping = ProtobufToArrowConfig.EnumMapping.DICTIONARY;
    this.unknownEnumValues = ProtobufToArrowConfig.UnknownEnumValues.FAIL;
    this.unsignedAsSigned = false;
    this.timestampUnit = TimeUnit.MICROSECOND;
  }

  /**
   * Sets the maximum number of messages in each batch, or {@link
   * ProtobufToArrowVectorIterator#NO_LIMIT_BATCH_SIZE} to convert all messages into one batch. The
   * default is {@link ProtobufToArrowVectorIterator#DEFAULT_BATCH_SIZE}.
   *
   * @param targetBatchSize the maximum number of messages in each batch
   * @return this builder
   */
  public ProtobufToArrowConfigBuilder setTargetBatchSize(int targetBatchSize) {
    this.targetBatchSize = targetBatchSize;
    return this;
  }

  /**
   * Sets the provider that the dictionaries of enum fields are added to. The caller owns the
   * provider, and must close it after the batches that use its dictionaries. A dictionary already
   * in the provider is reused if it was created for the same enum type and holds the same values;
   * other dictionaries are added with ids that the provider does not use yet, so a provider can be
   * shared by conversions of different message types.
   *
   * <p>The default is null, in which case each iterator creates its own provider, see {@link
   * ProtobufToArrowVectorIterator#getDictionaryProvider()}.
   *
   * @param provider the provider, or null
   * @return this builder
   */
  public ProtobufToArrowConfigBuilder setProvider(
      DictionaryProvider.MapDictionaryProvider provider) {
    this.provider = provider;
    return this;
  }

  /**
   * Sets the Arrow representation of enum fields. The default is {@link
   * ProtobufToArrowConfig.EnumMapping#DICTIONARY}.
   *
   * @param enumMapping the Arrow representation of enum fields
   * @return this builder
   */
  public ProtobufToArrowConfigBuilder setEnumMapping(
      ProtobufToArrowConfig.EnumMapping enumMapping) {
    this.enumMapping = enumMapping;
    return this;
  }

  /**
   * Sets the conversion of values of open enums that are not defined in the enum type. The default
   * is {@link ProtobufToArrowConfig.UnknownEnumValues#FAIL}.
   *
   * @param unknownEnumValues the conversion of unknown enum values
   * @return this builder
   */
  public ProtobufToArrowConfigBuilder setUnknownEnumValues(
      ProtobufToArrowConfig.UnknownEnumValues unknownEnumValues) {
    this.unknownEnumValues = unknownEnumValues;
    return this;
  }

  /**
   * Sets whether unsigned integer fields are mapped to signed Arrow types, for consumers that do
   * not support unsigned integers. If true, uint32 and fixed32 are widened to Int64, and uint64 and
   * fixed64 become Int64 with the same bits as the Java {@code long} value, so values of 2^63 and
   * above are negative. The default is false, which maps them to UInt32 and UInt64.
   *
   * @param unsignedAsSigned whether to map unsigned integers to signed types
   * @return this builder
   */
  public ProtobufToArrowConfigBuilder setUnsignedAsSigned(boolean unsignedAsSigned) {
    this.unsignedAsSigned = unsignedAsSigned;
    return this;
  }

  /**
   * Sets the unit of the UTC timestamps that google.protobuf.Timestamp fields are mapped to. The
   * default is {@link TimeUnit#MICROSECOND}, which covers the full range of protobuf timestamps.
   * {@link TimeUnit#NANOSECOND} keeps full precision, but only covers the years 1677 to 2262.
   *
   * @param timestampUnit the unit of the timestamps
   * @return this builder
   */
  public ProtobufToArrowConfigBuilder setTimestampUnit(TimeUnit timestampUnit) {
    this.timestampUnit = timestampUnit;
    return this;
  }

  /**
   * Builds the {@link ProtobufToArrowConfig} from the provided params.
   *
   * @return the config
   */
  public ProtobufToArrowConfig build() {
    return new ProtobufToArrowConfig(
        allocator,
        targetBatchSize,
        provider,
        enumMapping,
        unknownEnumValues,
        unsignedAsSigned,
        timestampUnit);
  }
}
