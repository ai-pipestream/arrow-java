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

/** This class builds {@link ProtobufToArrowConfig}s. */
public final class ProtobufToArrowConfigBuilder {

  private final BufferAllocator allocator;

  private int targetBatchSize;

  private boolean unsignedAsSigned;

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
    this.unsignedAsSigned = false;
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
   * Builds the {@link ProtobufToArrowConfig} from the provided params.
   *
   * @return the config
   */
  public ProtobufToArrowConfig build() {
    return new ProtobufToArrowConfig(allocator, targetBatchSize, unsignedAsSigned);
  }
}
