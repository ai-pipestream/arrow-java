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

/** This class configures the Protobuf-to-Arrow conversion process. */
public final class ProtobufToArrowConfig {

  private final BufferAllocator allocator;

  private final int targetBatchSize;

  private final boolean unsignedAsSigned;

  ProtobufToArrowConfig(BufferAllocator allocator, int targetBatchSize, boolean unsignedAsSigned) {
    Preconditions.checkArgument(
        targetBatchSize == ProtobufToArrowVectorIterator.NO_LIMIT_BATCH_SIZE || targetBatchSize > 0,
        "invalid targetBatchSize: %s",
        targetBatchSize);

    this.allocator = allocator;
    this.targetBatchSize = targetBatchSize;
    this.unsignedAsSigned = unsignedAsSigned;
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
   * Returns whether unsigned integer fields are mapped to signed Arrow types.
   *
   * @return whether unsigned integer fields are mapped to signed Arrow types
   */
  public boolean isUnsignedAsSigned() {
    return unsignedAsSigned;
  }
}
