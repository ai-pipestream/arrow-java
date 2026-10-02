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
import java.util.NoSuchElementException;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ValueVectorUtility;

/**
 * VectorSchemaRoot iterator for converting protobuf messages in batches. Each batch is a new root,
 * which the caller is responsible for closing.
 *
 * <p>If converting a batch fails, the messages read for that batch are lost, and every later call
 * to {@link #hasNext()} or {@link #next()} throws an {@link IllegalStateException}.
 */
public final class ProtobufToArrowVectorIterator
    implements Iterator<VectorSchemaRoot>, AutoCloseable {

  /** The batch size that converts all messages into a single batch. */
  public static final int NO_LIMIT_BATCH_SIZE = -1;

  /** The default batch size. */
  public static final int DEFAULT_BATCH_SIZE = 1024;

  private final Descriptor descriptor;

  private final Iterator<? extends Message> messages;

  private final ProtobufToArrowConfig config;

  private final Schema schema;

  private Throwable failure;

  private boolean closed;

  private ProtobufToArrowVectorIterator(
      Descriptor descriptor,
      Iterator<? extends Message> messages,
      ProtobufToArrowConfig config,
      Schema schema) {
    this.descriptor = descriptor;
    this.messages = messages;
    this.config = config;
    this.schema = schema;
  }

  static ProtobufToArrowVectorIterator create(
      Descriptor descriptor, Iterator<? extends Message> messages, ProtobufToArrowConfig config) {
    Preconditions.checkNotNull(config.getAllocator(), "allocator cannot be null");

    Schema schema = new SchemaConverter(config).convert(descriptor);
    return new ProtobufToArrowVectorIterator(descriptor, messages, config, schema);
  }

  @Override
  public boolean hasNext() {
    Preconditions.checkState(!closed, "The iterator is closed");
    if (failure != null) {
      throw new IllegalStateException("A previous batch failed to convert", failure);
    }
    return messages.hasNext();
  }

  /**
   * Gets the next batch of messages. The user is responsible for freeing its resources.
   *
   * @return the next batch
   */
  @Override
  public VectorSchemaRoot next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    try {
      return load();
    } catch (Throwable t) {
      failure = t;
      throw t;
    }
  }

  private VectorSchemaRoot load() {
    int targetBatchSize = config.getTargetBatchSize();
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, config.getAllocator());
    try {
      if (targetBatchSize != NO_LIMIT_BATCH_SIZE) {
        ValueVectorUtility.preAllocate(root, targetBatchSize);
      }
      MessageConsumer consumer = MessageConsumer.create(descriptor, root, config);
      int rowCount = 0;
      while (messages.hasNext()
          && (targetBatchSize == NO_LIMIT_BATCH_SIZE || rowCount < targetBatchSize)) {
        Message message = messages.next();
        checkDescriptor(message.getDescriptorForType());
        consumer.consume(rowCount++, message);
      }
      root.setRowCount(rowCount);
      return root;
    } catch (Throwable t) {
      root.close();
      throw t;
    }
  }

  private void checkDescriptor(Descriptor actual) {
    if (actual == descriptor) {
      return;
    }
    if (actual.getFullName().equals(descriptor.getFullName())) {
      throw new IllegalArgumentException(
          "Message of type "
              + actual.getFullName()
              + " has a different descriptor instance than the iterator. Messages must use the"
              + " descriptor that the iterator was created with.");
    }
    throw new IllegalArgumentException(
        "Expected a message of type "
            + descriptor.getFullName()
            + ", but got "
            + actual.getFullName());
  }

  /** Closes the iterator. Batches that it returned are not affected. */
  @Override
  public void close() {
    closed = true;
  }
}
