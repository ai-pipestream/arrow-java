.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

======================
Arrow Protobuf Adapter
======================

The Arrow Protobuf Adapter assists with working with Protocol Buffers
and Arrow data. It works from message descriptors, so descriptors of
generated message classes and descriptors loaded at runtime are both
supported. Currently, it supports converting message descriptors to
Arrow schemas, and messages to Arrow VectorSchemaRoots.

The adapter is experimental. The well-known types
``google.protobuf.Duration`` and ``Any``, and wrapper types such as
``google.protobuf.Int32Value``, are converted to structs like other
messages. Later versions may map them to dedicated Arrow types instead,
which would change the schema of their fields.

Descriptor to Schema Conversion
===============================

This can be accessed via the ProtobufToArrow class:

.. code-block:: java

   ProtobufToArrowConfig config = new ProtobufToArrowConfigBuilder().build();
   Schema schema = ProtobufToArrow.protobufToArrowSchema(MyMessage.getDescriptor(), config);

Each field of the message becomes a field of the schema, in the same
order.

Type Mapping
------------

+---------------------------+------------------------+-------+
| Protobuf Type             | Arrow Type             | Notes |
+===========================+========================+=======+
| bool                      | Bool                   |       |
+---------------------------+------------------------+-------+
| int32, sint32, sfixed32   | Int32                  |       |
+---------------------------+------------------------+-------+
| uint32, fixed32           | UInt32                 | \(1)  |
+---------------------------+------------------------+-------+
| int64, sint64, sfixed64   | Int64                  |       |
+---------------------------+------------------------+-------+
| uint64, fixed64           | UInt64                 | \(1)  |
+---------------------------+------------------------+-------+
| float                     | Float32                |       |
+---------------------------+------------------------+-------+
| double                    | Float64                |       |
+---------------------------+------------------------+-------+
| string                    | Utf8                   |       |
+---------------------------+------------------------+-------+
| bytes                     | Binary                 |       |
+---------------------------+------------------------+-------+
| message, group            | Struct                 | \(2)  |
+---------------------------+------------------------+-------+
| repeated                  | List                   |       |
+---------------------------+------------------------+-------+
| map                       | Map                    |       |
+---------------------------+------------------------+-------+

* \(1) If ``setUnsignedAsSigned(true)`` is used, these map to Int64
  instead, for consumers that do not support unsigned integers. 32-bit
  values are widened, while 64-bit values keep their bits, so values of
  2^63 and above are negative.
* \(2) Recursive message types cannot be converted, and neither can
  the well-known types google.protobuf.Timestamp, Struct, Value and
  ListValue.

A singular field is nullable if it tracks presence, as reported by
``FieldDescriptor.hasPresence()``: message fields, oneof members, proto2
fields, proto3 fields declared ``optional``, and fields with explicit
presence in editions. Other fields are not nullable, since protobuf does
not distinguish them being unset from having their default value.
Lists, list elements, maps, map keys and map values are never null.

Message types are expanded wherever they are used, so the size of the
schema depends on the number of paths through the message types rather
than on the number of types. If each message type has two fields of the
next type, the schema doubles in size at each level, so descriptors from
untrusted sources should be checked before they are converted.

Message to VectorSchemaRoot Conversion
======================================

This can be accessed via the ProtobufToArrow class. The resulting
ProtobufToArrowVectorIterator converts messages to Arrow data in
batches, using the schema described above. Each batch is a new
VectorSchemaRoot that the caller must close.

.. code-block:: java

   ProtobufToArrowConfig config = new ProtobufToArrowConfigBuilder(allocator)
       .setTargetBatchSize(4096)
       .build();
   try (ProtobufToArrowVectorIterator it =
       ProtobufToArrow.protobufToArrowIterator(MyMessage.getDescriptor(), messages, config)) {
     while (it.hasNext()) {
       try (VectorSchemaRoot root = it.next()) {
         // Consume the root…
       }
     }
   }

Fields that track presence are null when they are not set, even if
they have a default value. Map fields keep the last entry of each key,
as generated messages do, even if a ``DynamicMessage`` holds a key more
than once after parsing. Strings are decoded by protobuf-java, which
replaces bytes that are not valid UTF-8 with U+FFFD. Protobuf does not
validate UTF-8 in proto2 strings when parsing, so invalid bytes in such
strings are lost.

The messages can be generated messages or ``DynamicMessage`` instances,
but their descriptor must be the same ``Descriptor`` instance as the
one passed to ``protobufToArrowIterator``. A descriptor built
separately for the same type, for example from a ``FileDescriptorSet``,
is rejected.

If a batch fails to convert, the messages that were read for it are
lost, and the iterator throws ``IllegalStateException`` from then on.
