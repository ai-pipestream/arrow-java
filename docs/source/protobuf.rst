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
Arrow schemas, and messages to Arrow VectorSchemaRoots and back.

The adapter is experimental. The well-known types
``google.protobuf.Duration`` and ``Any``, and wrapper types such as
``google.protobuf.Int32Value``, are converted to structs like other
messages. Later versions may map them to dedicated Arrow types instead,
which would change the schema of their fields.

The adapter requires protobuf-java 3.25 or later.

Descriptor to Schema Conversion
===============================

This can be accessed via the ProtobufToArrow class:

.. code-block:: java

   ProtobufToArrowConfig config = new ProtobufToArrowConfigBuilder().build();
   Schema schema = ProtobufToArrow.protobufToArrowSchema(MyMessage.getDescriptor(), config);

Each field of the message becomes a field of the schema, in the same
order. The mapping of some types can be customized:

.. code-block:: java

   ProtobufToArrowConfig config = new ProtobufToArrowConfigBuilder()
       .setEnumMapping(ProtobufToArrowConfig.EnumMapping.NAME)
       .setUnsignedAsSigned(true)
       .setTimestampUnit(TimeUnit.NANOSECOND)
       .build();

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
| enum                      | Dictionary<Int32,Utf8> | \(2)  |
+---------------------------+------------------------+-------+
| message, group            | Struct                 | \(3)  |
+---------------------------+------------------------+-------+
| repeated                  | List                   |       |
+---------------------------+------------------------+-------+
| map                       | Map                    |       |
+---------------------------+------------------------+-------+
| google.protobuf.Timestamp | Timestamp[us, UTC]     | \(4)  |
+---------------------------+------------------------+-------+
| google.protobuf.Struct    | Utf8                   | \(5)  |
+---------------------------+------------------------+-------+
| google.protobuf.Value     | Utf8                   | \(5)  |
+---------------------------+------------------------+-------+
| google.protobuf.ListValue | Utf8                   | \(5)  |
+---------------------------+------------------------+-------+

* \(1) If ``setUnsignedAsSigned(true)`` is used, these map to Int64
  instead, for consumers that do not support unsigned integers. 32-bit
  values are widened, while 64-bit values keep their bits, so values of
  2^63 and above are negative.
* \(2) The indices refer to a dictionary of the enum value names, in
  declaration order. Fields with the same enum type share a dictionary.
  ``EnumMapping.NAME`` maps enums to the value name as Utf8 instead, and
  ``EnumMapping.NUMBER`` to the value number as Int32.
* \(3) Recursive message types cannot be converted, except for the
  well-known types below.
* \(4) The unit can be changed with ``setTimestampUnit``. Microseconds
  cover the full range of protobuf timestamps, nanoseconds only cover
  the years 1677 to 2262. Values are rounded down to the unit.
* \(5) These recursive types are represented as their proto3 JSON, as
  printed by ``JsonFormat`` from protobuf-java-util without whitespace.
  ``JsonFormat`` escapes the characters ``<``, ``>``, ``&``, ``=`` and
  ``'`` in strings, for example as ``\u003c``. Values nested more than
  100 levels deep, and numbers that are NaN or infinite, cannot be
  converted.

A singular field is nullable if it tracks presence, as reported by
``FieldDescriptor.hasPresence()``: message fields, oneof members, proto2
fields, proto3 fields declared ``optional``, and fields with explicit
presence in editions. Other fields are not nullable, since protobuf does
not distinguish them being unset from having their default value.
Lists, list elements, maps, map keys and map values are never null,
except for unknown enum values as described below.

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

Enums
-----

With ``EnumMapping.DICTIONARY``, the default, the iterator creates the
dictionaries of the enum fields when it is created. They are available
from ``ProtobufToArrowVectorIterator.getDictionaryProvider()``, for
example to write the batches with an ``ArrowStreamWriter``. By default,
the iterator creates its own dictionary provider and closes it when the
iterator is closed, so the iterator must stay open while the
dictionaries are used.

A dictionary provider can instead be set with ``setProvider``. The
caller then owns it and must close it. Dictionaries that are already in
the provider are reused when they hold the values of the same enum
type, and new dictionaries get ids that the provider does not use yet,
so a provider can be shared by the conversions of several message
types. In that case, the dictionary ids in the schema that
``protobufToArrowSchema`` returns can change when an iterator for
another message type adds dictionaries, so use the schema of the
batches instead. A config must not be used by several threads at once,
since creating an iterator can add dictionaries to its provider.

Open enums, such as proto3 enums, can hold values that are not defined
in the enum, for example when a message was written with a newer
version of the enum. By default, converting such a value fails.
``EnumMapping.NUMBER`` keeps the number, and
``setUnknownEnumValues(UnknownEnumValues.NULL)`` converts it to null
with the other mappings, which makes the fields of open enum types
nullable, including list elements and map values.

VectorSchemaRoot to Message Conversion
======================================

This can be accessed via the ArrowToProtobufUtils class. The resulting
CompositeProtobufProducer sets the fields of a message builder from a
row of the vectors, which are matched to the fields by name. Both
generated builders and ``DynamicMessage.Builder`` are supported.

.. code-block:: java

   CompositeProtobufProducer producer = ArrowToProtobufUtils.createCompositeProducer(
       root.getFieldVectors(), MyMessage.getDescriptor(), iterator.getDictionaryProvider());
   for (int row = 0; row < root.getRowCount(); row++) {
     MyMessage.Builder builder = MyMessage.newBuilder();
     producer.produce(row, builder);
     MyMessage message = builder.build();
   }

The vectors can have the types that the fields are mapped to, with any
of the options above. Strings and enum names can also be LargeUtf8 or
Utf8View, and bytes LargeBinary or BinaryView. Timestamps must have a
time zone. Strings are decoded as UTF-8, and invalid UTF-8, which Arrow
does not allow, is replaced with U+FFFD rather than rejected. The JSON
of ``google.protobuf.Struct``, ``Value`` and ``ListValue`` fields must
be a single JSON value as defined by RFC 8259, without duplicate keys,
unpaired surrogates or numbers out of the range of doubles, and nested
at most 100 levels deep.

Null values clear the field, and a null list or map becomes an empty
one. Fields without a vector are left unchanged, so a builder that is
reused must be cleared first unless the vectors cover every field.
Lists cannot have null elements, maps cannot have null entries, keys or
values, and at most one field of a oneof can be non-null. A map that
holds a key more than once keeps the last value, as protobuf parsing
does.

Some information does not survive a conversion to Arrow and back:
timestamps are rounded down to the configured unit, a
``google.protobuf.Value`` without a kind becomes a JSON null, invalid
UTF-8 in proto2 strings becomes U+FFFD, and extensions and unknown
fields are not converted. Unknown enum values
are only kept by ``EnumMapping.NUMBER``. With
``UnknownEnumValues.NULL``, they become null, so converting them back
clears singular fields, which then have their default value, and fails
for list elements and map values.
