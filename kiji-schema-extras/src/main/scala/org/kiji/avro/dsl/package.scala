/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.avro.dsl

/**
 * This package contains a DSL for Avro schemas and Avro values.
 * The DSL is mainly intended for integration with CLI tools and the KijiSchema DDL.
 *
 * The DSL is freely inspired from the IDL for Avro protocols, with several modifications:
 * <ul>
 *   <li> Recursive records are supported. </li>
 *   <li> There is currently no support for custom properties (eg. aliases). </li>
 *   <li> There is no default namespace, ie. Avro names must be fully-qualified everywhere. </li>
 *   <li> Separators between/after fields, enum symbols, schema declarations, etc are optional.
 *        Valid separators are "," and ";". </li>
 * </ul>
 *
 * Any primitive schema is declared by with its short name:
 *     null, boolean, int, long, float, double, string, bytes.
 *
 * Arrays and maps are declared as follows:
 * {{{
 *   array-decl ::= array<schema>
 *   map-decl ::= map<schema>
 * }}}
 *
 * Enums are declared as follows:
 * {{{
 *   enum name.space.EnumName { ( SymbolName (,;)? )* }
 * }}}
 *
 * Fixed byte arrays are declared as follows:
 * {{{
 *   fixed name.space.Fixed(nbytes)
 * }}}
 * For example, an MD5 sum can be declared as: {{{ fixed hash.MD5(16) }}}
 *
 * A record schema is declared as follows:
 * {{{
 *   record-decl ::= record name.space.RecordName { field-decl* }
 *   field-decl  ::= schema field_name [ = default-value ] (,;)?
 * }}}
 *
 * The DSL supports recursive records. For instance, a list can be declared as follows:
 * {{{
 *   record ns.List {
 *     int head
 *     union { null, ns.List } tail = null
 *   }
 * }}}
 *
 * Records may also be pre-declared. This is useful for mutually recursive structures:
 * {{{
 *   record ns.Edge
 *
 *   record ns.Node {
 *     string name
 *     array<ns.Edge> edges
 *   }
 *
 *   record ns.Edge {
 *     string label
 *     double weight
 *     union { null, Node } target = null
 *   }
 * }}}
 *
 * The DSL also supports Avro values, to allow specifying default values on a record field.
 * Primitive values are specified as in most languages like C or Java.
 * Long, float and doubles may take an optional qualifier, as in:
 * <ul>
 *   <li> "d" suffix for a double: "3.14159d", </li>
 *   <li> "l" suffix for a long: "1L", </li>
 *   <li> "f" suffix for a float: "3.14159f", </li>
 * </ul>
 * Strings support single, double and triple quotes as in Python or Scala.
 * Array values are declared as follows:
 *     {{{ [ ( value (,;)? )* ] }}}
 * For example, an array of integer can be written:
 * <ul>
 *   <li> {{{ [3 1 4 1 5 9] }}} </li>
 *   <li> {{{ [3, 1, 4, 1, 5, 9] }}} </li>
 *   <li> {{{ [3, 1, 4, 1, 5, 9,] }}} </li>
 *   <li> {{{ [3; 1; 4; 1; 5; 9] }}} </li>
 *   <li> {{{ [3; 1; 4; 1; 5; 9;] }}} </li>
 * </ul>
 *
 * Maps follow a similar pattern:
 *     {{{ { ( key:value (,;)? )* } }}}
 * For example, a map of integers can be written:
 * <ul>
 *   <li> {{{ {"key1": 1 "key2": 2}}} </li>
 *   <li> {{{ {"key1": 1, "key2": 2}}} </li>
 *   <li> {{{ {"key1": 1, "key2": 2,}}} </li>
 *   <li> {{{ {"key1": 1; "key2": 2}}} </li>
 *   <li> {{{ {"key1": 1; "key2": 2;}}} </li>
 * </ul>
 *
 * Arbitrary byte arrays are specified as an array of hexadecimal bytes, as follows:
 * <ul>
 *   <li> {{{ bytes(0301040109) }}} </li>
 *   <li> {{{ bytes(03,01,04,01,09) }}} </li>
 *   <li> {{{ bytes(03,01,04,01,09,) }}} </li>
 *   <li> {{{ bytes(03;01;04;01;09) }}} </li>
 * </ul>
 *
 * Fixed and Enum must specify their name, as in:
 * as in: {{{ ns.EnumName(Symbol) }}}
 * and: {{{ hash.MD5(deadfeeddeadfeeddeadfeeddeadfeed) }}}
 *
 * Record values must specify their name, as follows:
 *     {{{ ns.List { head=1, tail=ns.List { head=2 } } }}}
 */
