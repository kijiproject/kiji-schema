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
 * Representation of an Avro name.
 *
 * An Avro name is normally qualified by the namespace it belongs to.
 * For example, "name.space.SimpleName" is the name of an Avro element whose simple name is
 * "SimpleName" in the namespace "name.space".
 *
 * A namespace is always absolute and never relative: "name.space" is equivalent to ".name.space".
 * Simple names may sometimes be used relative to the namespace defined in some context. To limit
 * confusion, top-level Avro names must begin with a period, as in: ".TopLevelSimpleName".
 *
 * @param simpleName Simple name (eg. the 'X' in 'name.space.X').
 * @param namespace Namespace.
 */
class AvroName(
    /** Simple name of this Avro name. */
    val simpleName: String,

    /** Namespace, potentially empty (top-level name). */
    val namespace: String
) {

  /**
   * Fully-qualified name representation.
   */
  val fullName: String = { namespace + "." + simpleName }
}

/**
 * Companion object for AvroName.
 */
object AvroName {
  /**
   * Parses an Avro name into an AvroName object.
   *
   * @param name Avro name to parse.
   * @return the parsed AvroName object.
   */
  def fromFullName(name: String): AvroName = {
    var splits = name.split('.')
    if (splits.head.isEmpty) {
      splits = splits.drop(1)
    }
    val simpleName = splits.last
    splits = splits.dropRight(1)
    val namespace = splits.mkString(".")
    return new AvroName(simpleName=simpleName, namespace=namespace)
  }
}