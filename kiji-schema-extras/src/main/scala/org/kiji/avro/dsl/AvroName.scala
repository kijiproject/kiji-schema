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
 * @param name Simple name (eg. the 'X' in 'name.space.X').
 * @param ns Optional namespace, as a list of components
 *     (eg. the list ['name', 'space'] in 'name.space.X').
 */
class AvroName(
    /** Simple name of this Avro name. */
    val name: String,
    ns: Option[List[String]]
) {
  /**
   * Fully-qualified name representation.
   */
  val fullName: String = {
    ns match {
      case None => "." + name
      case Some(path) => path.mkString(".") + "." + name
    }
  }

  /** Namespace this Avro name belongs to. Potentially null. */
  val nameSpace: String = {
    ns match {
      case None => null
      case Some(path) => path.mkString(".")
    }
  }
}
