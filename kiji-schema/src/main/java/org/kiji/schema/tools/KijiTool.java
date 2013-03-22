/**
 * (c) Copyright 2012 WibiData, Inc.
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

package org.kiji.schema.tools;

import java.util.List;

import org.apache.hadoop.conf.Configurable;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;

/**
 * Base interface to be implemented by command-line tools that are launched through
 * the <tt>bin/kiji</tt> script. These tools are instantiated and run by the
 * {@link KijiToolLauncher}.
 *
 * <p>To register a tool to use with <tt>bin/kiji</tt>, you must also put the complete
 * name of the implementing class in a resource in your jar file at:
 * <tt>META-INF/services/org.kiji.schema.tools.KijiTool</tt>. You may publish multiple
 * tools in this way; put each class name on its own line in this file. You can put
 * this file in <tt>src/test/resources/</tt> in your Maven project, and it will be
 * incorporated into your jar.</p>
 */
@ApiAudience.Public
@ApiStability.Evolving
@Inheritance.Extensible
public interface KijiTool extends Configurable {
  /**
   * @return the name of the tool. This name is used by users to run the tool.
   *     For example, to provide the <tt>kiji ls</tt> tool, this should return "ls".
   */
  String getName();

  /** @return a short user-friendly description of the tool to print in help text. */
  String getDescription();

  /**
   * @return a short user-friendly category name to which this tool belongs.
   *
   * In the future, help text for tools may be arranged by category.
   */
  String getCategory();

  /**
   * The main logic of your tool.
   *
   * @param args the arguments on the command line (excluding the tool name itself).
   * @return The program exit code. 0 should indicate success.
   * @throws Exception If there is an error.
   */
  int toolMain(List<String> args) throws Exception;

  /** @return the tool usage string. */
  String getUsageString();
}
