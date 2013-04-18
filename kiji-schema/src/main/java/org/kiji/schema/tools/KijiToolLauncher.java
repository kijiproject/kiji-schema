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

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.GenericOptionsParser;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.delegation.Lookups;

/**
 * Main entry point to launch Kiji tools.
 *
 * <p>All tools launched through <tt>bin/kiji &lt;tool&gt;</tt> are detected and
 * instantiated through this module. Tools should implement the {@link KijiTool}
 * interface. In addition, each tool must advertise itself by adding a line to
 * a resource file contained in its jar at
 * <tt>META-INF/services/org.kiji.schema.tools.KijiTool</tt>.
 * This path can be added to your build by providing it under <tt>src/main/resources</tt>.</p>
 */
@ApiAudience.Public
@ApiStability.Evolving
public final class KijiToolLauncher extends Configured {

  /**
   * Programmatic entry point to the tool launcher. Locates a tool to run
   * based on the name provided as the first argument, then invokes it.
   * Hadoop property-based arguments will be parsed by KijiToolLauncher.run()
   * in a manner similar to Hadoop's ToolRunner.
   *
   * @param args The command-line arguments. The first one should be the
   *     name of the tool to run.
   * @throws Exception If there is an error.
   * @return 0 on program success, non-zero on error.
   */
  public int run(String[] args) throws Exception {
    if (args.length == 0) {
      System.err.println("Error: Must run 'kiji <toolName>'.");
      System.err.println("Try running 'kiji help' to see the available tools.");
      return 1;
    }

    String toolName = args[0];
    String[] nonToolNameArgs = Arrays.copyOfRange(args, 1, args.length);

    KijiTool tool = getToolForName(toolName);

    if (null == tool) {
      System.err.println("No tool available with name [" + toolName + "]");
      System.err.println("Try running 'kiji help' to see the available tools.");

      if (!System.getenv().containsKey("KIJI_MR_HOME")) {
        System.err.println("\nNote that you do not have the environment variable KIJI_MR_HOME");
        System.err.println("set. Set KIJI_MR_HOME to the path to a kiji-mapreduce distribution");
        System.err.println("to make kiji-mapreduce tools available.");
      }
      return 1;
    }

    // Continue on to Hadoop property argument parsing and tool launch.
    return run(tool, nonToolNameArgs);
  }

  /**
   * Return the tool specified by the 'toolName' argument.
   * (package-protected for use by the HelpTool, and for testing.)
   *
   * @param toolName the name of the tool to instantiate.
   * @return the KijiTool that provides for that name, or null if none does.
   */
  KijiTool getToolForName(String toolName) {
    KijiTool tool = null;

    // Iterate over available tools, searching for the one with
    // the same name as the supplied tool name argument.
    for (KijiTool candidate : Lookups.get(KijiTool.class)) {
      if (toolName.equals(candidate.getName())) {
        tool = candidate;
        break;
      }
    }

    return tool;
  }

  /**
   * Programmatic entry point to the tool launcher if a tool is already selected.
   * Hadoop property-based arguments will be parsed by KijiToolLauncher.run()
   * in a manner similar to Hadoop's ToolRunner.
   *
   * @param tool The KijiTool to run.
   * @param args The command-line arguments, excluding the name of the tool to run.
   * @throws Exception If there is an error.
   * @return 0 on program success, non-zero on error.
   */
  public int run(KijiTool tool, String[] args) throws Exception {
    Configuration conf = getConf();
    if (conf == null) {
      conf = new Configuration();
      setConf(conf);
    }

    // Mimic behavior of Hadoop's ToolRunner.run().
    GenericOptionsParser parser = new GenericOptionsParser(conf, args);
    conf = HBaseConfiguration.addHbaseResources(conf);

    tool.setConf(conf);

    // Get remaining arguments and invoke the tool with them.
    String[] toolArgs = parser.getRemainingArgs();

    // Work around for CDH4 and Hadoop1 setting different "GenericOptionsParser used" flags.
    conf.setBooleanIfUnset("mapred.used.genericoptionsparser", true);
    conf.setBooleanIfUnset("mapreduce.client.genericoptionsparser.used", true);
    return tool.toolMain(Arrays.asList(toolArgs));
  }

  /**
   * Program entry point. This method does not return; it calls System.exit() with the
   * return code from the called tool.
   *
   * @param args The command-line arguments, starting with the name of the tool to run.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new KijiToolLauncher().run(args));
  }
}
