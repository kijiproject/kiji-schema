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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.List;
import java.util.regex.Pattern;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;
import org.kiji.common.flags.Flag;
import org.kiji.common.flags.FlagParser;
import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.KijiNotInstalledException;
import org.kiji.schema.KijiURI;
import org.kiji.schema.KijiURIException;

/**
 * Base class for all command line tools. A kiji command line tool operates on a kiji instance,
 * specified through a {@link KijiURI} via the command-line (with a default value pointing to the
 * default kiji instance found in the hbase cluster specified by whatever hbase configuration is
 * on the classpath). If the kiji URI for the instance specifies more elements than the zookeeper
 * connection information and instance name they will be ignored.
 *
 * A command line tool, executed via {@link KijiToolLauncher}, will perform these steps when run:
 * <ol>
 *   <li>Parse command-line flags to the tool and set the appropriate fields. Subclasses
 *   wishing to add flags to a tool should use the {@link Flag} annotation.</li>
 *   <li>Use the kiji URI specified via the command line (or using the default
 *   <code>kiji://.env/default</code> to initialize the instance's configuration with
 *   hbase connection parameters and hbase resources.</li>
 *   <li>Run the {@link #validateFlags()} method. Subclasses wishing to validate custom
 *   command-line arguments should override this method
 *   but take care to call <code>super.validateFlags()</code></li>
 *   <li>Run the {@link #setup()} method. Subclasses wishing to implement custom setup logic
 *   should override this method but take care to call <code>super.setup()</code></li>
 *   <li>Run the {@link #run(java.util.List)} method. Subclasses should implement their main
 *   command logic here. The argument to <code>run</code> is a {@link String} list of
 *   arguments passed that were not arguments to the Hadoop framework or flags specified via
 *   {@link Flag} annotations.</li>
 *   <li>Run the {@link #cleanup()} method. Subclasses wishing to implement custom cleanup
 *   logic should override this method but take care to call <code>super.cleanup</code>.
 *   <code>cleanup</code> will run even if there is an exception while executing <code>setup</code>
 *   or <code>run</code>.
 * </ol>
 *
 * Tools needing to prompt the user for a yes/no answer should use the {@link #yesNoPrompt} method.
 */
@ApiAudience.Framework
@Inheritance.Extensible
public abstract class BaseTool extends Configured implements KijiTool {
  /** Used when prompting the user for feedback. */
  private static final Pattern YES_PATTERN = Pattern.compile("y|yes", Pattern.CASE_INSENSITIVE);
  private static final Pattern NO_PATTERN = Pattern.compile("n|no", Pattern.CASE_INSENSITIVE);

  @Flag(name="kiji", usage="A kiji URI identifying the kiji instance to use.")
  private String mInstanceURIStr = String.format("kiji://.env/%s",
      KijiConfiguration.DEFAULT_INSTANCE_NAME);

  @Flag(name="debug", usage="Print stacktraces if the command terminates with an error.")
  private boolean mDebugFlag = false;

  @Flag(name="interactive", usage="Whether the command is run in an interactive session or script."
      + "The default value is true.")
  private boolean mInteractiveFlag = true;

  /**
   * A URI used to track what element in kiji a tool is operating on.
   */
  private KijiURI mURI;

  /**
   * The print stream to write to.
   */
  private PrintStream mPrintStream = System.out;

  /**
   * Prompts the user for a yes or no answer to the specified question until they provide a valid
   * response (y/n/yes/no case insensitive) and reports the result. If yesNoPrompt is called in
   * non-interactive mode, an IllegalStateException is thrown.
   *
   * @param question The question to which a yes or no is expected in response.
   * @return <code>true</code> if the user answer yes, <code>false</code> if  the user answered no.
   * @throws IOException if there is a problem reading from the terminal.
   */
  protected final boolean yesNoPrompt(String question) throws IOException {
    Preconditions.checkState(mInteractiveFlag);
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
    Boolean yesOrNo = null;
    try {
      while (yesOrNo == null) {
        getPrintStream().println(question);
        getPrintStream().println("Please answer yes or no.");
        String response = reader.readLine();
        if (null == response) {
          throw new RuntimeException("Reached end of stream when reading yes or no response from "
              + "console!");
        }
        response = response.trim();
        if (YES_PATTERN.matcher(response).matches()) {
          yesOrNo = true;
        } else if (NO_PATTERN.matcher(response).matches()) {
          yesOrNo = false;
        }
      }
      return yesOrNo;
    } finally {
      reader.close();
    }
  }

  /**
   * Invoke the functionality of this tool, as supplied through its
   * implementation of the abstract methods of this class.
   *
   * @param args the command-line arguments to the tool not including the
   *     tool name itself.
   * @throws Exception if there's an error inside the tool.
   * @return 0 on success, non-zero on failure.
   */
  @Override
  public int toolMain(List<String> args) throws Exception {
    try {
      List<String> nonFlagArgs = FlagParser.init(this, args.toArray(new String[args.size()]));
      if (null == nonFlagArgs) {
        // There was a problem parsing the flags.
        return 1;
      }

      // Create a kiji URI from the string specified by the user, ignoring everything except
      // zookeeper connection information and the instance name.
      // Then use the URI to retrieve connection settings for the zookeeper quorum, which should be
      // enough to talk to an HBase instance.
      try {
        setURI(KijiURI.newBuilder(mInstanceURIStr).build());
      } catch (KijiURIException kue) {
        throw new IllegalArgumentException(
            String.format("Invalid kiji URI '--kiji=%s'.", mInstanceURIStr), kue);
      }
      getConf().setInt(HConstants.ZOOKEEPER_CLIENT_PORT, mURI.getZookeeperClientPort());
      getConf().set(HConstants.ZOOKEEPER_QUORUM,
          Joiner.on(",").join(mURI.getZookeeperQuorumOrdered()));
      setConf(HBaseConfiguration.addHbaseResources(getConf()));

      // Execute custom functionality implemented in subclasses.
      validateFlags();
      try {
        setup();
        return run(nonFlagArgs);
      } finally {
        cleanup();
      }
    } catch (KijiNotInstalledException knie) {
      getPrintStream().println(knie.getMessage());
      getPrintStream().println("Try: kiji install --kiji=kiji://.env/" + knie.getInstanceName());
      return 2;
    } catch (Exception exn) {
      if (mDebugFlag) {
        throw exn; // Debug mode enabled; throw error back to the user.
      } else {
        // Just pretty-print the error for the user.
        Throwable thr = exn;
        while (thr != null) {
          getPrintStream().println("Error: " + thr.getMessage());
          thr = thr.getCause();
        }
        return 3;
      }
    }
  }

  /**
   * Validates the command-line flags.
   *
   * @throws Exception If there is an invalid flag.
   */
  protected void validateFlags() throws Exception {}

  /**
   * Called to initialize the tool just before running.
   *
   * @throws Exception If there is an error.
   */
  protected void setup() throws Exception {}

  /**
   * Cleans up any open file handles, connections, etc.
   *
   * @throws IOException If there is an error.
   */
  protected void cleanup() throws IOException {}

  /**
   * Runs the tool.
   *
   * @param nonFlagArgs The arguments on the command-line that were not parsed as flags.
   * @return The program exit code.
   * @throws Exception If there is an error.
   */
  protected abstract int run(List<String> nonFlagArgs) throws Exception;

  /**
   * Returns the kiji URI of the target this tool operates on.
   *
   * @return The kiji URI of the target this tool operates on.
   */
  protected KijiURI getURI() {
    return mURI;
  }

  /**
   * Sets the kiji URI of the target this tool operates on.
   *
   * @param uri The kiji URI of the target this tool should operate on.
   */
  protected void setURI(KijiURI uri) {
    mURI = uri;
  }

  /**
   * Set the output print stream the tool should write to.  If you don't set it,
   * it will default to STDOUT.
   *
   * @param printStream The output print stream to use.
   */
  public void setPrintStream(PrintStream printStream) {
    mPrintStream = printStream;
  }

  /**
   * The output print stream the tool should be writing to.
   *
   * @return The print stream the tool should write to.
   */
  public PrintStream getPrintStream() {
    return mPrintStream;
  }

  /**
   * Whether or not this tool is being run interactively.
   *
   * @return Whether or not this tool is being run interactively.
   */
  protected final boolean isInteractive() {
    return mInteractiveFlag;
  }
}
