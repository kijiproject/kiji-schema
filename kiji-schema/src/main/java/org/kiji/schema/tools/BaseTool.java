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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configured;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;
import org.kiji.common.flags.Flag;
import org.kiji.common.flags.FlagParser;
import org.kiji.schema.KijiNotInstalledException;

/**
 * Base class for all Kiji command line tools.
 *
 * A command line tool, executed via {@link KijiToolLauncher}, will perform these steps when run:
 * <ol>
 *   <li> Parse command-line flags to the tool and set the appropriate fields. Subclasses
 *     wishing to add flags to a tool should use the {@link Flag} annotation.</li>
 *   <li> Run the {@link #validateFlags()} method. Subclasses wishing to validate custom
 *     command-line arguments should override this method but take care to call
 *     <code>super.validateFlags()</code></li>
 *   <li> Run the {@link #setup()} method. Subclasses wishing to implement custom setup logic
 *     should override this method but take care to call <code>super.setup()</code></li>
 *   <li> Run the {@link #run(java.util.List)} method. Subclasses should implement their main
 *     command logic here. The argument to <code>run</code> is a {@link String} list of
 *     arguments passed that were not arguments to the Hadoop framework or flags specified via
 *     {@link Flag} annotations.</li>
 *   <li> Run the {@link #cleanup()} method. Subclasses wishing to implement custom cleanup
 *     logic should override this method but take care to call <code>super.cleanup</code>.
 *     <code>cleanup</code> will run even if there is an exception while executing
 *     <code>setup</code> or <code>run</code>.
 * </ol>
 *
 * Tools needing to prompt the user for a yes/no answer should use the {@link #yesNoPrompt} method.
 */
@ApiAudience.Framework
@Inheritance.Extensible
public abstract class BaseTool extends Configured implements KijiTool {
  private static final Logger LOG = LoggerFactory.getLogger(BaseTool.class);
  /** Used when prompting the user for feedback. */
  private static final Pattern YES_PATTERN = Pattern.compile("y|yes", Pattern.CASE_INSENSITIVE);
  private static final Pattern NO_PATTERN = Pattern.compile("n|no", Pattern.CASE_INSENSITIVE);

  @Flag(name="debug", usage="Enables more verbose error messages.")
  private boolean mDebugFlag = false;

  @Flag(name="interactive", usage="Indicates whether the command may prompt the user for input.\n"
      + "\tSet --interactive=false to run the kiji command as part of a non-interactive script.")
  private boolean mInteractiveFlag = true;

  @Flag(name="help", usage="Print the usage message.")
  private boolean mHelp = false;

  /** The print stream to write to. */
  private PrintStream mPrintStream;

  /** Success tool exit code. */
  public static final int SUCCESS = 0;

  /** Failure tool exit code. */
  public static final int FAILURE = 1;

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
   * Checks with the user whether the specified operation may proceed.
   *
   * @param format String format with a question describing the operation about to be executed.
   * @param arguments String format arguments.
   * @return whether the operation may proceed, or not.
   * @throws IOException on I/O error.
   */
  protected boolean mayProceed(String format, Object...arguments) throws IOException {
    if (!isInteractive()) {
      return true;
    }
    if (yesNoPrompt(String.format(format, arguments))) {
      return true;
    }
    getPrintStream().println("Aborted.");
    return false;
  }

  /**
   * Invoke the functionality of this tool, as supplied through its
   * implementation of the abstract methods of this class.
   *
   * @param args the command-line arguments to the tool not including the
   *     tool name itself.
   * @throws Exception if there's an error inside the tool.
   * @return 0 on success, non-zero on failure.
   *
   * {@inheritDoc}
   */
  @Override
  public int toolMain(List<String> args) throws Exception {
    try {
      List<String> nonFlagArgs = FlagParser.init(this, args.toArray(new String[args.size()]));
      if (null == nonFlagArgs) {
        // There was a problem parsing the flags.
        return FAILURE;
      }

      if (mHelp) {
        printUsage();
        return SUCCESS;
      }

      // Execute custom functionality implemented in subclasses.
      validateFlags();
      boolean exceptionThrown = false;
      try {
        setup();
        return run(nonFlagArgs);
      } catch (Exception exn) {
        exceptionThrown = true;
        throw exn;
      } finally {
        if (exceptionThrown) {
          try {
            cleanup();
          } catch (Exception nestedExn) {
            LOG.error("Nested error in tool cleanup(), "
                + "likely caused by error in tool setup() or run(): {}",
                nestedExn.getMessage());
          }
        } else {
          cleanup();
        }
      }
    } catch (KijiNotInstalledException knie) {
      getPrintStream().println(knie.getMessage());
      getPrintStream().println("Try: kiji install --kiji=kiji://.env/" + knie.getInstanceName());
      return FAILURE;
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
   * Note: all subclasses of BaseTool should call super.cleanup()
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
   * The output print stream the tool should be writing to.
   * If no print stream is set, returns System.out
   *
   * @return The print stream the tool should write to.
   */
  public PrintStream getPrintStream() {
    if (null == mPrintStream) {
      mPrintStream = System.out;
    }
    return mPrintStream;
  }

  /**
   * Set the output print stream the tool should write to.  If you don't set it,
   * it will default to STDOUT.
   *
   * @param printStream The output print stream to use.
   */
  public void setPrintStream(PrintStream printStream) {
    if (null == mPrintStream) {
      mPrintStream = printStream;
    } else {
      getPrintStream().println("Printstream is already set.");
    }
  }

  /** Prints the tool usage message. */
  private void printUsage() {
    final PrintStream ps = getPrintStream();
    ps.println(getUsageString());
    ps.println("Flags:");
    FlagParser.printUsage(this, ps);
  }

  /** {@inheritDoc} */
  @Override
  public String getUsageString() {
    return String.format("Usage:%n"
        + "    kiji %s [flags...]%n",
        getName());
  }

  /**
   * Whether or not this tool is being run interactively.
   *
   * @return Whether or not this tool is being run interactively.
   */
  protected final boolean isInteractive() {
    return mInteractiveFlag;
  }

  /**
   * Whether or not this tool is being run with verbose debug messages.
   *
   * @return Whether or not this tool is being run with verbose debug messages.
   */
  protected final boolean hasVerboseDebug() {
    return mDebugFlag;
  }
}
