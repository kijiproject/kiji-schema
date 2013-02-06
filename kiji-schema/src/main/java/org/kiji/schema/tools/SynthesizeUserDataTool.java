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

import java.io.IOException;
import java.util.List;
import java.util.Random;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.tools.synth.DictionaryLoader;
import org.kiji.schema.tools.synth.EmailSynthesizer;
import org.kiji.schema.tools.synth.NGramSynthesizer;
import org.kiji.schema.tools.synth.WordSynthesizer;
import org.kiji.schema.util.ResourceUtils;

/**
 * Synthesize some user data into a kiji table.
 */
@ApiAudience.Private
public final class SynthesizeUserDataTool extends BaseTool {
  @Flag(name="name-dict", usage="File that contains people names, one per line")
  private String mNameDictionaryFilename = "org/kiji/schema/tools/synth/top_names.txt";

  @Flag(name="num-users", usage="Number of users to synthesize")
  private int mNumUsers = 100;

  @Flag(name="table", usage="kiji table data should be written to")
  private String mTableURIString = "";

  /** Kiji used by the tool. */
  private Kiji mKiji;
  /** KijiURI of the table to store synthesized data. */
  private KijiURI mURI;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "synthesize-user-data";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Synthesize user data into a kiji table.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Example";
  }

  /**
   * Load a list of people names from a file.
   *
   * @param filename The name of the file containing a dictionary of words, one per line.
   * @return The list of words from the file.
   * @throws IOException If there is an error.
   */
  private List<String> loadNameDictionary(String filename)
      throws IOException {
    DictionaryLoader loader = new DictionaryLoader();
    return loader.load(getClass().getClassLoader().getResourceAsStream(filename));
  }

  /** {@inheritDoc} */
  @Override
  protected void validateFlags() throws Exception {
    super.validateFlags();
    if (mTableURIString.isEmpty()) {
      throw new RequiredFlagException("table");
    }
  }

  /**
   * Opens a kiji instance.
   *
   * @return The opened kiji.
   * @throws IOException if there is an error.
   */
  private Kiji openKiji() throws IOException {
    return Kiji.Factory.open(getURI(), getConf());
  }

  /**
   * Retrieves the kiji instance used by this tool. On the first call to this method,
   * the kiji instance will be opened and will remain open until {@link #cleanup()} is called.
   *
   * @return The kiji instance.
   * @throws IOException if there is an error loading the kiji.
   */
  protected synchronized Kiji getKiji() throws IOException {
    if (null == mKiji) {
      mKiji = openKiji();
    }
    return mKiji;
  }

  /**
   * Returns the kiji URI of the target this tool operates on.
   *
   * @return The kiji URI of the target this tool operates on.
   */
  protected KijiURI getURI() {
    if (null == mURI) {
      getPrintStream().println("No URI specified.");
    }
    return mURI;
  }

  /**
   * Sets the kiji URI of the target this tool operates on.
   *
   * @param uri The kiji URI of the target this tool should operate on.
   */

  protected void setURI(KijiURI uri) {
    if (null == mURI) {
      mURI = uri;
    } else {
      getPrintStream().printf("URI is already set to: %s", mURI.toString());
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void setup() {
    setURI(parseURI(mTableURIString));
    getConf().setInt(HConstants.ZOOKEEPER_CLIENT_PORT, mURI.getZookeeperClientPort());
    getConf().set(HConstants.ZOOKEEPER_QUORUM,
        Joiner.on(",").join(mURI.getZookeeperQuorumOrdered()));
    setConf(HBaseConfiguration.addHbaseResources(getConf()));
  }

  /** {@inheritDoc} */
  @Override
  protected void cleanup() {
    ResourceUtils.releaseOrLog(mKiji);
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    // Generate a bunch of user rows with names and email addresses.
    Random random = new Random(System.currentTimeMillis());
    List<String> nameDictionary = loadNameDictionary(mNameDictionaryFilename);
    WordSynthesizer nameSynth = new WordSynthesizer(random, nameDictionary);
    NGramSynthesizer fullNameSynth = new NGramSynthesizer(nameSynth, 2);
    EmailSynthesizer emailSynth = new EmailSynthesizer(random, nameDictionary);

    getPrintStream().printf("Generating %d users on kiji table '%s'...%/n", mNumUsers, getURI());
    final KijiTable kijiTable = getKiji().openTable(getURI().getTable());

    KijiTableWriter tableWriter = kijiTable.openTableWriter();
    for (int i = 0; i < mNumUsers; i++) {
      String fullName = fullNameSynth.synthesize();
      String email = EmailSynthesizer.formatEmail(fullName.replace(" ", "."),
          emailSynth.synthesizeDomain());
      EntityId entityId = kijiTable.getEntityId(email);
      tableWriter.put(entityId, "info", "name", fullName);
      tableWriter.put(entityId, "info", "email", email);

      // Print some status so the user knows it's working.
      if (i % 1000 == 0) {
        getPrintStream().printf("%d rows synthesized...%n", i);
      }
    }
    tableWriter.close();
    kijiTable.close();

    getPrintStream().printf("%d rows synthesized...%n", mNumUsers);
    getPrintStream().println("Done.");

    return 0;
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new KijiToolLauncher().run(new SynthesizeUserDataTool(), args));
  }
}
