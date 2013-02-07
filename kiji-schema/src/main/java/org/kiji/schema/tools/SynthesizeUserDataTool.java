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

import com.google.common.base.Preconditions;

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.tools.synth.DictionaryLoader;
import org.kiji.schema.tools.synth.EmailSynthesizer;
import org.kiji.schema.tools.synth.NGramSynthesizer;
import org.kiji.schema.tools.synth.WordSynthesizer;

/**
 * Synthesize some user data into a kiji table.
 */
@ApiAudience.Private
public final class SynthesizeUserDataTool extends VersionValidatedTool {
  @Flag(name="name-dict", usage="File that contains people names, one per line")
  private String mNameDictionaryFilename = "org/kiji/schema/tools/synth/top_names.txt";

  @Flag(name="num-users", usage="Number of users to synthesize")
  private int mNumUsers = 100;

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
    Preconditions.checkArgument(getURI().getTable() != null,
        "Specify a table with --kiji=kiji://hbase-cluster/kiji-instance/table");
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

    getPrintStream().printf("Generating %d users on kiji table '%s'...%n", mNumUsers, getURI());
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
