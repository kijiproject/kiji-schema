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

package org.kiji.schema.tools.synth;

import java.util.List;
import java.util.Random;

import org.kiji.annotations.ApiAudience;

/**
 * Synthesizes a random email address.
 */
@ApiAudience.Private
public final class EmailSynthesizer implements Synthesizer<String> {
  /** A random number generator. */
  private Random mRandom;

  /** The total sum of the domain share. */
  private double mTotalShare;

  /** A synthesizer that generates n-grams. */
  private NGramSynthesizer mNGramSynthesizer;

  /** Top 20 email domains of 2006. */
  private enum Domain {
    YAHOO_COM("yahoo.com", 23.30),
    HOTMAIL_COM("hotmail.com", 19.25),
    AOL_COM("aol.com", 7.83),
    GMAIL_COM("gmail.com", 4.86),
    MSN_COM("msn.com", 2.21),
    COMCAST_NET("comcast.net", 1.84),
    HOTMAIL_CO_UK("hotmail.co.uk", 1.65),
    SBCGLOBAL_NET("sbcglobal.net", 1.63),
    YAHOO_CO_UK("yahoo.co.uk", 1.33),
    YAHOO_CO_IN("yahoo.co.in", 1.11),
    BELLSOUTH_NET("bellsouth.net", 0.91),
    VERIZON_NET("verizon.net", 0.73),
    EARTHLINK_NET("earthlink.net", 0.70),
    COX_NET("cox.net", 0.68),
    REDIFFMAIL_COM("rediffmail.com", 0.64),
    YAHOO_CA("yahoo.ca", 0.60),
    BTINTERNET_COM("btinternet.com", 0.57),
    CHARTER_NET("charter.net", 0.43),
    SHAW_CA("shaw.ca", 0.43),
    NTLWORLD_COM("ntlworld.com", 0.41);

    /** The domain string. */
    private String mDomain;
    /** The percent share of the email domain market. */
    private double mShare;

    /**
     * Constructs an email domain enum value.
     *
     * @param domain The name of the domain.
     * @param share The percent share of the market.
     */
    private Domain(String domain, double share) {
      mDomain = domain;
      mShare = share;
    }

    /**
     * Gets the name of the domain.
     *
     * @return The domain name.
     */
    public String getDomain() {
      return mDomain;
    }

    /**
     * Gets the percent market share of the domain.
     *
     * @return The market share in percents.
     */
    public double getShare() {
      return mShare;
    }
  }

  /**
   * Creates a new email synthesizer.
   *
   * @param random A random number generator.
   * @param nameDictionary A dictionary of names for the email addresses.
   */
  public EmailSynthesizer(Random random, List<String> nameDictionary) {
    mRandom = random;
    for (Domain domain : Domain.class.getEnumConstants()) {
      mTotalShare += domain.getShare();
    }
    mNGramSynthesizer = new NGramSynthesizer(new WordSynthesizer(mRandom, nameDictionary), 2);
  }

  @Override
  public String synthesize() {
    return formatEmail(mNGramSynthesizer.synthesize().replace(" ", "."), synthesizeDomain());
  }

  /**
   * Synthesize a random domain using the market share probabilities.
   *
   * @return A synthesized domain string.
   */
  public String synthesizeDomain() {
    double random = mRandom.nextDouble() * mTotalShare;

    String domainString = "unknown.com";
    for (Domain domain : Domain.class.getEnumConstants()) {
      random -= domain.getShare();
      if (random < 0) {
        domainString = domain.getDomain();
        break;
      }
    }

    return domainString;
  }

  /**
   * Construct an email address string.
   *
   * @param username The part before the '@' symbol.
   * @param domain The part after the '@' symbol.
   * @return An email address (&lt;username&gt;@&lt;domain&gt;).
   */
  public static String formatEmail(String username, String domain) {
    return username + "@" + domain;
  }
}
