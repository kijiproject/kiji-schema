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

package org.kiji.schema.util;

import static org.easymock.EasyMock.reportMatcher;

import org.apache.hadoop.hbase.client.Scan;
import org.easymock.IArgumentMatcher;

public class ScanEquals implements IArgumentMatcher {
  private Scan mExpected;

  public ScanEquals(Scan expected) {
    mExpected = expected;
  }

  @Override
  public boolean matches(Object actual) {
    if (!(actual instanceof Scan)) {
      return false;
    }
    // The toString() of Scan has all the relevant data we need to compare.
    return mExpected.toString().equals(actual.toString());
  }

  @Override
  public void appendTo(StringBuffer buffer) {
    buffer.append("eqScan(")
        .append(mExpected.toString())
        .append(")");
  }

  public static <T extends Scan> T eqScan(T in) {
    reportMatcher(new ScanEquals(in));
    return null;
  }
}
