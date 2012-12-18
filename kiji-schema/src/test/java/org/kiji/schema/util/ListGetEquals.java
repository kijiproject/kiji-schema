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

import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.easymock.IArgumentMatcher;

// This class is mostly copied from org.kiji.schema.util.GetEquals.
public class ListGetEquals implements IArgumentMatcher {
  private List<Get> mExpected;

  public ListGetEquals(List<Get> expected) {
    mExpected = expected;
  }

  @Override
  public boolean matches(Object actual) {
    if (!(actual instanceof List<?>)) {
      return false;
    }
    for (Object element : (List<?>) actual) {
      if (!(element instanceof Get)) {
        return false;
      }
    }

    // The toString() of Get has all the relevant data we need to compare.
    return mExpected.toString().equals(actual.toString());
  }

  @Override
  public void appendTo(StringBuffer buffer) {
    buffer.append("eqListGet(")
        .append(mExpected.toString())
        .append(")");
  }

  public static <T extends List<Get>> T eqListGet(T in) {
    reportMatcher(new ListGetEquals(in));
    return null;
  }
}

