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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.easymock.IArgumentMatcher;

public class PutEquals implements IArgumentMatcher {
  private Put mExpected;

  public PutEquals(Put expected) {
    mExpected = expected;
  }

  @Override
  public boolean matches(Object actual) {
    if (!(actual instanceof Put)) {
      return false;
    }
    Put put = (Put) actual;
    if (!Arrays.equals(mExpected.getRow(), put.getRow())) {
      return false;
    }
    Map<byte[], List<KeyValue>> expectedMap = mExpected.getFamilyMap();
    Map<byte[], List<KeyValue>> actualMap = put.getFamilyMap();
    if (expectedMap.size() != actualMap.size()) {
      return false;
    }
    for (byte[] key : expectedMap.keySet()) {
      List<KeyValue> expectedList = expectedMap.get(key);
      List<KeyValue> actualList = actualMap.get(key);
      if (null == expectedList && null == actualList) {
        continue;
      }
      if (null == expectedList || null == actualList) {
        return false;
      }
      if (expectedList.size() != actualList.size()) {
        return false;
      }
      for (int i = 0; i < expectedList.size(); i++) {
        KeyValue expectedKeyValue = expectedList.get(i);
        KeyValue actualKeyValue = actualList.get(i);
        if (!Arrays.equals(expectedKeyValue.getRow(), actualKeyValue.getRow())
            || !Arrays.equals(expectedKeyValue.getFamily(), actualKeyValue.getFamily())
            || !Arrays.equals(expectedKeyValue.getQualifier(), actualKeyValue.getQualifier())
            || !Arrays.equals(expectedKeyValue.getValue(), actualKeyValue.getValue())) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public void appendTo(StringBuffer buffer) {
    buffer.append("eqPut(")
        .append(mExpected.toString())
        .append(")");
  }

  public static <T extends Put> T eqPut(T in) {
    reportMatcher(new PutEquals(in));
    return null;
  }
}
