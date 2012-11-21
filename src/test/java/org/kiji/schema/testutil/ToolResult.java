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

package org.kiji.schema.testutil;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;

/**
 * Running a tool within integration tests produces a few things we want to know:
 * 1. A return code.
 * 2. An output stream (STDOUT).
 *
 * This class encapsulates the result of running a tool within an integration test.
 */
public class ToolResult {
  private int mReturnCode;
  private ByteArrayOutputStream mStdout;

  public ToolResult(int returnCode, ByteArrayOutputStream stdout) {
    mReturnCode = returnCode;
    mStdout = stdout;
  }

  public int getReturnCode() {
    return mReturnCode;
  }

  public ByteArrayOutputStream getStdout() {
    return mStdout;
  }

  public String getStdout(String encoding) throws UnsupportedEncodingException {
    return mStdout.toString(encoding);
  }

  public String getStdoutUtf8() {
    try {
      return getStdout("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("UTF-8 not supported", e);
    }
  }
}
