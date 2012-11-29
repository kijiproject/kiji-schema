// (c) Copyright 2011 WibiData, Inc.

package com.wibidata.core.testutil;

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
