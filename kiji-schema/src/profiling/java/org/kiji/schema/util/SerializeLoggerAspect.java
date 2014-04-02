/**
 * (c) Copyright 2013 WibiData, Inc.
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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.aspectj.lang.Aspects;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * This aspect is invoked after the main function in any Kiji tool. It
 * accesses logging information gathered by the LogTimerAspect and
 * serializes it to a local csv file.
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Aspect
public final class SerializeLoggerAspect {
  private String mPid;
  private LogTimerAspect mLogTimerAspect;
  /**
   * The directory prefix to which the profiling output will be saved.
   */
  private static final String PROFILE_OUTPUT_DIR = "/tmp/kijistats_";

  /**
   * Default constructor. Initializes the pid of the JVM running the tool
   * and the singleton LogTimerAspect for this JVM instance.
   */
  protected SerializeLoggerAspect() {
    mPid = ManagementFactory.getRuntimeMXBean().getName();
    if (Aspects.hasAspect(LogTimerAspect.class)) {
      mLogTimerAspect = Aspects.aspectOf(LogTimerAspect.class);
    } else {
      throw new RuntimeException("Log Timer aspect not found!");
    }
  }

  /**
   * Either gets the name of the specific tool that called this advice or the entire signature of
   * the function (if it was called from elsewhere).
   *
   * @param thisJoinPoint The joinpoint that matched the pointcut.
   * @return The string containing the tool name or function signature.
   */
  private String getJoinPointSignature(final JoinPoint thisJoinPoint) {
    Pattern p = Pattern.compile(".*\\.(\\w+)\\.toolMain.*");
    Matcher m = p.matcher(thisJoinPoint.getSignature().toLongString());
    String signature;
    if (m.matches()) {
      signature = m.group(1);
    } else {
      signature = thisJoinPoint.getSignature().toLongString();
    }
    return signature;
  }

  /**
   * Specific logic to create the output file for writing out the log timer data.
   * Example filename: /tmp/kijistats_20130617_1634_31925@DeepThought_VersionTool.csv where,
   * 20130617_1634 is the date and time in yyyymmdd_HHmm,
   * 31925@DeepThought is the process id,
   * VersionTool is the name of the tool being run.
   *
   * @param signature The signature of the joinpoint that matched the pointcut.
   * @return The name of the file which will store profiling information.
   */
  private String createProfilingFilename(String signature) {
    String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmm")
        .format(Calendar.getInstance().getTime());
    String filename = PROFILE_OUTPUT_DIR + timeStamp + "_" + mPid + "_" + signature + ".csv";
    return filename;
  }

  /**
   * Specific logic to write to the file.
   *
   * @param signature String representing either the tool that called aspect.
   * @param filename Name of the file to serialize to.
   */
  private void serializeToFile(String signature, String filename) {
    try {
      FileOutputStream fos = new FileOutputStream(filename, true);
      try {
        OutputStreamWriter out = new OutputStreamWriter(fos, "UTF-8");
        try {
          out.write("Process Id, Tool Name, Function Signature, Aggregate Time (nanoseconds), "
              + "Number of Invocations, Time per call (nanoseconds)\n");
          ConcurrentHashMap<String, LoggingInfo> signatureTimeMap =
              mLogTimerAspect.getSignatureTimeMap();
          // ensure that files do not end up with x.yzE7 format for floats
          NumberFormat nf = NumberFormat.getInstance();
          nf.setGroupingUsed(false);
          nf.setMinimumFractionDigits(1);
          nf.setMaximumFractionDigits(3);

          for (Map.Entry<String, LoggingInfo> entrySet: signatureTimeMap.entrySet()) {
            out.write(mPid + ", " + signature + ", "
                + entrySet.getKey() + ", " + entrySet.getValue().toString() + ", "
                + nf.format(entrySet.getValue().perCallTime()) + "\n");
          }
        } finally {
          out.close();
        }
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      } finally {
        fos.close();
      }
    } catch (IOException fnf) {
      fnf.printStackTrace();
    }
  }

  /**
   * Pointcut to match the toolMain function of BaseTool.
   */
  @Pointcut("execution(* org.kiji.schema.tools.BaseTool.toolMain(..))")
  protected void serializeTool(){
  }

  /**
   * Advice for running after any functions that match PointCut "serializeTool".
   *
   * @param thisJoinPoint The joinpoint that matched the pointcut.
   */
  @AfterReturning("serializeTool() && !cflowbelow(serializeTool())")
  public void afterToolMain(final JoinPoint thisJoinPoint) {
    String signature = getJoinPointSignature(thisJoinPoint);
    String filename = createProfilingFilename(signature);
    serializeToFile(signature, filename);
  }
}
