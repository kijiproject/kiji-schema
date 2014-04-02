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

import java.util.concurrent.ConcurrentHashMap;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * Aspect to measure encoding and decoding of Kiji cells and time spent
 * accessing the meta table.
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Aspect
public final class LogTimerAspect {
  /**
   * The ConcurrentHashMap containing information about a function call, the aggregate
   * time spent within this function and the number of times it was invoked.
   */
  private ConcurrentHashMap<String, LoggingInfo> mSignatureTimeMap = null;

  /**
   * Default constructor.
   */
  protected LogTimerAspect() {
    mSignatureTimeMap = new ConcurrentHashMap<String, LoggingInfo>();
  }

  /**
   * Get the HashMap of functions to information stored about them.
   *
   * @return HashMap containing function calls and time spent in them.
   */
  public ConcurrentHashMap<String, LoggingInfo> getSignatureTimeMap() {
    return mSignatureTimeMap;
  }

  /**
   * Advice around functions that match PointCut "profile".
   *
   * @param thisJoinPoint The JoinPoint that matched the pointcut.
   * @return Object returned by function which matched PointCut "profile".
   * @throws Throwable if there is an exception in the function the advice surrounds.
   */
  @Around("execution(* org.kiji.schema.KijiCellDecoder.*(..)) || "
      + "execution(* org.kiji.schema.KijiCellEncoder.*(..)) || "
      + "execution(* org.kiji.schema.KijiMetaTable.*(..)) || "
      + "execution(* org.kiji.schema.KijiSchemaTable.*(..)) || "
      + "execution(* org.kiji.schema.KijiPutter.put(..))")
  public Object aroundProfileMethods(final ProceedingJoinPoint thisJoinPoint) throws Throwable {
    final long start, end;
    start = System.nanoTime();
    Object returnanswer = thisJoinPoint.proceed();
    end = System.nanoTime();
    String funcSig = thisJoinPoint.getSignature().toLongString();
    if (!mSignatureTimeMap.containsKey(funcSig)) {
      mSignatureTimeMap.put(funcSig, new LoggingInfo(end - start, 1));
    } else {
      mSignatureTimeMap.get(funcSig).increment(end - start);
    }
    return returnanswer;
  }
}
