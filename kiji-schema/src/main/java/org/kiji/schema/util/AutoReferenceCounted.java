/**
 * (c) Copyright 2014 WibiData, Inc.
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

import java.io.Closeable;
import java.util.Collection;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;

/**
 * An {@code AutoReferenceCounted} is an object which can be registered with an
 * {@link AutoReferenceCountedReaper} instance to have resources closed prior to being garbage
 * collected. {@code AutoReferenceCounted} is similar to reference counting, except that the
 * reference counting is performed by the JVM garbage collector. Users of
 * {@code AutoReferenceCounted} objects need only to register the object with an
 * {@link AutoReferenceCountedReaper} upon creation; no explicit life-cycle management needs to be
 * done. The object's resources will automatically be released when the {@code AutoReferenceCounted}
 * is claimed by the garbage collector.
 *
 * {@code AutoReferenceCounted} and {@link AutoReferenceCountedReaper} use the
 * {@link java.lang.ref.PhantomReference} mechanism of the JVM to ensure that no threads have a
 * reference to the {@code AutoReferenceCounted} before closing the resources of the
 * {@code AutoReferenceCounted}.
 *
 * To fulfill the {@code AutoReferenceCounted} interface, classes must define the
 * {@link #getCloseableResources()} method, which must return the {@link Closeable} resources held
 * by the instance.
 *
 * *Special Note* because cleanup relies on the {@code AutoReferenceCounted} no longer being
 * reachable, users of {@code AutoReferenceCounted} instances must take special care to hold a
 * strong reference to instances while in use, and to let go of the reference after the
 * AutoReferenceCounted is no longer needed. Non-static inner-classes hold a reference to their
 * outer class, so the same care should be taken with non-static inner-class instances of an
 * {@code AutoReferenceCounted}. In particular, do *not* return a non-static inner class from
 * {@code #getCloseableResources}, as this will cause the {@code AutoReferenceCounted} to never be
 * collected due to a reachable cyclical reference.
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Sealed
public interface AutoReferenceCounted {

  /**
   * Returns a collection of Closeable resources held by this AutoReferenceCounted which must be
   * closed in order to properly dispose of this object.
   *
   * @return the Closeable resources held by this {@code AutoReferenceCounted}.
   */
  Collection<Closeable> getCloseableResources();
}
