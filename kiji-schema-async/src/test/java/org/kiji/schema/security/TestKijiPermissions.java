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

package org.kiji.schema.security;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

/** Unit tests for KijiPermissions. */
public class TestKijiPermissions {
  @Test
  public void testCreateEmpty() {
    KijiPermissions permissions = KijiPermissions.emptyPermissions();

    assert(0 == permissions.getActions().size());
    // No actions should be allowed
    for (KijiPermissions.Action action : KijiPermissions.Action.values()) {
      assert(!permissions.allowsAction(action));
    }
  }

  @Test
  public void testAdd() {
    KijiPermissions emptyPermissions = KijiPermissions.emptyPermissions();
    KijiPermissions grantPermissions = emptyPermissions.addAction(KijiPermissions.Action.GRANT);
    assert(grantPermissions.allowsAction(KijiPermissions.Action.GRANT));
  }

  @Test
  public void testCreateAndRemove() {
    Set<KijiPermissions.Action> actions = new HashSet<KijiPermissions.Action>();
    actions.add(KijiPermissions.Action.READ);
    actions.add(KijiPermissions.Action.WRITE);

    KijiPermissions readWritePermissions = KijiPermissions.newWithActions(actions);
    assert(readWritePermissions.allowsAction(KijiPermissions.Action.READ));
    assert(readWritePermissions.allowsAction(KijiPermissions.Action.WRITE));

    KijiPermissions readOnlyPermissions =
        readWritePermissions.removeAction(KijiPermissions.Action.WRITE);
    assert(readOnlyPermissions.allowsAction(KijiPermissions.Action.READ));
    assert(!readOnlyPermissions.allowsAction(KijiPermissions.Action.WRITE));
  }
}
