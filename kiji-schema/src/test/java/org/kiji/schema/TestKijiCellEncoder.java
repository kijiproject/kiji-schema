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

package org.kiji.schema;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.junit.Test;

import org.kiji.schema.avro.MyEnum;
import org.kiji.schema.avro.TestSpecificEnum;

public class TestKijiCellEncoder extends KijiClientTest {
  @Test
  public void testEncode() throws IOException {
    for (KijiCellFormat format : KijiCellFormat.values()) {
      encode(format);
    }
  }

  private void encode(KijiCellFormat format) throws IOException {
    final KijiCell<CharSequence> original =
        new KijiCell<CharSequence>(Schema.create(Schema.Type.STRING), new Utf8("foo"));

    final KijiCellEncoder encoder = new KijiCellEncoder(getKiji().getSchemaTable());
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    encoder.encode(original, out, KijiCellFormat.HASH);

    final KijiCellDecoderFactory decoderFactory =
        new SpecificCellDecoderFactory(getKiji().getSchemaTable());
    final KijiCellDecoder<CharSequence> decoder =
        decoderFactory.create(Schema.create(Schema.Type.STRING), KijiCellFormat.HASH);
    final KijiCell<CharSequence> decoded = decoder.decode(out.toByteArray());
    assertEquals(decoded, original);
  }

  @Test
  public void testEncodeSpecificEnum() throws IOException {
    for (KijiCellFormat format : KijiCellFormat.values()) {
      encodeSpecificEnum(format);
    }
  }

  private void encodeSpecificEnum(KijiCellFormat format) throws IOException {
    // Test that a specific record containing a union of an enum and null can be serialized.
    final TestSpecificEnum myEnumRecord = new TestSpecificEnum();
    myEnumRecord.setA(MyEnum.Cat);
    final KijiCell<TestSpecificEnum> original =
        new KijiCell<TestSpecificEnum>(TestSpecificEnum.SCHEMA$, myEnumRecord);

    final KijiCellEncoder encoder = new KijiCellEncoder(getKiji().getSchemaTable());
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    encoder.encode(original, out, format);

    final KijiCellDecoderFactory decoderFactory =
        new SpecificCellDecoderFactory(getKiji().getSchemaTable());
    final KijiCellDecoder<TestSpecificEnum> decoder =
        decoderFactory.create(TestSpecificEnum.SCHEMA$, format);
    final KijiCell<TestSpecificEnum> decoded = decoder.decode(out.toByteArray());
    assertEquals(decoded, original);

  }
}
