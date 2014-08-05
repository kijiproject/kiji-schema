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

package org.kiji.schema.extra.tools;

import java.util.{List => JList}
import java.util.Locale

import scala.collection.JavaConverters.asScalaIteratorConverter

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.HTableInterface
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.io.hfile.CacheConfig
import org.apache.hadoop.hbase.io.hfile.HFile
import org.apache.hadoop.hbase.io.hfile.HFileScanner
import org.kiji.common.flags.Flag
import org.kiji.common.flags.FlagParser
import org.kiji.schema.Kiji
import org.kiji.schema.KijiURI
import org.kiji.schema.hbase.HBaseFactory
import org.kiji.schema.platform.SchemaPlatformBridge
import org.kiji.schema.tools.BaseTool
import org.slf4j.LoggerFactory
import org.kiji.schema.impl.hbase.HBaseKijiTable

/** HFile testing utilities. */
class HFileTool extends BaseTool {
  private final val Log = LoggerFactory.getLogger(classOf[InspectFileTool])

  @Flag(name="path",
      usage="Path of the file to read from/write to.")
  var pathFlag: String = null

  @Flag(name="do",
      usage="Either 'import' or 'export'.")
  var doFlag: String = null

  @Flag(name="table",
      usage="""URI of a Kiji table to read from/write to.
        Only one of --kiji and --htable may be specified at a time.""")
  var tableFlag: String = null

  @Flag(name="hbase",
      usage="""URI of a Kiji instance to read from/write to.
        Only one of --table and --hbase may be specified at a time.""")
  var hbaseFlag: String = null

  @Flag(name="htable",
      usage="""Name of the HTable to read from/write to.
        Requires specifying an HBase instance with --hbase.""")
  var htableFlag: String = null

  @Flag(name="hfile-compression",
      usage="HFile compression algorithm: one of 'none', 'gz', 'lz4', 'lzo', 'snappy'.")
  var hfileCompressionFlag: String = "NONE"

  @Flag(name="hfile-block-size",
      usage="HFile block size, in bytes. Defaults to 64M.")
  var hfileBlockSizeFlag: Int = 64 * 1024 * 1024

  override def getName(): String = {
    return "hfile"
  }

  override def getCategory(): String = {
    return "extra"
  }

  override def getDescription(): String = {
    return "Exports/imports HBase/Kiji tables to/from HFile."
  }

  override def getUsageString(): String = {
    return """Usage:
        |    kiji hfile [--do=](import|export) \
        |        --path=<hfile-path> \
        |        ( --table=<kiji-table-uri>
        |        | --hbase=<hbase-instance-uri> --htable=<hbase-table-name> )
        |
        |Examples:
        |  Export Kiji table kiji://zkhost:port/default/table to /path/to/hfile:
        |    kiji hfile export --path=/path/to/hfile --table=kiji://zkhost:port/default/table
        |
        |  Import /path/to/hfile into HBase instance kiji://zkhost:port and table named 'table':
        |    kiji hfile import --path=/path/to/hfile --hbase=kiji://zkhost:port --htable=table
        |"""
        .stripMargin
  }

  /**
   * Dumps an HBase table to an HFile.
   *
   * @param table is the HBase table to dump.
   * @param path is the path of the HFile to write to.
   * @param compression is the algorithm to use to compress the HFile content.
   * @param blockSize is the block size, in bytes.
   */
  def writeToHFile(
      table: HTableInterface,
      path: Path,
      compression: String = "none",
      blockSize: Int = 64 * 1024 * 1024
  ): Unit = {
    val conf = HBaseConfiguration.create()
    val cacheConf = new CacheConfig(conf)
    val fs = FileSystem.get(conf)
    val writer: HFile.Writer = SchemaPlatformBridge.get().createHFileWriter(
        conf, fs, path, blockSize, compression)

    try {
      val scanner = table.getScanner(new Scan().setMaxVersions())
      try {
        for (result <- scanner.iterator.asScala) {
          val rowKey = result.getRow
          for (fentry <- result.getMap.entrySet.iterator.asScala) {
            val (family, qmap) = (fentry.getKey, fentry.getValue)
            for (qentry <- qmap.entrySet.iterator.asScala) {
              val (qualifier, series) = (qentry.getKey, qentry.getValue)
              for (tentry <- series.descendingMap.entrySet.iterator.asScala) {
                val (timestamp, value) = (tentry.getKey, tentry.getValue)
                val keyValue = new KeyValue(rowKey, family, qualifier, timestamp, value)
                writer.append(keyValue)
              }
            }
          }
        }
      } finally {
        scanner.close()
      }
    } finally {
      writer.close()
    }
  }

  /**
   * Populates an HBase table from an HFile.
   *
   * @param table is the HBase table to populate.
   * @param path is the path of the HFile to read from.
   */
  def readFromHFile(table: HTableInterface, path: Path): Unit = {
    val conf = HBaseConfiguration.create()
    val cacheConf = new CacheConfig(conf)
    val fs = FileSystem.get(conf)
    val reader: HFile.Reader = HFile.createReader(fs, path, cacheConf)
    try {
      val cacheBlocks = false
      val positionalRead = false

      /** HFileScanner has no close() method. */
      val scanner: HFileScanner = reader.getScanner(cacheBlocks, positionalRead)

      var hasNext = scanner.seekTo()
      while (hasNext) {
        val keyValue = scanner.getKeyValue
        val rowKey = keyValue.getRow
        val family = keyValue.getFamily
        val qualifier = keyValue.getQualifier
        val timestamp = keyValue.getTimestamp
        val value = keyValue.getValue
        table.put(new Put(rowKey).add(family, qualifier, timestamp, value))

        hasNext = scanner.next()
      }
    } finally {
      reader.close()
    }
  }

  /**
   * Program entry point.
   *
   * @param unparsed is the array of command-line arguments.
   */
  override def run(unparsed: JList[String]): Int = {
    // Requires either --do=(import|export) or a single unnamed argument (exclusive OR):
    if (!((unparsed.size == 1) ^ ((doFlag != null) && unparsed.isEmpty))) {
      FlagParser.printUsage(this, Console.out)
      return BaseTool.FAILURE
    }
    val action = if (doFlag != null) doFlag else unparsed.get(0)
    if (!Set("import", "export").contains(action)) {
      print("Unknown action '%s': specify either 'import' or 'export'.".format(action))
        FlagParser.printUsage(this, Console.out)
        return BaseTool.FAILURE
    }

    require(pathFlag != null, "Specify the file to read from/write to with --path=...")
    val filePath = new Path(pathFlag)

    val hfileCompression = hfileCompressionFlag.toUpperCase(Locale.ROOT)

    def runAction(htable: HTableInterface, path: Path) {
      action match {
        case "import" => readFromHFile(htable, path)
        case "export" => writeToHFile(htable, path, hfileCompression, hfileBlockSizeFlag)
        case _ => sys.error("Unknown action: %s".format(action))
      }
    }

    require((tableFlag != null) ^ (hbaseFlag != null), "Specify exactly one of --table and --hbase")
    require((htableFlag != null) == (hbaseFlag != null),
        "--htable must be specified along with --hbase, and may not specified if --table is.")
    if (tableFlag != null) {
      val tableURI = KijiURI.newBuilder(tableFlag).build()
      val kiji = Kiji.Factory.open(tableURI)
      try {
        val table = kiji.openTable(tableURI.getTable)
        try {
          val htable = table.asInstanceOf[HBaseKijiTable].openHTableConnection()
          try {
            runAction(htable, filePath)
          } finally {
            htable.close()
          }
        } finally {
          table.release()
        }
      } finally {
        kiji.release()
      }

    } else if ((hbaseFlag != null) && (htableFlag != null)) {
      val hbaseURI = KijiURI.newBuilder(hbaseFlag).build()
      val htableFactory = HBaseFactory.Provider.get().getHTableInterfaceFactory(hbaseURI)
      val htable = htableFactory.create(getConf, htableFlag)
      try {
          runAction(htable, filePath)
      } finally {
        htable.close()
      }

    } else {
      sys.error("No table specified")
    }

    return BaseTool.SUCCESS
  }
}
