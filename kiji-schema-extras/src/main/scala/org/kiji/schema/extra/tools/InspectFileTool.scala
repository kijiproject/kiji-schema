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

package org.kiji.schema.extra.tools

import java.util.Arrays

import org.apache.avro.file.DataFileReader
import org.apache.avro.file.FileReader
import org.apache.avro.file.SeekableInput
import org.apache.avro.generic.GenericDatumReader
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.AvroFSInput
import org.apache.hadoop.fs.FileContext
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.io.hfile.CacheConfig
import org.apache.hadoop.hbase.io.hfile.FixedFileTrailer
import org.apache.hadoop.hbase.io.hfile.HFile
import org.apache.hadoop.hbase.io.hfile.HFileReaderV2
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.MapFile
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.WritableComparable
import org.apache.hadoop.util.ReflectionUtils
import org.kiji.common.flags.Flag
import org.kiji.common.flags.FlagParser
import org.kiji.schema.tools.BaseTool
import org.slf4j.LoggerFactory

/**
 * Command-line tool to inspect common files used in Map/Reduce jobs.
 */
class InspectFileTool extends BaseTool {
  private final val Log = LoggerFactory.getLogger(classOf[InspectFileTool])

  @Flag(name="format", usage="File format. One of 'text', 'seq', 'hfile', 'map', 'avro'.")
  var format: String = null

  @Flag(name="path",
      usage="Optional path to the sequence file to inspect.")
  var pathFlag: String = null

  override def getName(): String = {
    return "cat"
  }

  override def getCategory(): String = {
    return "extra"
  }

  override def getDescription(): String = {
    return "Inspects the content of a file."
  }

  override def getUsageString(): String = {
    return """Usage:
        |    kiji inspect-file <path> [--format=(text|seq|avro|map|hfile)]
        |"""
        .stripMargin
  }

  /**
   * Reads a sequence file of (song ID, # of song plays) into a map.
   *
   * @param path is the Hadoop path of the sequence file to read.
   */
  private def readSequenceFile(path: Path): Unit = {
    val reader = new SequenceFile.Reader(getConf, SequenceFile.Reader.file(path))
    try {
      Console.err.println("Key class: %s".format(reader.getKeyClassName))
      Console.err.println("Value class: %s".format(reader.getValueClassName))
      Console.err.println("Compression type: %s".format(reader.getCompressionType))
      Console.err.println("Compression codec: %s".format(reader.getCompressionCodec))
      if (!reader.getMetadata.getMetadata.isEmpty) {
        Console.err.println("Metadata:%n%s".format(reader.getMetadata.getMetadata))
      } else {
        Console.err.println("File '%s' has no metadata".format(path))
      }

      val key: Writable =
          ReflectionUtils.newInstance(reader.getKeyClass, getConf)
          .asInstanceOf[Writable]
      val value: Writable =
          ReflectionUtils.newInstance(reader.getValueClass, getConf)
          .asInstanceOf[Writable]
      while (true) {
        val position = reader.getPosition
        if (!reader.next(key, value)) {
          return
        }
        Console.out.println("position=%s\tkey=%s\tvalue=%s".format(position, key, value))
      }
    } finally {
      reader.close()
    }
  }

  /**
   * Reads a Hadoop map file.
   *
   * @param path is the Hadoop path of the map file to read.
   */
  private def readMapFile(path: Path): Unit = {
    val reader = new MapFile.Reader(path, getConf)
    try {
      val key: WritableComparable[_] =
          ReflectionUtils.newInstance(reader.getKeyClass, getConf)
          .asInstanceOf[WritableComparable[_]]
      val value: Writable =
          ReflectionUtils.newInstance(reader.getValueClass, getConf)
          .asInstanceOf[Writable]
      while (true) {
        if (!reader.next(key, value)) {
          return
        }
        Console.out.println("key=%s\tvalue=%s".format(key, value))
      }
    } finally {
      reader.close()
    }
  }

  /**
   * Reads an HBase HFile.
   *
   * @param path is the Hadoop path to the HFile to read.
   */
  private def readHFile(path: Path): Unit = {
    val cacheConf = new CacheConfig(getConf)
    val fs = FileSystem.get(getConf)

    val status = fs.getFileStatus(path)
    val fileSize = status.getLen
    val istream = fs.open(path)
    val trailer = FixedFileTrailer.readFromStream(istream, fileSize)

    val closeIStream = true
    val reader: HFile.Reader =
        new HFileReaderV2(path, trailer, istream, fileSize, closeIStream, cacheConf)
    try {
      val cacheBlocks = false
      val positionalRead = false
      val scanner = reader.getScanner(cacheBlocks, positionalRead)

      var hasNext = scanner.seekTo()
      while (hasNext) {
        val keyValue = scanner.getKeyValue
        val rowKey = keyValue.getRow
        val family = keyValue.getFamily
        val qualifier = keyValue.getQualifier
        val timestamp = keyValue.getTimestamp
        val value = keyValue.getValue

        Console.out.println("row=%-30sfamily=%-10squalifer=%-10stimestamp=%s\tvalue=%s".format(
            Bytes.toStringBinary(rowKey),
            Bytes.toStringBinary(family),
            Bytes.toStringBinary(qualifier),
            timestamp,
            Bytes.toStringBinary(value)))

        hasNext = scanner.next()
      }
      // no need to close scanner
      // istream is closed by HFile.Reader
    } finally {
      reader.close()
    }
  }

  /**
   * Reads an Avro container file.
   *
   * @param path is the Hadoop path of the Avro container file to read.
   */
  def readAvroContainer(path: Path): Unit = {
    val context: FileContext = FileContext.getFileContext()
    val input: SeekableInput = new AvroFSInput(context, path)
    val datumReader: GenericDatumReader[_] = new GenericDatumReader()
    val reader: FileReader[_] = DataFileReader.openReader(input, datumReader)
    try {
      println("Schema:\n%s".format(reader.getSchema().toString(true)))
      var counter = 0
      while (reader.hasNext) {
        val rec = reader.next()
        println("entry #%d: %s".format(counter, rec))
        counter += 1
      }
    } finally {
      reader.close()
    }
  }

  /**
   * Reads a text file.
   *
   * @param path is the Hadoop path of the text file to read.
   */
  def readTextFile(path: Path): Unit = {
    val fs = FileSystem.get(getConf)
    val istream = fs.open(path)
    try {
      IOUtils.copy(istream, Console.out)
    } finally {
      istream.close()
    }
  }

  /**
   * Reads the first bytes in a file.
   *
   * @param path is the Hadoop path of the file to read from.
   * @param nbytes is the number of bytes to read.
   * @return the first bytes from the specified file.
   */
  def readFileStart(path: Path, conf: Configuration, nbytes: Int = 16): Array[Byte] = {
    val fs = FileSystem.get(conf)
    val istream = fs.open(path)
    try {
      val bytes = new Array[Byte](16)
      val nbytesRead = istream.read(bytes)
      return bytes.slice(0, nbytesRead)
    } finally {
      istream.close()
    }
  }

  /**
   * Tests whether a file's first bytes match a given magic code.
   *
   * @param bytes is the first bytes from a file to test.
   * @param magic is a magic code to test for.
   * @return whether the first bytes in the file match the specified magic code.
   */
  def isMagic(bytes: Array[Byte], magic: Array[Byte]): Boolean = {
    return Arrays.equals(bytes.slice(0, magic.length), magic)
  }

  // See DataFileConstants.MAGIC
  final val AvroContainerFileMagic = Array[Byte]('O', 'b', 'j')

  // See SequenceFile.VERSION
  final val SequenceFileMagic = Array[Byte]('S', 'E', 'Q')

  /**
   * Guesses a file type.
   *
   * @param path is the path of the file to guess the type of.
   * @param conf is a Hadoop configuration.
   * @return the guessed file type, or None.
   */
  def guessFileType(path: Path, conf: Configuration): Option[String] = {
    try {
      val bytes = readFileStart(path, conf)
      if (isMagic(bytes, AvroContainerFileMagic)) return Some("avro")
      if (isMagic(bytes, SequenceFileMagic)) return Some("seq")
    } catch {
      case exn => Console.err.println(exn)
    }
    return None
  }

  /**
   * Program entry point.
   *
   * @param args is the array of command-line arguments.
   */
  override def run(args: java.util.List[String]): Int = {
    val unparsed =
        FlagParser.init(InspectFileTool.this, args.toArray(new Array[String](args.size)))
    // Requires either --path=<path> or a single unnamed argument <path> (exclusive OR):
    if (!((unparsed.size == 1) ^ ((pathFlag != null) && unparsed.isEmpty))) {
      FlagParser.printUsage(this, Console.out)
      return BaseTool.FAILURE
    }

    val path: String = if (pathFlag != null) pathFlag else unparsed.get(0)
    val filePath = new Path(path)

    if (format == null) {
      format = guessFileType(filePath, getConf) match {
        case Some(fmt) => fmt
        case None => "text"  // assume text if no other type is detected
      }
      Log.info("Detected file type: {}", format)
    }
    format match {
      case "seq" => readSequenceFile(filePath)
      case "map" => readMapFile(filePath)
      case "hfile" => readHFile(filePath)
      case "avro" => readAvroContainer(filePath)
      case "text" => readTextFile(filePath)
      case _ => sys.error("Unknown file format: %s".format(format))
    }
    return BaseTool.SUCCESS
  }
}
