/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.util.NativeCodeLoader;

/**
 * jon:
 * 
 * This is my hacked up version of SequenceFile. All the warnings were there
 * originally. I want to keep the same options and file format, but want the
 * ability to flush data to disk at a fine granularity. I've removed a bunch of
 * the code from the 0.18.3 release version of Sequence file because they are
 * not relevant here.
 * 
 * This is a hack of the hadoop 0.18.3's o.a.h.io.SequenceFile so that it
 * flushes after every append. This is the format I am using for my write ahead
 * log, and it just so happens to be compatible with hadoop 0.18.3's normal file
 * format.
 * 
 * There are a few problems with the existing mechanisms: 1) it doesn't flush to
 * disk (I want to use this format for a WAL and 2) The ChecksumFs underneath
 * the default file system does not flush.
 * 
 * Summary of changes: * Added LocalFSOutputStream Class. * Added my own
 * createWriter function that uses LocalFSOutputStream * Added flush call to
 * sync in writer.
 */

public class FlushingSequenceFileWriter extends SequenceFile.Writer {

  public static Writer createWriter(Configuration conf, File f,
      Class<?> keyClass, Class<?> valClass) throws IOException {
    return createWriter(conf, f.getAbsolutePath(), keyClass, valClass, false,
        false, null, new SequenceFile.Metadata());
  }

  /**
   * jon: this is my hacked version that bypasses the buffer
   * 
   * Hiding this for now -- may open this up to enable compression in the future
   */
  private static Writer createWriter(Configuration conf, String fname,
      Class<?> keyClass, Class<?> valClass, boolean compress,
      boolean blockCompress, CompressionCodec codec, Metadata metadata)
      throws IOException {
    if (codec != null && (codec instanceof GzipCodec)
        && !NativeCodeLoader.isNativeCodeLoaded()
        && !ZlibFactory.isNativeZlibLoaded(conf)) {
      throw new IllegalArgumentException("SequenceFile doesn't work with "
          + "GzipCodec without native-hadoop code!");
    }

    Writer writer = null;
    FSDataOutputStream out = new FSDataOutputStream(
        new FlushingSequenceFileWriter.LocalFSFileOutputStream(new File(fname),
            true), null);
    if (!compress) {
      writer = new FlushingSequenceFileWriter(conf, out, keyClass, valClass,
          metadata);

      // TODO (jon). There are visibility issues that prevent me from hooking
      // into the RecordCompressWriter and BlockCompressWriters. These probably
      // don't make sense for this application at the momene.F

      // } else if (compress && !blockCompress) {
      // writer = new SequenceFile.RecordCompressWriter(conf, out, keyClass,
      // valClass, codec, metadata);
      // } else {
      // writer = new SequenceFile.BlockCompressWriter(conf, out, keyClass,
      // valClass, codec, metadata);
    }

    return writer;
  }

  /** Implicit constructor: needed for the period of transition! */
  FlushingSequenceFileWriter() {
    super();
  }

  /** Write to an arbitrary stream using a specified buffer size. */
  public FlushingSequenceFileWriter(Configuration conf, FSDataOutputStream out,
      Class<?> keyClass, Class<?> valClass, Metadata metadata)
      throws IOException {
    // Note: I want to do this but apparently cannot
    // super(conf, out, keyClass, valClass, metadata);

    // instead we duplicate the constructor code.
    this.ownOutputStream = true;
    init(null, conf, out, keyClass, valClass, false, null, metadata);

    initializeFileHeader();
    writeFileHeader();
    finalizeFileHeader();
  }

  /** create a sync point */
  public void sync() throws IOException {
    super.sync();

    // this is the addition that causes the buffer to flush out of app memory to
    // disk/kernel memory.
    this.out.flush();
  }

  // This was stolen from RawLocalFileSystem. I want a
  // FSOutputStream without a buffer in the way.
  /*****************************************************************************
   * For create()'s FSOutputStream.
   ****************************************************************************/
  static class LocalFSFileOutputStream extends OutputStream implements Syncable {
    FileOutputStream fos;

    private LocalFSFileOutputStream(File f, boolean append) throws IOException {
      this.fos = new FileOutputStream(f, append);
    }

    /*
     * Just forward to the fos
     */
    public void close() throws IOException {
      fos.close();
    }

    public void flush() throws IOException {
      fos.flush();
    }

    public void write(byte[] b, int off, int len) throws IOException {
      fos.write(b, off, len);
    }

    public void write(int b) throws IOException {
      fos.write(b);
    }

    /** {@inheritDoc} */
    public void sync() throws IOException {
      fos.getFD().sync();
    }
  }

} // SequenceFile
