// -*- mode:Java; tab-width:2; c-basic-offset:2; indent-tabs-mode:t -*- 

/**
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * 
 * Implements the Hadoop FS interfaces to allow applications to store
 * files in Ceph.
 */

package org.apache.hadoop.fs.cephrgw;


import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;

import com.ceph.rgw.CephRgwAdapter;

/**
 * <p>
 * An {@link OutputStream} for a CephRgwFileSystem and corresponding
 * Ceph instance.
 *
 * TODO:
 *  - When libcephfs-jni supports ByteBuffer interface we can get rid of the
 *  use of the buffer here to reduce memory copies and just use buffers in
 *  libcephfs. Currently it might be useful to reduce JNI crossings, but not
 *  much more.
 */
public class CephOutputStream extends OutputStream {
  private static final Log LOG = LogFactory.getLog(CephOutputStream.class);
  private boolean closed;

  private CephFsProto ceph;

  private long fileHandle;

  private byte[] buffer;
  private int bufUsed = 0;
  private long cephPos = 0;

  /**
   * Construct the CephOutputStream.
   * @param conf The FileSystem configuration.
   * @param fh The Ceph filehandle to connect to.
   */
  public CephOutputStream(Configuration conf, CephFsProto cephfs,
	long fh, int bufferSize) {
    ceph = cephfs;
    fileHandle = fh;
    closed = false;
    buffer = new byte[1<<22];
  }

  /**
   * Close the Ceph file handle if close() wasn't explicitly called.
   */
  protected void finalize() throws Throwable {
    try {
      if (!closed) {
        close();
      }
    } finally {
      super.finalize();
    }
  }

  /**
   * Ensure that the stream is opened.
   */
  private synchronized void checkOpen() throws IOException {
    if (closed)
      throw new IOException("operation on closed stream (fd=" + fileHandle + ")");
  }

  /**
   * Get the current position in the file.
   * @return The file offset in bytes.
   */
  public synchronized long getPos() throws IOException {
    checkOpen();
    return cephPos + bufUsed;
  }

  @Override
  public synchronized void write(int b) throws IOException {
    byte buf[] = new byte[1];
    buf[0] = (byte) b;    
    write(buf, 0, 1);
  }

  @Override
  public synchronized void write(byte buf[], int off, int len) throws IOException {
    checkOpen();

    while (len > 0) {
      int remaining = Math.min(len, buffer.length - bufUsed);
      System.arraycopy(buf, off, buffer, bufUsed, remaining);

      bufUsed += remaining;
      off += remaining;
      len -= remaining;
   
     if (buffer.length == bufUsed)
       flushBuffer();
    }
  }

  /*
   * Moves data from the buffer into libcephfs.
   */
  private synchronized void flushBuffer() throws IOException {
    if (bufUsed == 0)
      return;

    while (bufUsed > 0) {
      int ret = ceph.write(fileHandle, cephPos, buffer, bufUsed);
      if (ret < 0)
        throw new IOException("ceph.write: ret=" + ret);
        cephPos += ret;
      if (ret ==bufUsed) {
        bufUsed = 0;
        return;
      }

      assert(ret > 0);
      assert(ret < bufUsed);

      /*
       * TODO: handle a partial write by shifting the remainder of the data in
       * the buffer back to the beginning and retrying the write. It would
       * probably be better to use a ByteBuffer 'view' here, and I believe
       * using a ByteBuffer has some other performance benefits but we'll
       * likely need to update the libcephfs-jni implementation.
       */
      int remaining = bufUsed - ret;
      System.arraycopy(buffer, ret, buffer, 0, remaining);
      bufUsed -= ret;
    }

    assert(bufUsed == 0);
  }
   
  @Override
  public synchronized void flush() throws IOException {
    checkOpen();
    flushBuffer(); // buffer -> libcephfs
    ceph.fsync(fileHandle); // libcephfs -> cluster
  }
  
  @Override
  public synchronized void close() throws IOException {
    checkOpen();
    flush();
    ceph.close(fileHandle);
    closed = true;
  }
}
