package org.apache.avro.ipc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Protocol;
import org.apache.avro.ipc.Transceiver;

public class AccountingTransceiver extends Transceiver {
  Transceiver xcvr;

  AtomicLong bytesWritten = new AtomicLong();

  public AccountingTransceiver(Transceiver xcvr) {
    this.xcvr = xcvr;
  }

  @Override
  public String getRemoteName() {
    return xcvr.getRemoteName();
  }

  // Transceive is explicitly excluded because it calls readBuffers and
  // writeBuffers virtual funcs.

  @Override
  public List<ByteBuffer> readBuffers() throws IOException {
    return xcvr.readBuffers();
  }

  @Override
  public void writeBuffers(List<ByteBuffer> arg0) throws IOException {
    long len = getLength(arg0); // must be done before writing them.
    xcvr.writeBuffers(arg0);
    bytesWritten.addAndGet(len);
  }

  @Override
  public boolean isConnected() {
    return xcvr.isConnected();
  }

  @Override
  public void setRemote(Protocol protocol) {
    xcvr.setRemote(protocol);
  }

  @Override
  public Protocol getRemote() {
    return xcvr.getRemote();
  }

  @Override
  public void close() throws IOException {
    xcvr.close();
  }

  public long getSentBytes() {
    return bytesWritten.get();
  }

  static int getLength(List<ByteBuffer> buffers) {
    int length = 0;
    for (ByteBuffer buffer : buffers) {
      length += 4;
      length += buffer.remaining();
    }
    length += 4;
    return length;
  }

  static long bufferLens(List<ByteBuffer> buffers) throws IOException {
    long len = getLength(buffers);
    return len + 4;
  }

}
