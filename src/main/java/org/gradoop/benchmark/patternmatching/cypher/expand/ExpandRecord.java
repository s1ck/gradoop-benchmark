package org.gradoop.benchmark.patternmatching.cypher.expand;

import com.google.common.primitives.Longs;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.Value;

import java.io.IOException;
import java.util.Iterator;

public class ExpandRecord implements Value, CopyableValue<ExpandRecord>, Iterable<Long> {
  private byte[] path;

  public ExpandRecord() {
    this.path = new byte[0];
  }

  public ExpandRecord(byte[] path) {
    this.path = path;
  }

  public static ExpandRecord fromEdge(Tuple3<Long, Long, Long> edge) {
    byte[] newPath = new byte[3*Long.BYTES];
    System.arraycopy(Longs.toByteArray(edge.f0), 0, newPath,  0,   Long.BYTES);
    System.arraycopy(Longs.toByteArray(edge.f1), 0, newPath,  8,   Long.BYTES);
    System.arraycopy(Longs.toByteArray(edge.f2), 0, newPath,  16,  Long.BYTES);

    return new ExpandRecord(newPath);
  }

  public ExpandRecord expand(Tuple3<Long,Long,Long> edge) {
    byte[] newPath = new byte[path.length + 3*Long.BYTES];
    System.arraycopy(path, 0, newPath, 0, path.length);
    System.arraycopy(Longs.toByteArray(edge.f0), 0, newPath, path.length,      Long.BYTES);
    System.arraycopy(Longs.toByteArray(edge.f1), 0, newPath, path.length + 8,  Long.BYTES);
    System.arraycopy(Longs.toByteArray(edge.f2), 0, newPath, path.length + 16, Long.BYTES);

    return new ExpandRecord(newPath);
  }

  public int size() {
    return path.length / 8;
  }

  public long getId(int column) {
    return Longs.fromByteArray(
      ArrayUtils.subarray(path, column*Longs.BYTES, column*Long.BYTES + Long.BYTES)
    );
  }

  public byte[] getPath() {
    return path;
  }

  public void setPath(byte[] path) {
    this.path = path;
  }

  @Override
  public int getBinaryLength() {
    return -1;
  }

  @Override
  public void copyTo(ExpandRecord target) {
    if (target.path == null || target.path.length != this.path.length) {
      target.path = new byte[this.path.length];
    }
    System.arraycopy(this.path, 0, target.path, 0, this.path.length);
  }

  @Override
  public ExpandRecord copy() {
    ExpandRecord res = new ExpandRecord();
    copyTo(res);

    return res;
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    int length = source.readInt();
    target.writeInt(length);
    target.write(source, length);
  }

  @Override
  public void write(DataOutputView out) throws IOException {
    out.writeInt(this.path.length);
    out.write(this.path, 0, this.path.length);
  }

  @Override
  public void read(DataInputView in) throws IOException {
    final int length = in.readInt();
    byte[] buffer = this.path;
    if (buffer == null || buffer.length != length) {
      buffer = new byte[length];
      this.path =  buffer;
    }

    in.readFully(buffer);
  }

  @Override
  public Iterator<Long> iterator() {
    Iterator<Long> it = new Iterator<Long>() {
      private int currentOffset = 0;

      @Override
      public boolean hasNext() {
        return currentOffset < path.length;
      }

      @Override
      public Long next() {
        return Longs.fromByteArray(
          ArrayUtils.subarray(path , currentOffset, currentOffset += Long.BYTES)
        );
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }

    };
    return it;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("[");
    for (int i = 0; i<this.size(); i++) {
      builder.append(getId(i));
      builder.append(",");
    }
    builder.append("]");
    return builder.toString();
  }
}
