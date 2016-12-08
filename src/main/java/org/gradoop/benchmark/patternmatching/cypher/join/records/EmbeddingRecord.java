package org.gradoop.benchmark.patternmatching.cypher.join.records;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.Value;

import java.io.IOException;
import java.util.Iterator;

@java.lang.SuppressWarnings("ALL")
public class EmbeddingRecord implements Value, CopyableValue<EmbeddingRecord>, Iterable<byte[]> {
  public static final transient byte ID_ENTRY_TYPE = 0x00;
  public static final transient byte PROJECTION_ENTRY_TYPE = 0x01;
  public static final transient byte LIST_ENTRY_TYPE = 0x02;

  public byte[] data;
  private int size;

  public EmbeddingRecord() {
    this.data = new byte[0];
    this.size = 0;
  }

  public EmbeddingRecord(byte[] data, int size) {
    this.data = data;
    this.size = size;
  }

  public void add(long id) {
    byte[] entry = new byte[13];
    System.arraycopy(Ints.toByteArray(13),0,entry,0, 4);
    entry[4] = ID_ENTRY_TYPE;
    System.arraycopy(Longs.toByteArray(id),0,entry,5,8);

    this.data = org.apache.commons.lang.ArrayUtils.addAll(data, entry);
    size++;
  }


  public int size() {
    return this.size;
  }

  public byte[] getRawEntry(int column) {
    int offset = getOffset(column);
    int size = Ints.fromByteArray(
      org.apache.commons.lang.ArrayUtils.subarray(data, offset, offset+4)
    );

    return ArrayUtils.subarray(data,offset, offset+size);
  }

  public long getId(int column) {
    return Longs.fromByteArray(getRawId(column));
  }

  public byte[] getRawId(int column) {
    int offset = getOffset(column);

    if(data[offset+4] == LIST_ENTRY_TYPE) {
      throw new UnsupportedOperationException("Can't return id for ListEntry");
    }

    return org.apache.commons.lang.ArrayUtils.subarray(data, offset+5, offset+13);
  }

  private int getOffset(int column) {
    checkColumn(column);

    int offset = 0;
    for(int i = 0; i < column; i++) {
      offset += Ints.fromByteArray(
        org.apache.commons.lang.ArrayUtils.subarray(data, offset, offset+4)
      );
    }

    return offset;
  }

  private void checkColumn(int column) {
    if (column < 0) {
      throw new IndexOutOfBoundsException("Negative columns are not allowed");
    }
    if (column >= this.size) {
      throw new IndexOutOfBoundsException(column + " >= " + this.size());
    }
  }

  @Override
  public int getBinaryLength() {
    return -1;
  }

  @Override
  public void copyTo(EmbeddingRecord target) {
    if (target.data == null || target.data.length < this.data.length) {
      target.data = new byte[this.data.length];
      target.size = this.size;
    }
    System.arraycopy(this.data, 0, target.data, 0, this.data.length);
  }

  @Override
  public EmbeddingRecord copy() {
    EmbeddingRecord res = new EmbeddingRecord();
    copyTo(res);

    return res;
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    byte[] integer = new byte[4];
    source.read(integer,0,4);
    target.write(integer);

    source.read(integer,0,4);
    target.write(integer);
    int size = Ints.fromByteArray(integer);

    target.write(source, size);
  }

  @Override
  public void write(DataOutputView out) throws IOException {
    out.write(Ints.toByteArray(this.size));
    out.write(Ints.toByteArray(this.data.length));
    out.write(this.data, 0, this.data.length);
  }

  @Override
  public void read(DataInputView in) throws IOException {
    this.size = in.readInt();

    final int length = in.readInt();

    byte[] buffer = this.data;
    if (buffer == null || buffer.length < length) {
      buffer = new byte[length];
      this.data =  buffer;
    }

    in.readFully(buffer);
  }

  @Override
  public String toString() {
    String res = "[ ";
    for (int i=0; i<this.size(); i++) {
      res += getId(i) + ", ";
    }
    res += "]";
    return res;
  }

  @Override
  public Iterator<byte[]> iterator() {
    Iterator<byte[]> it = new Iterator<byte[]>() {
      private int currentOffset = 0;

      @Override
      public boolean hasNext() {
        return currentOffset < data.length;
      }

      @Override
      public byte[] next() {
        int size = Ints.fromByteArray(ArrayUtils.subarray(data, currentOffset, currentOffset + 4));
        byte[] res = ArrayUtils.subarray(data, currentOffset, currentOffset + size);
        currentOffset += size;
        return res;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }

    };
    return it;
  }
}