package org.gradoop.benchmark.patternmatching.cypher.join;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.Value;

import java.io.IOException;

@java.lang.SuppressWarnings("ALL")
public class EmbeddingRecord implements Value, CopyableValue<EmbeddingRecord> {
  public static final transient byte ID_ENTRY_TYPE = 0x00;
  public static final transient byte PROJECTION_ENTRY_TYPE = 0x01;
  public static final transient byte LIST_ENTRY_TYPE = 0x02;

  private byte[] data;
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

  //public void add(long id, List<PropertyValue> properties) {
  //  int size = 13 + properties.stream().mapToInt(PropertyValue::getByteSize).sum() + properties.size() * 4;
  //  byte[] entry = new byte[size];
  //
  //  System.arraycopy(Ints.toByteArray(size),0,entry,0, 4);
  //  entry[4] = PROJECTION_ENTRY_TYPE;
  //  System.arraycopy(Longs.toByteArray(id),0,entry,5,8);
  //
  //  int offset = 13;
  //  for (PropertyValue property : properties) {
  //    System.arraycopy(Ints.toByteArray(property.getByteSize()), 0, entry, offset, 4);
  //    System.arraycopy(property.getRawBytes(), 0, entry, offset+4, property.getByteSize());
  //    offset += (property.getByteSize() + 4);
  //  }
  //
  //  this.data = org.apache.commons.lang.ArrayUtils.addAll(data, entry);
  //  this.size++;
  //}

  public void add(long[] ids) {
    int size = 5+8*ids.length;
    byte[] entry = new byte[size];

    System.arraycopy(Ints.toByteArray(size),0,entry,0, 4);
    entry[4] = LIST_ENTRY_TYPE;

    int offset = 5;
    for(long id : ids) {
      System.arraycopy(Longs.toByteArray(id), 0, entry, offset, 8);
      offset += 8;
    }

    this.data = org.apache.commons.lang.ArrayUtils.addAll(data, entry);
    this.size++;
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

  //public PropertyValue getProperty(int column, int propertyIndex) {
  //  int offset = getOffset(column);
  //
  //  if(data[offset+4] != PROJECTION_ENTRY_TYPE) {
  //    throw new UnsupportedOperationException("Can't return properties for non ProjectionEntry");
  //  }
  //
  //  int end = offset + Ints.fromByteArray(
  //    org.apache.commons.lang.ArrayUtils.subarray(data, offset, offset + 4));
  //
  //  offset += 13;
  //  int i = 0;
  //  while(i < propertyIndex && offset < end) {
  //    offset += Ints.fromByteArray(
  //      org.apache.commons.lang.ArrayUtils.subarray(data, offset, offset+4)) + 4;
  //    i++;
  //  }
  //
  //  if(offset < end) {
  //    int propertySize = Ints.fromByteArray(
  //      org.apache.commons.lang.ArrayUtils.subarray(data, offset, offset + 4));
  //    return PropertyValue.fromRawBytes(
  //        org.apache.commons.lang.ArrayUtils.subarray(data, offset + 4, offset + 4 + propertySize)
  //    );
  //  } else {
  //    return PropertyValue.NULL_VALUE;
  //  }
  //}

  public long[] getListEntry(int column) {
    int offset = getOffset(column);
    int count = (Ints.fromByteArray(
      org.apache.commons.lang.ArrayUtils.subarray(data, offset, offset + 4)) - 5) / 8;

    if(data[offset+4] != LIST_ENTRY_TYPE) {
      throw new UnsupportedOperationException("Can't return ListEntry for non ListEntry");
    }

    offset += 5;

    long[] ids = new long[count];

    for(int i = 0; i < count; i++) {
      ids[i] = Longs.fromByteArray(
        org.apache.commons.lang.ArrayUtils.subarray(data,offset, offset + 8));
      offset += 8;
    }

    return ids;
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

  public long serialize(DataOutputView target) throws IOException {
    target.write(Ints.toByteArray(this.size),0, Integer.BYTES);
    target.write(Ints.toByteArray(this.data.length),0, Integer.BYTES);
    target.write(this.data, 0, this.data.length);

    return 2*Integer.BYTES + this.data.length;
  }

  public void deserialize(DataInputView source) throws IOException {
    read(source);
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
    serialize(out);
  }

  @Override
  public void read(DataInputView in) throws IOException {
    this.size = in.readInt();

    final int length = in.readInt();
    byte[] buffer = new byte[length];
    in.readFully(data, 0, length);
    this.data  = buffer;
  }
}
