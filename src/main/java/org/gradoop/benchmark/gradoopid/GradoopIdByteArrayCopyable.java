package org.gradoop.benchmark.gradoopid;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.NormalizableKey;
import org.apache.hadoop.io.WritableComparable;
import org.bson.types.ObjectId;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Wraps a byte array representing a BSON {@link ObjectId}.
 */
public class GradoopIdByteArrayCopyable implements
  WritableComparable<GradoopIdByteArrayCopyable>,
  NormalizableKey<GradoopIdByteArrayCopyable>,
  CopyableValue<GradoopIdByteArrayCopyable> {

  private byte[] bytes = new byte[12];

  public GradoopIdByteArrayCopyable() {}

  private GradoopIdByteArrayCopyable(byte[] bytes) {
    this.bytes = bytes;
  }

  public static GradoopIdByteArrayCopyable get() {
    return new GradoopIdByteArrayCopyable(new ObjectId().toByteArray());
  }

  public static GradoopIdByteArrayCopyable fromString(String s) {
    return new GradoopIdByteArrayCopyable(new ObjectId(s).toByteArray());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    GradoopIdByteArrayCopyable other = (GradoopIdByteArrayCopyable) o;

    if (getCounter() != other.getCounter()) {
      return false;
    }
    if (getMachineIdentifier() != other.getMachineIdentifier()) {
      return false;
    }
    if (getProcessIdentifier() != other.getProcessIdentifier()) {
      return false;
    }
    if (getTimeStamp() != other.getTimeStamp()) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = getTimeStamp();
    result = 31 * result + getMachineIdentifier();
    result = 31 * result + (int) getProcessIdentifier();
    result = 31 * result + getCounter();
    return result;
  }

  @Override
  public int getMaxNormalizedKeyLen() {
    return 0;
  }

  @Override
  public void copyNormalizedKey(MemorySegment memory, int offset, int len) {
    memory.put(offset, bytes, 0, len);
  }

  @Override
  public int compareTo(GradoopIdByteArrayCopyable o) {
    if (o == null) {
      throw new NullPointerException();
    }

    byte[] otherByteArray = o.bytes;
    for (int i = 0; i < 12; i++) {
      if (bytes[i] != otherByteArray[i]) {
        return ((bytes[i] & 0xff) < (otherByteArray[i] & 0xff)) ? -1 : 1;
      }
    }
    return 0;
  }

  @Override
  public void write(DataOutputView out) throws IOException {
    out.write(bytes);
  }

  @Override
  public void read(DataInputView in) throws IOException {
    in.readFully(bytes);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.write(bytes);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    dataInput.readFully(bytes);
  }

  private int getTimeStamp() {
    return makeInt(bytes[0], bytes[1], bytes[2], bytes[3]);
  }

  private int getMachineIdentifier() {
    return makeInt((byte) 0, bytes[4], bytes[5], bytes[6]);
  }

  private short getProcessIdentifier() {
    return (short) makeInt((byte) 0, (byte) 0, bytes[7], bytes[8]);
  }

  private int getCounter() {
    return makeInt((byte) 0, bytes[9], bytes[10], bytes[11]);
  }

  private static int makeInt(final byte b3, final byte b2, final byte b1, final byte b0) {
    return (((b3) << 24) |
      ((b2 & 0xff) << 16) |
      ((b1 & 0xff) << 8) |
      ((b0 & 0xff)));
  }

  @Override
  public int getBinaryLength() {
    return 12;
  }

  @Override
  public void copyTo(GradoopIdByteArrayCopyable target) {
    target.bytes = this.bytes;
  }

  @Override
  public GradoopIdByteArrayCopyable copy() {
    return new GradoopIdByteArrayCopyable(bytes);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    target.write(source, 12);
  }
}
