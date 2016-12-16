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
public class GradoopIdByteArrayWithHashCode implements
//  WritableComparable<GradoopIdByteArrayWithHashCode>,
  NormalizableKey<GradoopIdByteArrayWithHashCode>,
  CopyableValue<GradoopIdByteArrayWithHashCode> {

  /**
   * Byte  0 -  3: hash code
   * Byte  4 -  7: timestamp
   * Byte  8 - 10: machine identifier
   * Byte 11 - 12: process identifier
   * Byte 13 - 15: counter
   */
  private static final int ID_SIZE = 16;

  private static final int ID_OFFSET = 4;

  private byte[] bytes = new byte[ID_SIZE];

  public GradoopIdByteArrayWithHashCode() {}

  private GradoopIdByteArrayWithHashCode(byte[] bytes) {
    this.bytes = bytes;
  }

  public static GradoopIdByteArrayWithHashCode fromString(String s) {
    if (!ObjectId.isValid(s)) {
      throw new IllegalArgumentException(
        "invalid hexadecimal representation of a GradoopId: [" + s + "]");
    }

    byte[] b = new byte[ID_SIZE];

    for (int i = 0; i < b.length - ID_OFFSET; i++) {
      b[ID_OFFSET + i] = (byte) Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16);
    }

    // write hash code to byte[]
    int hashCode = computeHashCode(b);
    byte[] hashCodeBytes = makeBytes(hashCode);
    b[0] = hashCodeBytes[0];
    b[1] = hashCodeBytes[1];
    b[2] = hashCodeBytes[2];
    b[3] = hashCodeBytes[3];

    return new GradoopIdByteArrayWithHashCode(b);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    GradoopIdByteArrayWithHashCode other = (GradoopIdByteArrayWithHashCode) o;

//    // compare hash code (byte 0 to 3)
//    if (!equalsInRange(bytes, other.bytes, 0, 3)) {
//      return false;
//    }
    // compare counter (byte 9 to 11)
    if (!equalsInRange(bytes, other.bytes, ID_OFFSET + 9, ID_OFFSET + 11)) {
      return false;
    }
    // compare machine identifier (byte 4 to 6)
    if (!equalsInRange(bytes, other.bytes, ID_OFFSET + 4, ID_OFFSET + 6)) {
      return false;
    }
    // compare process identifier (byte 7 to 8)
    if (!equalsInRange(bytes, other.bytes, ID_OFFSET + 7, ID_OFFSET + 8)) {
      return false;
    }
    // compare timestamp (byte 0 to 3)
    if (!equalsInRange(bytes, other.bytes, ID_OFFSET, ID_OFFSET + 3)) {
      return false;
    }

    return true;
  }

  private boolean equalsInRange (byte[] f1, byte[] f2, int from, int to) {
    while (from <= to) {
      if (f1[from] != f2[from]) {
        return false;
      }
      ++from;
    }
    return true;
  }

  private static int computeHashCode(byte[] bytes) {
    int result = getTimeStamp(bytes);
    result = 31 * result + getMachineIdentifier(bytes);
    result = 31 * result + (int) getProcessIdentifier(bytes);
    result = 31 * result + getCounter(bytes);
    return result;
  }

  @Override
  public int hashCode() {
    return makeInt(bytes[0], bytes[1], bytes[2], bytes[3]);
  }

  @Override
  public int getMaxNormalizedKeyLen() {
    return 12;
  }

  @Override
  public void copyNormalizedKey(MemorySegment memory, int offset, int len) {
    memory.put(offset, bytes, 0, len);
  }

  @Override
  public int compareTo(GradoopIdByteArrayWithHashCode o) {
    byte[] otherByteArray = o.bytes;
    for (int i = ID_OFFSET; i < ID_SIZE; i++) {
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

//  @Override
//  public void write(DataOutput dataOutput) throws IOException {
//    dataOutput.write(bytes);
//  }
//
//  @Override
//  public void readFields(DataInput dataInput) throws IOException {
//    dataInput.readFully(bytes);
//  }

  private static int getTimeStamp(byte[] bytes) {
    return makeInt(bytes[ID_OFFSET], bytes[ID_OFFSET + 1], bytes[ID_OFFSET + 2], bytes[ID_OFFSET + 3]);
  }

  private static int getMachineIdentifier(byte[] bytes) {
    return makeInt((byte) 0, bytes[ID_OFFSET + 4], bytes[ID_OFFSET + 5], bytes[ID_OFFSET + 6]);
  }

  private static short getProcessIdentifier(byte[] bytes) {
    return (short) makeInt((byte) 0, (byte) 0, bytes[ID_OFFSET + 7], bytes[ID_OFFSET + 8]);
  }

  private static int getCounter(byte[] bytes) {
    return makeInt((byte) 0, bytes[ID_OFFSET + 9], bytes[ID_OFFSET + 10], bytes[ID_OFFSET + 11]);
  }

  private static int makeInt(final byte b3, final byte b2, final byte b1, final byte b0) {
    return (((b3) << 24) |
      ((b2 & 0xff) << 16) |
      ((b1 & 0xff) << 8) |
      ((b0 & 0xff)));
  }

  private static byte[] makeBytes(int i) {
    return new byte[] {
      (byte)(i >>> 24),
      (byte)(i >>> 16),
      (byte)(i >>> 8),
      (byte)i};
  }

  @Override
  public int getBinaryLength() {
    return ID_SIZE;
  }

  @Override
  public void copyTo(GradoopIdByteArrayWithHashCode target) {
    target.bytes = this.bytes;
  }

  @Override
  public GradoopIdByteArrayWithHashCode copy() {
    return new GradoopIdByteArrayWithHashCode(bytes);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    target.write(source, ID_SIZE);
  }
}
