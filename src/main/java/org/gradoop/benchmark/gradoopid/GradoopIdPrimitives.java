package org.gradoop.benchmark.gradoopid;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.types.NormalizableKey;
import org.apache.hadoop.io.WritableComparable;
import org.bson.types.ObjectId;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Wraps a the primitive components representing a BSON {@link ObjectId}.
 */
public class GradoopIdPrimitives implements
  WritableComparable<GradoopIdPrimitives>,
  NormalizableKey<GradoopIdPrimitives> {

  private int timestamp;

  private int machineIdentifier;

  private short processIdentifier;

  private int counter;

  public GradoopIdPrimitives() {}

  private GradoopIdPrimitives(int timestamp, int machineIdentifier, short processIdentifier, int counter) {
    this.timestamp = timestamp;
    this.machineIdentifier = machineIdentifier;
    this.processIdentifier = processIdentifier;
    this.counter = counter;
  }

  public static GradoopIdPrimitives get() {
    ObjectId objectId = new ObjectId();
    return new GradoopIdPrimitives(
      objectId.getTimestamp(),
      objectId.getMachineIdentifier(),
      objectId.getProcessIdentifier(),
      objectId.getCounter());
  }

  public static GradoopIdPrimitives fromString(String s) {
    ObjectId objectId = new ObjectId(s);
    return new GradoopIdPrimitives(
      objectId.getTimestamp(),
      objectId.getMachineIdentifier(),
      objectId.getProcessIdentifier(),
      objectId.getCounter());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    GradoopIdPrimitives other = (GradoopIdPrimitives) o;

    if (counter != other.counter) {
      return false;
    }
    if (machineIdentifier != other.machineIdentifier) {
      return false;
    }
    if (processIdentifier != other.processIdentifier) {
      return false;
    }
    if (timestamp != other.timestamp) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = timestamp;
    result = 31 * result + machineIdentifier;
    result = 31 * result + (int) processIdentifier;
    result = 31 * result + counter;
    return result;
  }

  @Override
  public int getMaxNormalizedKeyLen() {
    return 12;
  }

  @Override
  public void copyNormalizedKey(MemorySegment memory, int offset, int len) {
    memory.put(offset, toByteArray(), 0, len);
  }

  @Override
  public int compareTo(GradoopIdPrimitives o) {
    if (o == null) {
      throw new NullPointerException();
    }

    byte[] byteArray = toByteArray();
    byte[] otherByteArray = o.toByteArray();
    for (int i = 0; i < 12; i++) {
      if (byteArray[i] != otherByteArray[i]) {
        return ((byteArray[i] & 0xff) < (otherByteArray[i] & 0xff)) ? -1 : 1;
      }
    }
    return 0;
  }

  @Override
  public void write(DataOutputView out) throws IOException {
    out.write(toByteArray());
  }

  @Override
  public void read(DataInputView in) throws IOException {
    timestamp = makeInt(in.readByte(), in.readByte(), in.readByte(), in.readByte());
    machineIdentifier = makeInt((byte) 0, in.readByte(), in.readByte(), in.readByte());
    processIdentifier = (short) makeInt((byte) 0, (byte) 0, in.readByte(), in.readByte());
    counter = makeInt((byte) 0, in.readByte(), in.readByte(), in.readByte());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.write(toByteArray());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    timestamp = makeInt(in.readByte(), in.readByte(), in.readByte(), in.readByte());
    machineIdentifier = makeInt((byte) 0, in.readByte(), in.readByte(), in.readByte());
    processIdentifier = (short) makeInt((byte) 0, (byte) 0, in.readByte(), in.readByte());
    counter = makeInt((byte) 0, in.readByte(), in.readByte(), in.readByte());
  }

  private static int makeInt(final byte b3, final byte b2, final byte b1, final byte b0) {
    return (((b3) << 24) |
      ((b2 & 0xff) << 16) |
      ((b1 & 0xff) << 8) |
      ((b0 & 0xff)));
  }

  private byte[] toByteArray() {
    byte[] bytes = new byte[12];
    bytes[0] = int3(timestamp);
    bytes[1] = int2(timestamp);
    bytes[2] = int1(timestamp);
    bytes[3] = int0(timestamp);
    bytes[4] = int2(machineIdentifier);
    bytes[5] = int1(machineIdentifier);
    bytes[6] = int0(machineIdentifier);
    bytes[7] = short1(processIdentifier);
    bytes[8] = short0(processIdentifier);
    bytes[9] = int2(counter);
    bytes[10] = int1(counter);
    bytes[11] = int0(counter);
    return bytes;
  }

  private static byte int3(final int x) {
    return (byte) (x >> 24);
  }

  private static byte int2(final int x) {
    return (byte) (x >> 16);
  }

  private static byte int1(final int x) {
    return (byte) (x >> 8);
  }

  private static byte int0(final int x) {
    return (byte) (x);
  }

  private static byte short1(final short x) {
    return (byte) (x >> 8);
  }

  private static byte short0(final short x) {
    return (byte) (x);
  }
}
