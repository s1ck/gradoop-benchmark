package org.gradoop.benchmark.patternmatching.cypher.join;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.Value;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class EmbeddingRecord implements Value, CopyableValue<EmbeddingRecord> {
  private byte[] stream;
  private int binaryLen;

  public EmbeddingRecord(byte[] stream) {
    this.stream = stream;
    this.binaryLen = stream.length;
  }

  public EmbeddingRecord(int arity) {
    this.stream = new byte[8*arity];
    this.binaryLen = stream.length;
  }

  public EmbeddingRecord() {
    this(0);
  }

  public void set(int pos, Long value) {
    int offset = pos * 8;
    byte[] bytes = Longs.toByteArray(value);
    System.arraycopy(bytes, 0, stream, offset, 8);
  }

  public Long get(int pos) {
    int offset = pos*8;
    return Bytes.toLong(ArrayUtils.subarray(stream, offset, offset+8));
  }

  public int size() {
    return binaryLen / 8;
  }

  public void copyFrom(EmbeddingRecord src, int srcIndex, int targetIndex) {
    int srcOffset = srcIndex * 8;
    int targetOffset = targetIndex * 8;

    System.arraycopy(src.stream, srcOffset, stream, targetOffset, 8);
  }

  @Override
  public int getBinaryLength() {
    return -1;
  }

  @Override
  public void copyTo(EmbeddingRecord target) {
    if (target.stream == null || target.stream.length < this.binaryLen) {
			target.stream = new byte[this.binaryLen];
      target.binaryLen = this.binaryLen;
    }
    System.arraycopy(this.stream, 0, target.stream, 0, this.binaryLen);
  }

  @Override
  public EmbeddingRecord copy() {
    EmbeddingRecord res = new EmbeddingRecord(this.binaryLen);
    copyTo(res);

    return res;
  }

  public long serialize(DataOutputView target) throws IOException {

		target.write(Ints.toByteArray(this.binaryLen),0, Integer.BYTES);
		target.write(this.stream, 0, this.binaryLen);

		return Integer.BYTES + this.binaryLen;
	}

  public void deserialize(DataInputView source) throws IOException {
		read(source);
	}

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    int val = source.readUnsignedByte();
    target.writeByte(val);

    if (val >= MAX_BIT) {
      int shift = 7;
      int curr;
      val = val & 0x7f;
      while ((curr = source.readUnsignedByte()) >= MAX_BIT) {
        target.writeByte(curr);
        val |= (curr & 0x7f) << shift;
        shift += 7;
      }
      target.writeByte(curr);
      val |= curr << shift;
    }

    target.write(source, val);
  }

  @Override
  public void write(DataOutputView out) throws IOException {
    out.write(Ints.toByteArray(this.binaryLen),0, Integer.BYTES);
		out.write(this.stream, 0, this.binaryLen);
  }

  @Override
  public void read(DataInputView in) throws IOException {
    final int len = in.readInt();
		this.binaryLen = len;
    
		byte[] data = new byte[len];
		in.readFully(data, 0, len);
    this.stream  = data;
  }

	private static final int readVarLengthInt(DataInput in) throws IOException {
		// read first byte
		int val = in.readUnsignedByte();
		if (val >= MAX_BIT) {
			int shift = 7;
			int curr;
			val = val & 0x7f;
			while ((curr = in.readUnsignedByte()) >= MAX_BIT) {
				val |= (curr & 0x7f) << shift;
				shift += 7;
			}
			val |= curr << shift;
		}
		return val;
	}

	private static final int MAX_BIT = 0x1 << 7;

  public String toString() {
    String str = "[ ";
    for (int i = 0; i < size(); i++) {
      str += get(i) +", ";
    }
    str += "]";
    return str;
  }
}
