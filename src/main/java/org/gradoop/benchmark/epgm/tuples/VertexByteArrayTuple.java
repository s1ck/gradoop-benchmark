package org.gradoop.benchmark.epgm.tuples;

import org.apache.flink.api.java.tuple.Tuple4;

public class VertexByteArrayTuple extends Tuple4<byte[], byte[], byte[], byte[]> {

  public VertexByteArrayTuple() {
  }

  public VertexByteArrayTuple(byte[] value0, byte[] value1, byte[] value2, byte[] value3) {
    super(value0, value1, value2, value3);
  }

  public byte[] getId() {
    return f0;
  }

  public void setId(byte[] id) {
    this.f0 = id;
  }

  public byte[] getLabel() {
    return f1;
  }

  public void setLabel(byte[] label) {
    this.f1 = label;
  }

  public byte[] getProperties() {
    return f2;
  }

  public void setProperties(byte[] properties) {
    this.f2 = properties;
  }

  public byte[] getGraphIds() {
    return f3;
  }

  public void setGraphIds(byte[] graphIds) {
    this.f3 = graphIds;
  }
}
