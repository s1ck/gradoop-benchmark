package org.gradoop.benchmark.epgm.tuples;

import org.apache.flink.api.java.tuple.Tuple6;

public class EdgeByteArrayTuple extends Tuple6<byte[], byte[], byte[], byte[], byte[], byte[]> {

  public EdgeByteArrayTuple() {
  }

  public EdgeByteArrayTuple(byte[] value0, byte[] value1, byte[] value2, byte[] value3, byte[] value4, byte[] value5) {
    super(value0, value1, value2, value3, value4, value5);
  }

  public byte[] getId() {
    return f0;
  }

  public void setId(byte[] id) {
    this.f0 = id;
  }

  public byte[] getSourceId() {
    return f1;
  }

  public void setSourceId(byte[] id) {
    this.f1 = id;
  }

  public byte[] getTargetId() {
    return f2;
  }

  public void setTargetId(byte[] id) {
    this.f2 = id;
  }

  public byte[] getLabel() {
    return f3;
  }

  public void setLabel(byte[] label) {
    this.f3 = label;
  }

  public byte[] getProperties() {
    return f4;
  }

  public void setProperties(byte[] properties) {
    this.f4 = properties;
  }

  public byte[] getGraphIds() {
    return f5;
  }

  public void setGraphIds(byte[] graphIds) {
    this.f5 = graphIds;
  }
}
