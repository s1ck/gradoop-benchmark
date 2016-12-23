package org.gradoop.benchmark.epgm.tuples;

import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.common.model.impl.id.GradoopId;

public class EdgePartialByteArrayTuple extends Tuple6<GradoopId, GradoopId, GradoopId, byte[], byte[], byte[]> {

  public EdgePartialByteArrayTuple() {
  }

  public EdgePartialByteArrayTuple(GradoopId value0, GradoopId value1, GradoopId value2, byte[] value3, byte[] value4, byte[] value5) {
    super(value0, value1, value2, value3, value4, value5);
  }

  public GradoopId getId() {
    return f0;
  }

  public void setId(GradoopId id) {
    this.f0 = id;
  }

  public GradoopId getSourceId() {
    return f1;
  }

  public void setSourceId(GradoopId id) {
    this.f1 = id;
  }

  public GradoopId getTargetId() {
    return f2;
  }

  public void setTargetId(GradoopId id) {
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
