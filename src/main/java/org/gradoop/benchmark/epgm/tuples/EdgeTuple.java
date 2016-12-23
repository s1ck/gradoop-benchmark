package org.gradoop.benchmark.epgm.tuples;

import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.properties.Properties;

public class EdgeTuple
  extends Tuple6<GradoopId, GradoopId, GradoopId, String, Properties, GradoopIdList> {

  public EdgeTuple() {
  }

  public EdgeTuple(GradoopId value0, GradoopId value1, GradoopId value2, String value3,
    Properties value4, GradoopIdList value5) {
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

  public String getLabel() {
    return f3;
  }

  public void setLabel(String label) {
    this.f3 = label;
  }

  public org.gradoop.common.model.impl.properties.Properties getProperties() {
    return f4;
  }

  public void setProperties(org.gradoop.common.model.impl.properties.Properties properties) {
    this.f4 = properties;
  }

  public GradoopIdList getGraphIds() {
    return f5;
  }

  public void setGraphIds(GradoopIdList graphIds) {
    this.f5 = graphIds;
  }
}
