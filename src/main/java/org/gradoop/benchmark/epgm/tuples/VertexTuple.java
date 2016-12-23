package org.gradoop.benchmark.epgm.tuples;

import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.properties.Properties;

public class VertexTuple extends Tuple4<GradoopId, String, Properties, GradoopIdList> {

  public VertexTuple() {
  }

  public VertexTuple(GradoopId value0, String value1, Properties value2, GradoopIdList value3) {
    super(value0, value1, value2, value3);
  }

  public GradoopId getId() {
    return f0;
  }

  public void setId(GradoopId id) {
    this.f0 = id;
  }

  public String getLabel() {
    return f1;
  }

  public void setLabel(String label) {
    this.f1 = label;
  }

  public Properties getProperties() {
    return f2;
  }

  public void setProperties(Properties properties) {
    this.f2 = properties;
  }

  public GradoopIdList getGraphIds() {
    return f3;
  }

  public void setGraphIds(GradoopIdList graphIds) {
    this.f3 = graphIds;
  }
}
