package org.gradoop.benchmark.epgm.tuples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.Edge;

@FunctionAnnotation.ForwardedFields("id->f0;sourceId->f1;targetId->f2;label->f3;properties->f4;graphIds->f5")
public class ToEdgeTuple implements MapFunction<Edge, EdgeTuple> {

  @Override
  public EdgeTuple map(Edge e) throws Exception {
    return new EdgeTuple(e.getId(), e.getSourceId(), e.getTargetId(), e.getLabel(), e.getProperties(), e.getGraphIds());
  }
}
