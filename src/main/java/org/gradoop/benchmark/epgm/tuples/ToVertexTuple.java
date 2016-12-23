package org.gradoop.benchmark.epgm.tuples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.Vertex;

@FunctionAnnotation.ForwardedFields("id->f0;label->f1;properties->f2;graphIds->f3")
public class ToVertexTuple implements MapFunction<Vertex, VertexTuple> {

  @Override
  public VertexTuple map(Vertex v) throws Exception {
    return new VertexTuple(v.getId(), v.getLabel(), v.getProperties(), v.getGraphIds());
  }
}
