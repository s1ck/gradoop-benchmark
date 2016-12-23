package org.gradoop.benchmark.epgm.tuples;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.gradoop.common.model.impl.pojo.Edge;

import java.io.ByteArrayOutputStream;

public class ToEdgePartialByteArrayTuple extends RichMapFunction<Edge, EdgePartialByteArrayTuple> {

  private transient ByteArrayOutputStream byteStream;

  private transient DataOutputView dataOutputView;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    byteStream = new ByteArrayOutputStream();
    dataOutputView = new DataOutputViewStreamWrapper(byteStream);
  }

  @Override
  public EdgePartialByteArrayTuple map(Edge e) throws Exception {
    byteStream.reset();
    e.getProperties().write(dataOutputView);

    return new EdgePartialByteArrayTuple(
      e.getId(),
      e.getSourceId(),
      e.getTargetId(),
      e.getLabel().getBytes(),
      byteStream.toByteArray(),
      e.getGraphIds().toByteArray()
    );
  }
}
