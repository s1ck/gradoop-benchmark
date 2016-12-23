package org.gradoop.benchmark.epgm.tuples;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.gradoop.common.model.impl.pojo.Edge;

import java.io.ByteArrayOutputStream;

public class ToEdgeByteArrayTuple extends RichMapFunction<Edge, EdgeByteArrayTuple> {

  private transient ByteArrayOutputStream byteStream;

  private transient DataOutputView dataOutputView;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    byteStream = new ByteArrayOutputStream();
    dataOutputView = new DataOutputViewStreamWrapper(byteStream);
  }

  @Override
  public EdgeByteArrayTuple map(Edge e) throws Exception {
    byteStream.reset();
    e.getProperties().write(dataOutputView);

    return new EdgeByteArrayTuple(
      e.getId().toByteArray(),
      e.getSourceId().toByteArray(),
      e.getTargetId().toByteArray(),
      e.getLabel().getBytes(),
      byteStream.toByteArray(),
      e.getGraphIds().toByteArray()
    );
  }
}
