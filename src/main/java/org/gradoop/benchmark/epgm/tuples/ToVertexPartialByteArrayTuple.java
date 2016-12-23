package org.gradoop.benchmark.epgm.tuples;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.io.ByteArrayOutputStream;

public class ToVertexPartialByteArrayTuple extends RichMapFunction<Vertex, VertexPartialByteArrayTuple> {

  private transient ByteArrayOutputStream byteStream;

  private transient DataOutputView dataOutputView;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    byteStream = new ByteArrayOutputStream();
    dataOutputView = new DataOutputViewStreamWrapper(byteStream);
  }

  @Override
  public VertexPartialByteArrayTuple map(Vertex v) throws Exception {
    byteStream.reset();
    v.getProperties().write(dataOutputView);

    return new VertexPartialByteArrayTuple(
      v.getId(),
      v.getLabel().getBytes(),
      byteStream.toByteArray(),
      v.getGraphIds().toByteArray()
    );
  }
}
