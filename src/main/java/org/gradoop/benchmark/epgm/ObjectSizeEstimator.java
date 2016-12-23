package org.gradoop.benchmark.epgm;

import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.benchmark.epgm.tuples.*;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Computes the average (estimated) vertex and edge object size in the given graph.
 */
public class ObjectSizeEstimator {

  public static void main(String[] args) throws Exception {
    String inputPath = args[0];

    DataSource jsonDataSource = new JSONDataSource(
      inputPath + "graphs.json",
      inputPath + "nodes.json",
      inputPath + "edges.json",
      GradoopFlinkConfig.createConfig(ExecutionEnvironment.getExecutionEnvironment()));

    DataSet<VertexPartialByteArrayTuple> vertices = jsonDataSource.getLogicalGraph().getVertices()
      .map(new ToVertexPartialByteArrayTuple());
    DataSet<EdgePartialByteArrayTuple> edges = jsonDataSource.getLogicalGraph().getEdges()
      .map(new ToEdgePartialByteArrayTuple());

    getAverageEstimatedObjectSize(vertices,"avg(ObjectSize(vertex))")
      .union(getAverageEstimatedObjectSize(edges, "avg(ObjectSize(edge))")).print();
  }

  /**
   * Computes the average object size of the elements in the specified dataset.
   *
   * @param dataSet dataset with objects to measure
   * @param prefix prefix for output
   * @param <T> type
   * @return dataset containing exactly one tuple: (prefix, average object size)
   */
  public static <T> DataSet<Tuple2<String, Double>> getAverageEstimatedObjectSize(DataSet<T> dataSet, final String prefix) {
    return dataSet
      .map(v -> Tuple2.of(ObjectSizeCalculator.getObjectSize(v), 1L))
      .returns(new TypeHint<Tuple2<Long, Long>>() {})
      .sum(0).andSum(1)
      .map(t -> Tuple2.of(prefix, t.f0 / (double) t.f1))
      .returns(new TypeHint<Tuple2<String, Double>>() {});
  }
}
