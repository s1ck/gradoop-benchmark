package org.gradoop.benchmark.transformer;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Converts a Gradoop EPGM graph from JSON to a representation where each vertex and edge is
 * represented by a {@link Long} value.
 */
public class JsonGraphToLongGraph {

  public static void main(String[] args) throws Exception {
    String inputPath = args[0];
    String outputPath = args[1];

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSource jsonDataSource = new JSONDataSource(
      inputPath + "graphs.json",
      inputPath + "nodes.json",
      inputPath + "edges.json",
      GradoopFlinkConfig.createConfig(env));

    DataSet<Tuple2<Long, GradoopId>> vertexIds =
      DataSetUtils.zipWithUniqueId(jsonDataSource.getLogicalGraph()
        .getVertices()
        .map(new Id<>()));

    DataSet<Tuple3<Long, GradoopId, GradoopId>> edgeIds =
      DataSetUtils.zipWithUniqueId(jsonDataSource.getLogicalGraph()
        .getEdges()
        .map(e -> Tuple3.of(e.getId(), e.getSourceId(), e.getTargetId()))
        .returns(new TypeHint<Tuple3<GradoopId, GradoopId, GradoopId>>() {})
      )
      .map(t -> Tuple3.of(t.f0, t.f1.f1, t.f1.f2))
      .returns(new TypeHint<Tuple3<Long, GradoopId, GradoopId>>() {});


    DataSet<Tuple3<Long, Long, GradoopId>> partialEdges = vertexIds
      .join(edgeIds)
      .where(1).equalTo(1)
      .with((first, second) -> Tuple3.of(second.f0, first.f0, second.f2))
      .returns(new TypeHint<Tuple3<Long, Long, GradoopId>>() {});

    DataSet<Tuple3<Long, Long, Long>> newEdges = vertexIds
      .join(partialEdges)
      .where(1).equalTo(2)
      .with((first, second) -> Tuple3.of(second.f0, second.f1, first.f0))
      .returns(new TypeHint<Tuple3<Long, Long, Long>>() {});

    DataSet<Tuple1<Long>> newVertices = vertexIds.project(0);

    newVertices.writeAsCsv(outputPath + "vertices");
    newEdges.writeAsCsv(outputPath + "edges");

    env.execute();
  }
}
