package org.gradoop.benchmark.transformer;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Converts a Gradoop EPGM graph from JSON to a representation where each vertex and edge is
 * represented by a {@link org.gradoop.common.model.impl.id.GradoopId} value.
 */
public class JsonGraphToGradoopIdGraph {

  public static void main(String[] args) throws Exception {
    String inputPath = args[0];
    String outputPath = args[1];

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSource jsonDataSource = new JSONDataSource(
      inputPath + "graphs.json",
      inputPath + "nodes.json",
      inputPath + "edges.json",
      GradoopFlinkConfig.createConfig(env));

    DataSet<Tuple1<GradoopId>> vertices = jsonDataSource.getLogicalGraph().getVertices()
      .map(v -> Tuple1.of(v.getId()))
      .returns(new TypeHint<Tuple1<GradoopId>>() {});

    DataSet<Tuple3<GradoopId, GradoopId, GradoopId>> edges = jsonDataSource.getLogicalGraph().getEdges()
      .map(e -> Tuple3.of(e.getId(), e.getSourceId(), e.getTargetId()))
      .returns(new TypeHint<Tuple3<GradoopId, GradoopId, GradoopId>>() {});

    vertices.writeAsCsv(outputPath + "vertices");
    edges.writeAsCsv(outputPath + "edges");

    env.execute();
  }
}
