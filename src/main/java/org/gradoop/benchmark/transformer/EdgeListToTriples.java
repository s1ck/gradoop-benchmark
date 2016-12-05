package org.gradoop.benchmark.transformer;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Converts:
 *
 * sourceId targetId -> edgeId sourceId targetId
 *
 * Output can be either {@link Long} or {@link GradoopId}
 */
public class EdgeListToTriples {

  /**
   * run -c org.gradoop.Transformer gradoop.jar [long|gradoopid] inputPath outputPath
   *
   * @param args arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    if (args[0].toLowerCase().equals("long")) {
      transformToLong(args[1], args[2]);
    } else if (args[0].toLowerCase().equals("gradoopid")) {
      transformToGradoopId(args[1], args[2]);
    }
  }

  private static void transformToLong(String inputPath, String outputPath) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSet<Tuple2<Long, Long>> edgesFromFile = env.readCsvFile(inputPath)
      .ignoreComments("#")
      .fieldDelimiter("\t")
      .types(Long.class, Long.class);

    DataSet<Tuple3<Long, Long, Long>> edges = DataSetUtils
      .zipWithUniqueId(edgesFromFile)
      .map(value -> Tuple3.of(value.f1.f0, value.f0, value.f1.f1))
      .returns(new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO))
      .withForwardedFields("f0->f1;f1.f0->f0;f1.f1->f2");

    edges.writeAsCsv(outputPath, System.getProperty("line.separator"), "\t");
    env.execute();
  }

  private static void transformToGradoopId(String inputPath, String outputPath) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSet<Tuple2<Long, Long>> edges = env.readCsvFile(inputPath)
      .ignoreComments("#")
      .fieldDelimiter("\t")
      .types(Long.class, Long.class);

    // assign a unique long id to each edge tuple
    DataSet<Tuple2<Long, Tuple2<Long, Long>>> edgesWithId = DataSetUtils
      .zipWithUniqueId(edges);

    // transform to ImportEdge
    DataSet<ImportEdge<Long>> importEdges = edgesWithId
      .map(new MapFunction<Tuple2<Long, Tuple2<Long, Long>>, ImportEdge<Long>>() {

        private static final String EDGE_LABEL = "link";

        @Override
        public ImportEdge<Long> map(
          Tuple2<Long, Tuple2<Long, Long>> edgeTriple) throws Exception {
          return new ImportEdge<>(
            Long.parseLong(edgeTriple.f1.f0.toString() + edgeTriple.f1.f1.toString()), // edge id
            edgeTriple.f1.f0,   // source vertex id
            edgeTriple.f1.f1,  // target vertex id
            EDGE_LABEL);
        }
      }).withForwardedFields("f0;f1.f0->f1;f1.f1->f2");

    //--------------------------------------------------------------------------
    // Read vertices
    //--------------------------------------------------------------------------

    // extract vertex identifiers from edge tuples
    DataSet<Tuple1<Long>> vertices = edges
      .<Tuple1<Long>>project(0)
      .union(edges.<Tuple1<Long>>project(1))
      .distinct();

    // transform to ImportVertex
    DataSet<ImportVertex<Long>> importVertices = vertices
      .map(new MapFunction<Tuple1<Long>, ImportVertex<Long>>() {

        private static final String VERTEX_LABEL = "Node";

        @Override
        public ImportVertex<Long> map(Tuple1<Long> vertex) throws Exception {
          return new ImportVertex<>(
            vertex.f0, // vertex id
            VERTEX_LABEL);
        }
      }).withForwardedFields("f0");

    //--------------------------------------------------------------------------
    // Create logical graph
    //--------------------------------------------------------------------------

    // create default Gradoop config
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

    // create datasource
    org.gradoop.flink.io.api.DataSource dataSource = new GraphDataSource<>(
      importVertices, importEdges, config);

    TypeInformation<GradoopId> gradoopIdTypeInformation = TypeExtractor.getForClass(GradoopId.class);

    // read logical graph
    dataSource.getLogicalGraph().getEdges()
      .map(value -> Tuple3.of(value.getSourceId(), value.getId(), value.getTargetId()))
      .returns(new TupleTypeInfo<>(gradoopIdTypeInformation, gradoopIdTypeInformation, gradoopIdTypeInformation))
      .writeAsCsv(outputPath, System.getProperty("line.separator"), "\t");

    env.execute();
  }
}
