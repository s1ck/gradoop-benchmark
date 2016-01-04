package org.gradoop.benchmark;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.EPGMDatabase;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.summarization.Summarization;
import org.gradoop.model.impl.operators.summarization.SummarizationStrategy;
import org.gradoop.model.impl.operators.summarization.functions.aggregation
  .CountAggregator;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.properties.PropertyList;
import org.gradoop.util.GradoopFlinkConfig;
import org.s1ck.ldbc.LDBCToFlink;
import org.s1ck.ldbc.tuples.LDBCEdge;
import org.s1ck.ldbc.tuples.LDBCVertex;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

public class LDBCConverter implements ProgramDescription {

  public static final String NODES_JSON = "nodes.json";
  public static final String EDGES_JSON = "edges.json";
  public static final String GRAPHS_JSON = "graphs.json";

  public static void main(String[] args) throws Exception {
    final String task = args[0];
    final String inputDir = args[1];

    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    final LDBCToFlink ldbcToFlink = new LDBCToFlink(inputDir, env);

    switch (task) {
    case "label_stats" :
      printLabelStatistics(ldbcToFlink);
      break;
    case "convert" :
      convertToGradoop(ldbcToFlink, env, args[2]);
      break;
    case "summarize" :
      summarize(env, inputDir, args[2]);
      break;
    }
  }

  public static void printLabelStatistics(LDBCToFlink ldbcToFlink) throws
    Exception {
    ldbcToFlink.getVertices()
      .map(new MapFunction<LDBCVertex, Tuple2<String, Integer>>() {
        public Tuple2<String, Integer> map(LDBCVertex ldbcVertex) throws
          Exception {
          return new Tuple2<>(ldbcVertex.getLabel(), 1);
        }
      })
      .groupBy(0)
      .sum(1)
      .print();
  }

  public static void convertToGradoop(LDBCToFlink ldbcToFlink, ExecutionEnvironment env, String outputDir) throws
    Exception {
    GradoopFlinkConfig<GraphHeadPojo, VertexPojo, EdgePojo> gradoopConf =
      GradoopFlinkConfig.createDefaultConfig(env);

    MapOperator<LDBCVertex, Tuple2<Long, VertexPojo>> vertexMap =
      ldbcToFlink.getVertices()
        .map(new LDBC2GradoopVertex(gradoopConf.getVertexFactory()));

    DataSet<VertexPojo> vertices = vertexMap.map(
      new MapFunction<Tuple2<Long, VertexPojo>, VertexPojo>() {
        @Override
        public VertexPojo map(Tuple2<Long, VertexPojo> longVertexPojoTuple2)
          throws
          Exception {
          return longVertexPojoTuple2.f1;
        }
      }).withForwardedFields("f1->*");

    DataSet<EdgePojo> edges = vertexMap
      .join(ldbcToFlink.getEdges()).where(0).equalTo(2).with(
        new JoinFunction<Tuple2<Long, VertexPojo>, LDBCEdge, Tuple2<GradoopId,
          LDBCEdge>>() {

          @Override
          public Tuple2<GradoopId, LDBCEdge> join(
            Tuple2<Long, VertexPojo> vertexMapEntry, LDBCEdge ldbcEdge) throws
            Exception {
            return new Tuple2<>(vertexMapEntry.f1.getId(), ldbcEdge);
          }
        })
      .join(vertexMap)
      .where("1.3").equalTo(0)
      .with(new LDBC2GradoopEdge(gradoopConf.getEdgeFactory()));

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> logicalGraph =
      LogicalGraph.fromDataSets(vertices, edges, gradoopConf);

    logicalGraph.writeAsJson(
      outputDir + NODES_JSON,
      outputDir + EDGES_JSON,
      outputDir + GRAPHS_JSON
    );
  }

  @SuppressWarnings("unchecked")
  public static void summarize(ExecutionEnvironment env, String inputDir, String outputDir) throws
    Exception {
    GradoopFlinkConfig<GraphHeadPojo, VertexPojo, EdgePojo> gradoopConf =
      GradoopFlinkConfig.createDefaultConfig(env);

    EPGMDatabase<GraphHeadPojo, VertexPojo, EdgePojo> epgmDatabase =
      EPGMDatabase.fromJsonFile(
        inputDir + NODES_JSON,
        inputDir + EDGES_JSON,
        inputDir + GRAPHS_JSON,
        gradoopConf
      );

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> summaryGraph =
      epgmDatabase.getDatabaseGraph().callForGraph(
        new Summarization.SummarizationBuilder<GraphHeadPojo, VertexPojo, EdgePojo>()
          .setStrategy(SummarizationStrategy.GROUP_REDUCE)
          .useVertexLabel(true)
          .useEdgeLabel(true)
          .setVertexValueAggregator(new CountAggregator())
          .setEdgeValueAggregator(new CountAggregator())
          .build());

    summaryGraph.writeAsJson(
      outputDir + NODES_JSON,
      outputDir + EDGES_JSON,
      outputDir + GRAPHS_JSON
    );
  }

  public static final class LDBC2GradoopEdge implements JoinFunction<Tuple2<GradoopId, LDBCEdge>, Tuple2<Long,VertexPojo>, EdgePojo> {

    private final EPGMEdgeFactory<EdgePojo> factory;

    public LDBC2GradoopEdge(EPGMEdgeFactory<EdgePojo> factory) {
      this.factory = factory;
    }

    @Override
    public EdgePojo join(
      Tuple2<GradoopId, LDBCEdge> edge,
      Tuple2<Long, VertexPojo> vertexMapEntry) throws Exception {
      String label        = edge.f1.getLabel();
      GradoopId sourceId  = edge.f0;
      GradoopId targetId  = vertexMapEntry.f1.getId();
      PropertyList props  = new PropertyList();

      for (Map.Entry<String, Object> edgeProp : edge.f1.getProperties()
        .entrySet()) {
        String key    = edgeProp.getKey();
        Object value  = edgeProp.getValue();

        if (value instanceof Date) {
          value = ((Date) value).getTime();
        }
        if (value instanceof ArrayList) {
          value = ((ArrayList) value).get(0);
        }
        props.set(key, value);
      }

      return factory.createEdge(label, sourceId, targetId, props);
    }
  }

  public static final class LDBC2GradoopVertex implements MapFunction<LDBCVertex, Tuple2<Long, VertexPojo>> {

    private final EPGMVertexFactory<VertexPojo> factory;

    public LDBC2GradoopVertex(EPGMVertexFactory<VertexPojo> factory) {
      this.factory = factory;
    }

    @Override
    public Tuple2<Long, VertexPojo> map(LDBCVertex ldbcVertex) throws Exception {
      String label        = ldbcVertex.getLabel();
      PropertyList props  = new PropertyList();

      for (Map.Entry<String, Object> edgeProp : ldbcVertex.getProperties()
        .entrySet()) {
        String key    = edgeProp.getKey();
        Object value  = edgeProp.getValue();

        if (value instanceof Date) {
          value = ((Date) value).getTime();
        }
        if (value instanceof ArrayList) {
          value = ((ArrayList) value).get(0);
        }
        props.set(key, value);
      }

      VertexPojo v = factory.createVertex(label, props);

      return new Tuple2<>(ldbcVertex.getVertexId(), v);
    }
  }

  public String getDescription() {
    return this.getClass().getName();
  }


}
