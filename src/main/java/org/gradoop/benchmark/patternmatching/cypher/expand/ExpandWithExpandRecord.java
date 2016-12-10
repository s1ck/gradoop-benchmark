package org.gradoop.benchmark.patternmatching.cypher.expand;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;


public class ExpandWithExpandRecord {
  private DataSet<Tuple3<Long, Long, Long>> candidateEdges;
  private final int lowerBound;
  private final int upperBound;


  public ExpandWithExpandRecord(DataSet<Tuple3<Long,Long,Long>> candidateEdges, int lowerBound, int upperBound) {
    this.candidateEdges = candidateEdges;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
  }


  public DataSet<ExpandRecord> evaluate() {
    DataSet<ExpandRecord> initialWorkingSet = preProcess();

    DataSet<ExpandRecord> iterationResults = iterate(initialWorkingSet);

    return postProcess(iterationResults);
  }


  private DataSet<ExpandRecord> preProcess() {
    return candidateEdges.map( edge -> ExpandRecord.fromEdge(edge)).returns(ExpandRecord.class);
  }


  private DataSet<ExpandRecord> iterate(DataSet<ExpandRecord> initialWorkingSet) {

    IterativeDataSet<ExpandRecord> iteration = initialWorkingSet.iterate(upperBound-1);

    DataSet<ExpandRecord> nextWorkingSet = iteration
      .filter(new FilterPreviousEmbedding())
      .join(candidateEdges)
        .where(new ExpandRecordKeySelector())
        .equalTo(0)
        .with(new JoinExpandRecords());

    DataSet<ExpandRecord> solutionSet = nextWorkingSet.union(iteration);

    return iteration.closeWith(solutionSet, nextWorkingSet);
  }


  private DataSet<ExpandRecord> postProcess(DataSet<ExpandRecord> iterationResults) {
    return iterationResults.filter(new FilterLowerBound(lowerBound));
  }


  private class JoinExpandRecords
    extends RichFlatJoinFunction<ExpandRecord, Tuple3<Long, Long, Long>, ExpandRecord> {

    @Override
    public void join(ExpandRecord first, Tuple3<Long, Long, Long> second,
      Collector<ExpandRecord> out) throws Exception {

      if (first.canExpand(second)) {
        out.collect(first.expand(second));
      }
    }
  }

  private class ExpandRecordKeySelector implements KeySelector<ExpandRecord, Long> {
    @Override
    public Long getKey(ExpandRecord value) throws Exception {
      return value.getId(value.size() -1);
    }
  }

  private class FilterPreviousEmbedding extends RichFilterFunction<ExpandRecord> {
    @Override
    public boolean filter(ExpandRecord embedding) {
      int currentIteration = getIterationRuntimeContext().getSuperstepNumber() * 3;
      return embedding.size() >= currentIteration;
    }
  }

  private class FilterLowerBound extends RichFilterFunction<ExpandRecord> {
    private int minSize;

    public FilterLowerBound(int lowerBound) {
      this.minSize = lowerBound * 3;
    }

    @Override
    public boolean filter(ExpandRecord embedding) {
      return embedding.size() >= minSize;
    }
  }
}
