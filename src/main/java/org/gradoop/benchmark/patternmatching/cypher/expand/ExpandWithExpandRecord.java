package org.gradoop.benchmark.patternmatching.cypher.expand;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;


public class ExpandWithExpandRecord {
  private DataSet<Tuple3<Long, Long, Long>> candidateEdges;
  private final int lowerBound;
  private final int upperBound;


  public ExpandWithExpandRecord(DataSet<Tuple3<Long,Long,Long>> candidateEdges, int lowerBound, int upperBound) {
    this.candidateEdges = candidateEdges;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
  }


  public void evaluate() throws Exception {
    DataSet<ExpandRecord> initialWorkingSet = preProcess();

    DataSet<ExpandRecord> iterationResults = iterate(initialWorkingSet);

    System.out.println(postProcess(iterationResults).count());
  }


  private DataSet<ExpandRecord> preProcess() {
    return candidateEdges.map( edge -> ExpandRecord.fromEdge(edge)).returns(ExpandRecord.class);
  }


  private DataSet<ExpandRecord> iterate(DataSet<ExpandRecord> initialWorkingSet) {

    IterativeDataSet<ExpandRecord> iteration = initialWorkingSet.iterate(upperBound);

    DataSet<ExpandRecord> nextWorkingSet = iteration
      .filter(new FilterPreviousEmbedding())
      .join(candidateEdges)
        .where(new ExpandRecordKeySelector())
        .equalTo(0)
        .with((expandRecord, edge) -> expandRecord.expand(edge))
        .returns(ExpandRecord.class);

    DataSet<ExpandRecord> solutionSet = nextWorkingSet.union(iteration);

    return iteration.closeWith(solutionSet, nextWorkingSet);
  }


  private DataSet<ExpandRecord> postProcess(DataSet<ExpandRecord> iterationResults) {
    return iterationResults.filter(new FilterLowerBound(lowerBound));
  }


  private class ExpandRecordKeySelector implements KeySelector<ExpandRecord, Long> {
    @Override
    public Long getKey(ExpandRecord value) throws Exception {
      return value.getId(value.getSize() -1);
    }
  }

  private class FilterPreviousEmbedding extends RichFilterFunction<ExpandRecord> {
    /**
     * super step
     */
    private int currentIteration;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      currentIteration = getIterationRuntimeContext().getSuperstepNumber() * 3;
    }

    @Override
    public boolean filter(ExpandRecord embedding) {
      return embedding.getSize() >= currentIteration;
    }
  }

  private class FilterLowerBound extends RichFilterFunction<ExpandRecord> {
    private int minSize;

    public FilterLowerBound(int lowerBound) {
      this.minSize = lowerBound * 3;
    }

    @Override
    public boolean filter(ExpandRecord embedding) {
      return embedding.getSize() >= minSize;
    }
  }
}
