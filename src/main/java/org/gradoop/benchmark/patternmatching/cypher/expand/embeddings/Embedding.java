package org.gradoop.benchmark.patternmatching.cypher.expand.embeddings;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Arrays;

public class Embedding {
  private EmbeddingEntry[] entries;

  public Embedding(EmbeddingEntry[] entries) {
    this.entries = entries;
  }

  public Embedding(int size) {
    entries = new EmbeddingEntry[size];
  }

  public Embedding() {
  }

  public EmbeddingEntry get(int column) {
    return entries[column];
  }

  public EmbeddingEntry[] getEntries() {
    return entries;
  }

  public void setEntries(EmbeddingEntry[] entries) {
    this.entries = entries;
  }

  public EmbeddingEntry[] copyEntries() {
    return entries;
  }

  public void add(Tuple3<Long, Long, Long> edge) {
    EmbeddingEntry[] newEntries = new EmbeddingEntry[entries.length + 3];
    System.arraycopy(entries, 0, newEntries, 0, entries.length);
    newEntries[entries.length] = new IdEntry(edge.f0);
    newEntries[entries.length+1] = new IdEntry(edge.f1);
    newEntries[entries.length+2] = new IdEntry(edge.f2);
    entries = newEntries;
  }

  public int size() {
    return entries.length;
  }

  @Override
  public String toString() {
    return Arrays.toString(entries);
  }
}
