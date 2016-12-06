package org.gradoop.benchmark.patternmatching.cypher.join.embeddings;

import com.google.common.primitives.Longs;
import org.gradoop.common.model.impl.id.GradoopId;

public class IdEntry implements EmbeddingEntry {
  public final byte[] id;

  public IdEntry(byte[] id) {
    this.id = id;
  }

  public IdEntry(GradoopId id) {
    this.id = id.getRawBytes();
  }

  public IdEntry(Long id) {
    this.id = Longs.toByteArray(id);
  }

  @Override
  public byte[] getId() {
    return id;
  }
}
