/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.benchmark.gradoopid;

import org.apache.flink.api.java.tuple.Tuple4;
import org.bson.types.ObjectId;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Wraps a 96-Bit BSON {@link ObjectId} in a Tuple4.
 *
 * f0: timestamp
 * f1: machine identifier
 * f2: process identifier
 * f3: counter
 */
public class GradoopIdTuple4 extends Tuple4<Integer, Integer, Short, Integer> {

  /**
   * Create a new ObjectId.
   */
  public GradoopIdTuple4() {
  }

  /**
   * Create GradoopId from existing ObjectId.
   *
   * @param objectId ObjectId
   */
  GradoopIdTuple4(ObjectId objectId) {
    checkNotNull(objectId, "ObjectId was null");
    this.f0 = objectId.getTimestamp();
    this.f1 = objectId.getMachineIdentifier();
    this.f2 = objectId.getProcessIdentifier();
    this.f3 = objectId.getCounter();
  }

  /**
   * Returns the Gradoop ID represented by a string.
   *
   * @param string string representation
   * @return Gradoop ID
   */
  public static GradoopIdTuple4 fromString(String string) {
    checkNotNull(string, "ID string was null");
    checkArgument(!string.isEmpty(), "ID string was empty");
    return new GradoopIdTuple4(new ObjectId(string));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GradoopIdTuple4)) {
      return false;
    }
    GradoopIdTuple4 that = (GradoopIdTuple4) o;
    if(!this.f3.equals(that.f3)) { // counter
      return false;
    }
    if (!this.f1.equals(that.f1)) { // machine
      return false;
    }
    if (!this.f2.equals(that.f2)) { // process
      return false;
    }
    if (!this.f0.equals(that.f0)) { // timestamp
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = f0;
    result = 31 * result + f1;
    result = 31 * result + (int) f2;
    result = 31 * result + f3;
    return result;
  }
}
