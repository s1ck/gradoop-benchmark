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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.bson.types.ObjectId;
import org.junit.Test;
import scala.Int;

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
public class GradoopIdTuple2 extends Tuple2<Integer, Long> {

  /**
   * Create a new ObjectId.
   */
  public GradoopIdTuple2() {
  }

  /**
   * Create GradoopId from existing ObjectId.
   *
   * @param objectId ObjectId
   */
  GradoopIdTuple2(ObjectId objectId) {
    checkNotNull(objectId, "ObjectId was null");
    this.f0 = objectId.getTimestamp();
    this.f1 = ((long) objectId.getMachineIdentifier()) << 40;
    this.f1 += ((long) objectId.getProcessIdentifier()) << 24;
    this.f1 += (long) objectId.getCounter();
    System.out.println(objectId.getCounter());
  }

  /**
   * Returns the Gradoop ID represented by a string.
   *
   * @param string string representation
   * @return Gradoop ID
   */
  public static GradoopIdTuple2 fromString(String string) {
    checkNotNull(string, "ID string was null");
    checkArgument(!string.isEmpty(), "ID string was empty");
    return new GradoopIdTuple2(new ObjectId(string));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GradoopIdTuple2)) {
      return false;
    }
    GradoopIdTuple2 that = (GradoopIdTuple2) o;
    if(!this.f0.equals(that.f0)) { // counter
      return false;
    }
    if (!this.f1.equals(that.f1)) { // machine
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = f0;
    result = 31 * result + (int)(f1>>>40) & 0x00FFFFFF;
    result = 31 * result + (int)(f1>>>24) & 0x0000FFFF;
    result = 31 * result + (int)(long)f1 & 0x00FFFFFF;
    return result;
  }
}
