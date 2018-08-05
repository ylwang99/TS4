/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package ts4.ts4_core.tweets.search;

import java.util.Comparator;

import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;

import edu.umd.cloud9.io.pair.PairOfLongFloat;

public class TopNFast {
  int maxSize;
  int len;
  private class ScoredLongPriorityQueue extends ObjectHeapPriorityQueue<PairOfLongFloat> {
    private ScoredLongPriorityQueue(int maxSize) {
      super(maxSize, new Comparator<PairOfLongFloat>() {
        public int compare(PairOfLongFloat obj0, PairOfLongFloat obj1) {
          if (((PairOfLongFloat) obj0).getRightElement() < ((PairOfLongFloat) obj1).getRightElement()) return -1;
          else if (((PairOfLongFloat) obj0).getRightElement() > ((PairOfLongFloat) obj1).getRightElement()) return 1;
          else return 0;
        }
      });
    }
  }

  private final ScoredLongPriorityQueue queue;

  public TopNFast(int n) {
    len = 0;
    maxSize = n;
    queue = new ScoredLongPriorityQueue(n);
  }

  public void add(long id, float f) {
    if (len < maxSize) {
      queue.enqueue(new PairOfLongFloat(id, f));
      len ++;
    } else {
      PairOfLongFloat top = queue.first();
      if (top.getRightElement() < f) {
        queue.dequeue();
        queue.enqueue(new PairOfLongFloat(id, f));
      }
    }
  }

  public PairOfLongFloat[] extractAll() {
    int len = queue.size();
    PairOfLongFloat[] arr = new PairOfLongFloat[len];
    for (int i = 0; i < len; i++) {
      arr[len - 1 - i] = queue.dequeue();
    }
    return arr;
  }
}