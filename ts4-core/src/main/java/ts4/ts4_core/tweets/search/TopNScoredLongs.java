package ts4.ts4_core.tweets.search;

import java.util.PriorityQueue;

public class TopNScoredLongs {
	private class ScoredLongPriorityQueue extends PriorityQueue<PairOfLongFloat> {
		private static final long serialVersionUID = 1L;
		private int maxSize;
		private int size;
		
		private ScoredLongPriorityQueue() {}
		
		private ScoredLongPriorityQueue(int maxSize) {
			this.maxSize = maxSize;
			size = 0;
		}

		@Override
		public boolean add(PairOfLongFloat e) {
			if(!contains(e)) {
				if (size < maxSize) {
					offer(e);
					size ++;
				} else {
					if (peek().getValue() < e.getValue()) {
						poll();
						offer(e);
						size ++;
					}
				}
				return true;
			}
			return false;
		}
		
		@Override
		public PairOfLongFloat poll() {
			if (size() > 0) size --;
			return super.poll();
		}
	}

	private final ScoredLongPriorityQueue queue;

	public TopNScoredLongs(int n) {
		queue = new ScoredLongPriorityQueue(n);
	}

	public void add(Long id, float score) {
		queue.add(new PairOfLongFloat(id, score));
	}

	public PairOfLongFloat[] extractAll() {
		int len = queue.size();
		PairOfLongFloat[] arr = (PairOfLongFloat[]) new PairOfLongFloat[len];
		for (int i = 0; i < len; i++) {
			arr[len - 1 - i] = queue.poll();
		}
		return arr;
	}
}

class PairOfLongFloat implements Comparable<PairOfLongFloat> {
	private long id;
	private float score;

	public PairOfLongFloat(long id, float score) {
		this.id = id;
		this.score = score;
	}

	public long getKey() {
		return id;
	}

	public float getValue() {
		return score;
	}

	public int compareTo(PairOfLongFloat other) {
		if (score < other.score) return -1;
		else if (score > other.score) return 1;
		return 0;
	}

	public boolean equals(Object obj) {
		return id == ((PairOfLongFloat) obj).id;
	}
}