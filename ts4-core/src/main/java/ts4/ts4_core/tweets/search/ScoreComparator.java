package ts4.ts4_core.tweets.search;

import java.util.Comparator;

public class ScoreComparator implements Comparator<ScoreIdPair>{
	public int compare(ScoreIdPair o1, ScoreIdPair o2) {
		return o1.getScore().equals(o2.getScore()) ?
				o1.getIndex().compareTo(o2.getIndex())
				: o2.getScore().compareTo(o1.getScore());
	}
}

class ScoreIdPair {
	Double score;
	Integer index;
	public ScoreIdPair(Double score, Integer index) {
		this.score = score;
		this.index = index;
	}
	public Double getScore() {
		return score;
	}
	public Integer getIndex() {
		return index;
	}
}
