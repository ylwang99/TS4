package ts4.ts4_core.tweets.util;

import java.io.Serializable;

import edu.umd.cloud9.util.map.HMapKI;

@SuppressWarnings("serial")
public class CFStats implements Serializable {
	private HMapKI<String> cf2Freq; 
	private int totalTerm;
	
	public void setCf(HMapKI<String> cf2Freq) {
		this.cf2Freq = cf2Freq;
	}
	
	public void setTotalTermCnt(int totalTerm) {
		this.totalTerm = totalTerm;
	}
	
	public int getFreq(String term) {
		return cf2Freq.get(term);
	}
	
	public int getTotalTermCnt() {
		return totalTerm;
	}
}