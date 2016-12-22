package mrhw2.task2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {
	
	public CompositeKeyComparator() {
		super(CompositeKey.class, true);
	}
	
	@Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {
		CompositeKey cKey1 = (CompositeKey) wc1;
		CompositeKey cKey2 = (CompositeKey) wc2;
        int compare= cKey1.getStationId().compareTo(cKey2.getStationId());
        if (compare == 0) {
            compare = cKey1.getYear().compareTo(cKey2.getYear());
        }
        return compare;
    }
	
}
