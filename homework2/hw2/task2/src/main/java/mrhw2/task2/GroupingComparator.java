package mrhw2.task2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class GroupingComparator extends WritableComparator {

	public GroupingComparator() {
		super(CompositeKey.class, true);
	}
	
	@Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {
		CompositeKey cKey1 = (CompositeKey) wc1;
		CompositeKey cKey2 = (CompositeKey) wc2;
        return cKey1.getStationId().compareTo(cKey2.getStationId());
    }

}
