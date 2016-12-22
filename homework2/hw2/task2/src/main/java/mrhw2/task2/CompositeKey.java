package mrhw2.task2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKey implements WritableComparable<CompositeKey> {
	
	public String stationId;
    public String year;
    
    public CompositeKey() {
        stationId = new String();
        year = new String();
    }
    
    public CompositeKey(String stationId, String year) {
        this.stationId = stationId;
        this.year = year;
    }
    
    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof CompositeKey)) return false;
        CompositeKey cKey = (CompositeKey) o;
        return (cKey.year.equals(year) && cKey.stationId.equals(stationId));
    }
    
    
	@Override
	public int hashCode() {
		int hashValue = stationId.hashCode();
		hashValue = 31 * hashValue + year.hashCode();
        return hashValue;
	}

	public String getStationId() {
		return stationId;
	}

	public void setStationId(String stationId) {
		this.stationId = stationId;
	}

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public void readFields(DataInput arg0) throws IOException {
		stationId = WritableUtils.readString(arg0);
        year = WritableUtils.readString(arg0);
		
	}

	public void write(DataOutput arg0) throws IOException {
		WritableUtils.writeString(arg0, stationId);
        WritableUtils.writeString(arg0, year);
		
	}

	public int compareTo(CompositeKey o) {
		int compare = stationId.compareTo(o.stationId);
        if (compare == 0) {
            compare = year.compareTo(o.year);
        }
        return compare;
	}

}
