package mrhw2.task2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class StationWritable implements Writable {
	public String year;
    public Integer tmaxValue;
    public Integer tmaxCount;
    public Integer tminValue;
    public Integer tminCount;
    
    public StationWritable() {
        year = new String();
        tmaxValue = 0;
        tmaxCount = 0;
        tminValue = 0;
        tminCount = 0;
    }

    public StationWritable(String year, Integer tmaxValue, Integer tmaxCount, Integer tminValue, Integer tminCount) {
        this.year = year;
        this.tmaxValue = tmaxValue;
        this.tmaxCount = tmaxCount;
        this.tminValue = tminValue;
        this.tminCount = tminCount;
    }
    
	public void readFields(DataInput dataInput) throws IOException {
		year = WritableUtils.readString(dataInput);
        tmaxValue = WritableUtils.readVInt(dataInput);
        tmaxCount = WritableUtils.readVInt(dataInput);
        tminValue = WritableUtils.readVInt(dataInput);
        tminCount = WritableUtils.readVInt(dataInput);
		
	}
	
	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, year);
        WritableUtils.writeVInt(dataOutput, tmaxValue);
        WritableUtils.writeVInt(dataOutput, tmaxCount);
        WritableUtils.writeVInt(dataOutput, tminValue);
        WritableUtils.writeVInt(dataOutput, tminCount);
		
	}
	
	public String getYear() {
		return year;
	}
	public void setYear(String year) {
		this.year = year;
	}
	public Integer getTmaxValue() {
		return tmaxValue;
	}
	public void setTmaxValue(Integer tmaxValue) {
		this.tmaxValue = tmaxValue;
	}
	public Integer getTmaxCount() {
		return tmaxCount;
	}
	public void setTmaxCount(Integer tmaxCount) {
		this.tmaxCount = tmaxCount;
	}
	public Integer getTminValue() {
		return tminValue;
	}
	public void setTminValue(Integer tminValue) {
		this.tminValue = tminValue;
	}
	public Integer getTminCount() {
		return tminCount;
	}
	public void setTminCount(Integer tminCount) {
		this.tminCount = tminCount;
	}
	
	
}
