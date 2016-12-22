package mrhw2.combiner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class StationWritable implements Writable{
	
	public Integer tmaxValue;
    public Integer tmaxCount;
    public Integer tminValue;
    public Integer tminCount;

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
	
	public StationWritable() {
        this.tmaxValue = 0;
        this.tmaxCount = 0;
        this.tminValue = 0;
        this.tminCount = 0;
    }

	public StationWritable(Integer tmaxValue, Integer tmaxCount, Integer tminValue, Integer tminCount) {
        this.tmaxValue = tmaxValue;
        this.tmaxCount = tmaxCount;
        this.tminValue = tminValue;
        this.tminCount = tminCount;
    }

    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeVInt(dataOutput, tmaxValue);
        WritableUtils.writeVInt(dataOutput, tmaxCount);
        WritableUtils.writeVInt(dataOutput, tminValue);
        WritableUtils.writeVInt(dataOutput, tminCount);
    }

    public void readFields(DataInput dataInput) throws IOException {
        tmaxValue = WritableUtils.readVInt(dataInput);
        tmaxCount = WritableUtils.readVInt(dataInput);
        tminValue = WritableUtils.readVInt(dataInput);
        tminCount = WritableUtils.readVInt(dataInput);

    }
	
	
}
