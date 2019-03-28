package com.eki.mapreduce.AvgCount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AvgCountTuple implements Writable {

    private Double avg = 0D;
    private Integer count = 0;

    public Double getAvg() {
        return avg;
    }

    public void setAvg(Double avg) {
        this.avg = avg;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(avg);
        dataOutput.writeInt(count);
    }

    public void readFields(DataInput dataInput) throws IOException {
        avg = dataInput.readDouble();
        count = dataInput.readInt();
    }

    @Override
    public String toString() {
        return avg + "\t" + count;
    }
}
