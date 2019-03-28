package com.eki.mapreduce.MinMaxCount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MinMaxCountTuple implements Writable {

    private Integer min = 0;
    private Integer max = 0;
    private Integer count = 0;

    public Integer getMin() {
        return min;
    }

    public void setMin(Integer min) {
        this.min = min;
    }

    public Integer getMax() {
        return max;
    }

    public void setMax(Integer max) {
        this.max = max;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(min);
        dataOutput.writeInt(max);
        dataOutput.writeInt(count);
    }

    public void readFields(DataInput dataInput) throws IOException {
        min = dataInput.readInt();
        max = dataInput.readInt();
        count = dataInput.readInt();
    }

    @Override
    public String toString() {
        return min + "\t" + max + "\t" + count;
    }
}
