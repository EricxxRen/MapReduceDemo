package com.eki.mapreduce.MedianStdDev;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MedianStdDevTuple implements Writable {

    private Float median = 0F;
    private Float stdev = 0F;

    public Float getMedian() {
        return median;
    }

    public void setMedian(Float median) {
        this.median = median;
    }

    public Float getStdev() {
        return stdev;
    }

    public void setStdev(Float stdev) {
        this.stdev = stdev;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(median);
        dataOutput.writeFloat(stdev);
    }

    public void readFields(DataInput dataInput) throws IOException {
        median = dataInput.readFloat();
        stdev = dataInput.readFloat();
    }

    @Override
    public String toString() {
        return median + "\t" + stdev;
    }
}
