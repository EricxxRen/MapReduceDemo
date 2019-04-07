package com.eki.mapreduce.Filters;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TopTenDriver {

    public static class TopTenMapper extends Mapper<Object, Text, NullWritable, SortedMapWritable> {

        private SortedMapWritable repToRecord = new SortedMapWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            Text year = new Text(values[3] + ":" + values[1]);
            IntWritable height = new IntWritable(Integer.parseInt(values[2]));
            repToRecord.put(height, year);
            if (repToRecord.size() > 10) {
                repToRecord.remove(repToRecord.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), repToRecord);
        }
    }

    public static void main(String[] args) {

    }
}
