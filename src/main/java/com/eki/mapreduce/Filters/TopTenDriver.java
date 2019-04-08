package com.eki.mapreduce.Filters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.TreeMap;

/**
 * "MapReduce设计模式" 58页
 */
public class TopTenDriver {

    public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {

        private TreeMap<Integer, Text> repToRecord = new TreeMap<Integer, Text>();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            Text year = new Text(values[3] + ":" + values[1]);
            Integer height = Integer.parseInt(values[2]);
            repToRecord.put(height, new Text(value));
            if (repToRecord.size() > 10) {
                repToRecord.remove(repToRecord.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Text t : repToRecord.values()) {
                context.write(NullWritable.get(), t);
            }
        }
    }

    public static class TopTenReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

        private TreeMap<Integer, Text> repToRecord = new TreeMap<Integer, Text>();

        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                String[] tokens = value.toString().split(",");
                Integer height = Integer.parseInt(tokens[2]);
                repToRecord.put(height, new Text(value));
                if (repToRecord.size() > 10) {
                    repToRecord.remove(repToRecord.firstKey());
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Text t : repToRecord.descendingMap().values()) {
                String[] values = t.toString().split(",");
                Text year = new Text(values[3] + ":" + values[1]);
                Integer height = Integer.parseInt(values[2]);
                Text outvalue = new Text(height + "\t" + year.toString());
                context.write(NullWritable.get(), outvalue);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length != 2) {
            System.err.println("Not enough Arguments");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name","local");
        conf.set("fs.defaultFS","file:///");

        Job job = Job.getInstance(conf);
        job.setJarByClass(TopTenDriver.class);

        job.setMapperClass(TopTenMapper.class);
        job.setReducerClass(TopTenReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputValueClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
