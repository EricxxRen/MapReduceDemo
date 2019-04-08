package com.eki.mapreduce.Filters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * "MapReduce设计模式" 63页
 */
public class DistinctDriver {

    public static class DistinctMapper extends Mapper<Object, Text, Text, NullWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] height = value.toString().split(",");
            Text outvalue = new Text(height[2]);
            context.write(outvalue, NullWritable.get());
        }
    }

    public static class DistinctReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
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
        job.setJarByClass(DistinctDriver.class);

        job.setMapperClass(DistinctMapper.class);
        job.setCombinerClass(DistinctReducer.class);
        job.setReducerClass(DistinctReducer.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
