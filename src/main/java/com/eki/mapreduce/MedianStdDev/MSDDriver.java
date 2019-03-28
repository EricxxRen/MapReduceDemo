package com.eki.mapreduce.MedianStdDev;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * "MapReduce设计模式" 24页
 */
public class MSDDriver {
    public static class MSDMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text year = new Text();
        private IntWritable height = new IntWritable();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            year = new Text(values[3] + ":" + values[1]);
            height = new IntWritable(Integer.parseInt(values[2]));
            context.write(year, height);
        }
    }

    public static class MSDReducer extends Reducer<Text, IntWritable, Text, MedianStdDevTuple> {
        private MedianStdDevTuple result = new MedianStdDevTuple();
        private List<Float> height = new ArrayList<Float>();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            float count = 0;

            //注意每次reduce循环需要清零result和height的arraylist
            result.setStdev(0F);
            result.setMedian(0F);
            height.clear();

            for (IntWritable v : values) {
                height.add((float) v.get());
                sum += v.get();
                count += 1;
            }

            Collections.sort(height);

            if (count % 2 == 0) {
                float upper = height.get((int) count / 2);
                float lower = height.get((int) count / 2 -1);
                result.setMedian((upper + lower) / 2.0F);
            } else {
                result.setMedian(height.get((int) count / 2));
            }

            float mean = sum / count;
            float sumOfSquares = 0.0F;
            for (float h : height) {
                sumOfSquares += (h - mean) * (h - mean);
            }

            float stdev = (float) Math.sqrt(sumOfSquares / (count - 1));
            result.setStdev(stdev);
            context.write(key, result);
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
        job.setJarByClass(MSDDriver.class);

        job.setMapperClass(MSDMapper.class);
        //job.setCombinerClass();
        job.setReducerClass(MSDReducer.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MedianStdDevTuple.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
