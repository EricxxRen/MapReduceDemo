package com.eki.mapreduce.AvgCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * "MapReduce设计模式" 20页
 */
public class AvgCountDriver {

    public static class ACMapper extends Mapper<Object, Text, Text, AvgCountTuple> {

        private AvgCountTuple act = new AvgCountTuple();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            act.setAvg(Double.parseDouble(values[2]));
            act.setCount(1);
            context.write(new Text(values[3]), act);
        }
    }

    public static class ACReducer extends Reducer<Text, AvgCountTuple, Text, AvgCountTuple> {

        private AvgCountTuple result = new AvgCountTuple();

        @Override
        protected void reduce(Text key, Iterable<AvgCountTuple> values, Context context) throws IOException, InterruptedException {
            Double sum = 0D;
            Integer count = 0;

            for (AvgCountTuple value : values) {
                sum += value.getAvg() * value.getCount();
                count += value.getCount();
            }

            result.setAvg(sum/count);
            result.setCount(count);

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
        job.setJarByClass(AvgCountDriver.class);

        job.setMapperClass(AvgCountDriver.ACMapper.class);
        //job.setCombinerClass(AvgCountDriver.ACReducer.class);
        job.setReducerClass(AvgCountDriver.ACReducer.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputValueClass(AvgCountTuple.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AvgCountTuple.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
