package com.eki.mapreduce.Filters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Random;

/**
 * "MapReduce设计模式" 44页
 * 在args[0]里输入需要采样的百分比,取20%就输入20
 * 控制台会显示采样获得的条数
 */
public class SimpleRandomSamplingDriver {

    public static class SRSMapper extends Mapper<Object, Text, NullWritable, Text> {

        private Random random = new Random();
        private Double percentage;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            percentage = Double.parseDouble(context.getConfiguration().get("percentage")) / 100;
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Double thisOne = random.nextDouble();

            if (thisOne < percentage) {
                context.write(NullWritable.get(), value);
                context.getCounter("total", "total#").increment(1);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length != 3) {
            System.err.println("Not enough Arguments");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name","local");
        conf.set("fs.defaultFS","file:///");
        conf.set("percentage", args[0]);

        Job job = Job.getInstance(conf);
        job.setJarByClass(SimpleRandomSamplingDriver.class);

        job.setMapperClass(SRSMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        int code = job.waitForCompletion(true) ? 0 : 1;

        if (code == 0) {
            for (Counter counter : job.getCounters().getGroup("total")) {
                System.out.println(counter.getDisplayName() + "\t" + counter.getValue());
            }
        }

        System.exit(code);
    }
}
