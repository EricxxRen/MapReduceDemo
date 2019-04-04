package com.eki.mapreduce.CountInput;

import com.eki.mapreduce.MedianStdDev.InputCountDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * "MapReduce设计模式" 37页
 */
public class CounterDriver {

    public static class CountMapper extends Mapper<Object, Text, NullWritable, NullWritable> {

        public static final String YEAR_GENDER = "YEAR_GENDER";

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            Text outkey = new Text(values[3] + ":" + values[1]);

            context.getCounter(YEAR_GENDER, outkey.toString()).increment(1);
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
        job.setJarByClass(CounterDriver.class);

        job.setMapperClass(CountMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        Path outputPath = new Path(args[1]);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        System.exit(job.waitForCompletion(true) ? 0 : 1);
        int code = job.waitForCompletion(true) ? 0 : 1;

        if (code == 0) {
            for (Counter counter : job.getCounters().getGroup(
                    CountMapper.YEAR_GENDER
            )) {
                System.out.println(counter.getDisplayName() + "\t" + counter.getValue());
            }
        }

        FileSystem.get(conf).delete(outputPath, true);

        System.exit(code);

    }
}
