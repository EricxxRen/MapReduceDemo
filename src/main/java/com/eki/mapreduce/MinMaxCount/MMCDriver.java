package com.eki.mapreduce.MinMaxCount;

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
 * "MapReduce设计模式" 16页
 */
public class MMCDriver {
    public static class MMCMapper extends Mapper<Object, Text, Text, MinMaxCountTuple> {
        private Text outKey = new Text();
        private MinMaxCountTuple tuple = new MinMaxCountTuple();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            tuple.setMin(Integer.parseInt(values[2]));
            tuple.setMax(Integer.parseInt(values[2]));
            tuple.setCount(1);
            outKey.set(values[3]);
            context.write(outKey,tuple);


        }
    }

    public static class MMCReducer extends Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {
        private MinMaxCountTuple result = new MinMaxCountTuple();
        @Override
        protected void reduce(Text key, Iterable<MinMaxCountTuple> values, Context context) throws IOException, InterruptedException {
            result.setMin(null);
            result.setMax(null);
            result.setCount(0);
            int sum = 0;

            for (MinMaxCountTuple tuple : values) {
                if (result.getMin() == null || tuple.getMin().compareTo(result.getMin()) < 0) {
                    result.setMin(tuple.getMin());
                }

                if (result.getMax() == null || tuple.getMax().compareTo(result.getMax()) > 0) {
                    result.setMax(tuple.getMax());
                }

                sum += tuple.getCount();
            }

            result.setCount(sum);

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
        job.setJarByClass(MMCDriver.class);

        job.setMapperClass(MMCMapper.class);
        job.setCombinerClass(MMCReducer.class);
        job.setReducerClass(MMCReducer.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputValueClass(MinMaxCountTuple.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MinMaxCountTuple.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
