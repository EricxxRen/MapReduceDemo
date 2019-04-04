package com.eki.mapreduce.Filters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * "MapReduce设计模式" 43页
 * 在args中写了regex
 * 匹配"1,M,181,2003"的regex为"\d+,[M,F],1[4-9]\d,200[0-3]"
 * /d+表示任何数字
 * [M,F]为M或F
 * 1[4-9]\d表示只匹配身高在140到199之间的
 * 200[0-3]表示只匹配2000年到2003年的
 */
public class GrepFilterDriver {

    public static class GrepMapper extends Mapper<Object, Text, NullWritable, Text> {

        private String mapregex = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mapregex = context.getConfiguration().get("mapregex");
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String txt = value.toString();

            if (txt.matches(mapregex)) {
                context.write(NullWritable.get(), value);
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
        conf.set("mapregex", args[0]);

        Job job = Job.getInstance(conf);
        job.setJarByClass(GrepFilterDriver.class);

        job.setMapperClass(GrepMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
