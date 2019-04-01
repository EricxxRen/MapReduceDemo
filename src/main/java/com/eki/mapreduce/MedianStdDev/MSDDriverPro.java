package com.eki.mapreduce.MedianStdDev;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * "MapReduce设计模式" 26页
 */
public class MSDDriverPro {

    //input:"1,M,181,2003"
    //outkey:"2003:M"
    //outvalue:"{{170,1},{165,1},...}"
    public static class MSDProMapper extends Mapper<Object, Text, Text, SortedMapWritable> {
        private Text year = new Text();
        private IntWritable height = new IntWritable();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            year = new Text(values[3] + ":" + values[1]);
            height = new IntWritable(Integer.parseInt(values[2]));
            SortedMapWritable smw = new SortedMapWritable();
            smw.put(height, new LongWritable(1));
            context.write(year, smw);
        }
    }

    //inkey:"2003:M"
    //invalue:"{{170,1},{165,1},...}"
    //outkey:"2003:M"
    //outvalue:"{{170,2},{165,5},...}"
    public static class MSDProCombiner extends Reducer<Text, SortedMapWritable, Text, SortedMapWritable> {
        private SortedMapWritable combined = new SortedMapWritable();
        @Override
        protected void reduce(Text key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException {

            //注意每次reduce循环需要清零combined的TreeMap
            combined.clear();

            for (SortedMapWritable value : values) {
                Set<Map.Entry<WritableComparable, Writable>> entries = value.entrySet();
                for (Map.Entry<WritableComparable, Writable> entry : entries) {
                    LongWritable count = (LongWritable) combined.get(entry.getKey());
                    if (count == null) {
                        combined.put(entry.getKey(), new LongWritable(((LongWritable)entry.getValue()).get()));
                    } else {
                        combined.put(entry.getKey(), new LongWritable(((LongWritable) entry.getValue()).get() + count.get()));
                    }
                }
            }
            context.write(key, combined);
        }
    }

    //inkey:"2003:M"
    //invalue:"{{170,2},{165,5},...}"abc
    public static class MSDProReducer extends Reducer<Text, SortedMapWritable, Text, MedianStdDevTuple> {
        private MedianStdDevTuple result = new MedianStdDevTuple();
        private TreeMap<Integer, Long> heightCounts = new TreeMap<Integer, Long>();
        @Override
        protected void reduce(Text key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            long totalCount = 0;

            //注意每次reduce循环需要清零result和heightCounts的TreeMap
            result.setMedian(0F);
            result.setStdev(0F);
            heightCounts.clear();

            for (SortedMapWritable value : values) {
                Set<Map.Entry<WritableComparable, Writable>> entrySet = value.entrySet();
                for (Map.Entry<WritableComparable, Writable> entry : entrySet) {
                    int height = ((IntWritable) entry.getKey()).get();
                    long count = ((LongWritable) entry.getValue()).get();

                    totalCount += count;
                    sum += height * count;

                    Long storedCounts = heightCounts.get(height);
                    if (storedCounts == null) {
                        heightCounts.put(height, count);
                    } else {
                        heightCounts.put(height, storedCounts + count);
                    }
                }
            }

            long medianIndex = totalCount / 2L;
            long previousHeight = 0;
            long previousCount = 0;
            long count = 0;
            for (Map.Entry<Integer, Long> entry : heightCounts.entrySet()) {
                count = previousCount + entry.getValue();

                if (previousCount <= medianIndex && count > medianIndex) {
                    if (totalCount % 2 == 0 && previousCount == medianIndex) {
                        result.setMedian((float) (entry.getKey() + previousHeight) / 2.0f);
                    } else {
                        result.setMedian((float) entry.getKey());
                    }
                    break;
                }

                previousHeight = entry.getKey();
                previousCount = count;
            }

            float mean = sum / totalCount;

            float sumOfSquares = 0f;
            for (Map.Entry<Integer, Long> entry : heightCounts.entrySet()) {
                sumOfSquares += (entry.getKey() - mean) * (entry.getKey() - mean) * entry.getValue();
            }

            result.setStdev((float) Math.sqrt(sumOfSquares / (totalCount - 1)));
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
        job.setJarByClass(MSDDriverPro.class);

        job.setMapperClass(MSDProMapper.class);
        job.setCombinerClass(MSDProCombiner.class);
        job.setReducerClass(MSDProReducer.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputValueClass(SortedMapWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MedianStdDevTuple.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
