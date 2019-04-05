package com.eki.mapreduce.Filters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

/**
 * "MapReduce设计模式" 51页
 * 使用BloomFilterTrainer产生的Bloom filter过滤文件
 */
public class BloomFilterDriver {

    public static class BFMapper extends Mapper<Object, Text, Text, NullWritable> {
        BloomFilter filter = new BloomFilter();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //从DistributedCache中获取文件
            URI[] files = DistributedCache.getCacheFiles(context.getConfiguration());
            System.out.println("Reading Bloom filter from: " + files[0].getPath());

            //打开文件
            DataInputStream strm = new DataInputStream(
                    new FileInputStream(files[0].getPath())
            );

            //读取文件并加载到filter中
            filter.readFields(strm);
            strm.close();

        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String comments = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(comments);

            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken();
                if (filter.membershipTest(new Key(word.getBytes()))) {
                    context.write(value, NullWritable.get());
                    break;
                }
            }

        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name","local");
        conf.set("fs.defaultFS","file:///");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: BloomFiltering <in> <cachefile> <out>");
            System.exit(1);
        }

        FileSystem.get(conf).delete(new Path(otherArgs[2]), true);

        Job job = Job.getInstance(conf);
        job.setJarByClass(BloomFilterDriver.class);

        job.setMapperClass(BFMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        DistributedCache.addCacheFile(
                FileSystem.get(conf).makeQualified(new Path(args[1])).toUri(), job.getConfiguration()
        );

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
