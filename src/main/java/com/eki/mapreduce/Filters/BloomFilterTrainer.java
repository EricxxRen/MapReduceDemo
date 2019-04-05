package com.eki.mapreduce.Filters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

/**
 * "MapReduce设计模式" 49页
 * 训练Bloom filter
 * 是一个通用的应用程序
 * 使用预先确定的词集产生Bloom filter
 */
public class BloomFilterTrainer {

    public static void main(String[] args) throws IOException {

        //输入文件的路径，为1个gzip输入文件或gzip的目录
        Path inputFile = new Path(args[0]);
        //文件中的元素数目
        int numMembers = Integer.parseInt(args[1]);
        //可以容忍的误判率
        float falsePosRate = Float.parseFloat(args[2]);
        //输出文件路径，存储训练好的Bloom filter
        Path bfFile = new Path(args[3]);

        //根据大概值计算最佳vectorSize和K值
        int vectorSize = getOptimalBloomFIlterSize(numMembers, falsePosRate);
        int nbHash = getOptimalK(numMembers, vectorSize);

        //生成新的Bloom filter
        BloomFilter filter = new BloomFilter(vectorSize, nbHash, Hash.MURMUR_HASH);

        System.out.println("Training Bloom filter of size " + vectorSize
        + " with " + nbHash + " hash functions, " + numMembers
        + " approximate number of records, and " + falsePosRate
        + " false positive rate");

        //读取gzip文件
        String line = null;
        int numElements = 0;
        FileSystem fs = FileSystem.get(new Configuration());

        for (FileStatus status : fs.listStatus(inputFile)) {
            BufferedReader rdr = new BufferedReader(new InputStreamReader(
                    new GZIPInputStream(fs.open(status.getPath()))
            ));

            System.out.println("Reading " + status.getPath());

            line = rdr.readLine();

            while (null != line) {
                filter.add(new Key(line.getBytes()));
                ++numElements;
            }

            rdr.close();
        }

        System.out.println("Trained Bloom filter with " + numElements + " entries.");

        System.out.println("Serializing Bloom filter to HDFS at " + bfFile);

        //输出训练好的Bloom filter到文件
        FSDataOutputStream strm = fs.create(bfFile);
        filter.write(strm);
        strm.flush();
        strm.close();

        System.exit(0);

    }

    /**
     * 根据元素个数及误判率计算最优Bloom filter大小
     * @param numElements 元素个数
     * @param falsePosRate 希望的误判率
     * @return 最优的Bloom filter大小
     */
    public static int getOptimalBloomFIlterSize (int numElements, float falsePosRate) {
        return (int) (-numElements * (float) Math.log(falsePosRate) / Math.pow(Math.log(2), 2));
    }

    /**
     *
     * @param numElements 元素个数
     * @param vectorSize Bloom filter大小
     * @return 最优k值，为最接近的integer
     */
    public static int getOptimalK (float numElements, float vectorSize) {
        return (int) Math.round(vectorSize * Math.log(2) / numElements);
    }

}
