package com.eki.mapreduce.ch4;

import com.eki.mapreduce.util.MRDPUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class PostCommentDriver {

    public static class PostMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = MRDPUtil.transformXml2Map(value.toString());
            String postID = parsed.get("ID");

            if (postID == null) {
                return;
            }

            outKey.set(postID);
            outValue.set("P" + value.toString());
            context.write(outKey, outValue);
        }
    }

    public static class CommentMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = MRDPUtil.transformXml2Map(value.toString());
            String postID = parsed.get("postID");

            if (postID == null) {
                return;
            }

            outKey.set(postID);
            outValue.set("C" + value.toString());
            context.write(outKey, outValue);
        }
    }

    public static class PCReducer extends Reducer<Text, Text, Text, NullWritable> {
        private String post = null;
        private ArrayList<String> comments = new ArrayList<String>();
        private DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            post = null;
            comments.clear();

            for (Text value : values) {
                if (value.charAt(0) == 'P') {
                    post = value.toString().substring(1, value.toString().length()).trim();
                } else {
                    comments.add(value.toString().substring(1, value.toString().length()).trim());
                }
            }

            if (post != null) {

            }
        }

        private String nestElements (DocumentBuilderFactory dbf, String post, ArrayList<String> comments) {
            return null;
        }
    }

    public static void main(String[] args) {

    }
}
