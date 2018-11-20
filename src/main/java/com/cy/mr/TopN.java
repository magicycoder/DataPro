package com.cy.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.net.URI;
import java.util.*;

public class TopN {


    public static class TopNMapper extends Mapper<Object, Text, NullWritable, Text> {

        private SortedMap<Integer, String> top = new TreeMap<Integer, String>();
        private Text outVal = new Text();
        private int N = 10;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.N = context.getConfiguration().getInt("N", 10);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] tokens = value.toString().split(",");
            int num = Integer.parseInt(tokens[1]);
            String keyAsString = tokens[0];
            String compositeVal = keyAsString + "," + num;
            top.put(num, compositeVal);

            if (top.size() > N) {
                top.remove(top.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            for (String val : top.values()) {
                outVal.set(val);
                context.write(NullWritable.get(), outVal);
            }
        }
    }

    public static class TopNReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

        private int N = 10;
        private SortedMap<Integer, String> top = new TreeMap<Integer, String>();
        private Text outVal = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.N = context.getConfiguration().getInt("N", 10);
        }

        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text val : values) {
                String[] tokens = val.toString().split(",");
                int num = Integer.parseInt(tokens[1]);
                String keyAsString = tokens[0];
                String compositeVal = keyAsString + "," + num;
                top.put(num, compositeVal);
            }

            if (top.size() > N) {
                top.remove(top.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String val : top.values()) {
                outVal.set(val);
                context.write(NullWritable.get(), outVal);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.out.println("Usage: topN <in> <out>");
            System.exit(2);
        }
        Path inPath = new Path(otherArgs[0]);
        Path outPath = new Path(otherArgs[1]);

        FileSystem fs = FileSystem.get(new URI("hdfs://127.0.0.1:9000"), conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        Job job = new Job(conf, "topN");
        job.setJarByClass(TopN.class);
        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);



    }


}
