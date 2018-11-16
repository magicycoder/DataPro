package com.cy.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public class LocationCount {

    public static class LCMapper extends Mapper<Object, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outVal = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            outKey.set(tokens[0]);
            outVal.set(tokens[1]);
            context.write(outKey, outVal);
        }
    }

    public static class LCReducer extends Reducer<Text, Text, Text, IntWritable> {

        private IntWritable outVal = new IntWritable();


        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Set<Text> set = new HashSet<Text>();

            for (Text val : values) {
                set.add(val);
            }
            outVal.set(set.size());
            context.write(key, outVal);

        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.out.println("Usage: location count <in> <out>");
            System.exit(2);
        }
        Path inPath = new Path(otherArgs[0]);
        Path outPath = new Path(otherArgs[1]);

        FileSystem fs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000/"), conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        Job job = new Job(conf, "location count");
        job.setJarByClass(LocationCount.class);
        job.setMapperClass(LCMapper.class);
        job.setReducerClass(LCReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
