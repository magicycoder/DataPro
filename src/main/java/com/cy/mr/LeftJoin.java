package com.cy.mr;

import edu.umd.cloud9.io.pair.PairOfStrings;
import javafx.scene.control.TextFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.awt.image.DataBuffer;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

public class LeftJoin {

    public static class LJUsrMapper extends Mapper<LongWritable, Text, PairOfStrings, PairOfStrings> {

        private PairOfStrings outKey = new PairOfStrings();
        private PairOfStrings outVal = new PairOfStrings();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] tokens = value.toString().split(",");
            String uid = tokens[0];
            String location = tokens[1];
            outKey.set(uid, "1");
            outVal.set("L", location);
            context.write(outKey, outVal);
        }
    }

    public static class LJOdrMapper extends Mapper<LongWritable, Text, PairOfStrings, PairOfStrings> {

        private PairOfStrings outKey = new PairOfStrings();
        private PairOfStrings outVal = new PairOfStrings();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] tokens = value.toString().split(",");
            String pid = tokens[1];
            String uid = tokens[2];
            outKey.set(uid, "2");
            outVal.set("P", pid);
            context.write(outKey, outVal);

        }
    }

    public static class LJReducer extends Reducer<PairOfStrings, PairOfStrings, NullWritable, Text> {

        private NullWritable outKey = NullWritable.get();
        private Text outVal = new Text();

        @Override
        protected void reduce(PairOfStrings key, Iterable<PairOfStrings> values, Context context) throws IOException, InterruptedException {

            Iterator<PairOfStrings> itr = values.iterator();
            if (itr.hasNext()) {
                PairOfStrings pair = itr.next();
                String location;
                if ("L".equals(pair.getLeftElement())) {
                    location = pair.getRightElement();
                }
                else {
                    return;
                }

                while (itr.hasNext()) {
                    pair = itr.next();
                    String pid = pair.getRightElement();
                    outVal.set(pid + "," + location);
                    context.write(outKey, outVal);
                }

            }
        }
    }

    public static class LJPartitioner extends Partitioner<PairOfStrings, PairOfStrings> {

        @Override
        public int getPartition(PairOfStrings p1, PairOfStrings p2, int numPartitions) {
            return p1.getLeftElement().compareTo(p2.getLeftElement());
        }
    }

    public static class LJComparator implements RawComparator<PairOfStrings> {

        public int compare(PairOfStrings o1, PairOfStrings o2) {
            return o1.getLeftElement().compareTo(o2.getLeftElement());
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            DataInputBuffer buffer = new DataInputBuffer();
            PairOfStrings p1 = new PairOfStrings();
            PairOfStrings p2 = new PairOfStrings();

            try {
                buffer.reset(b1, s1, l1);
                p1.readFields(buffer);
                buffer.reset(b2, s2, l2);
                p2.readFields(buffer);
                return compare(p1, p2);
            } catch (Exception e) {
                return -1;
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.out.println("Usage: left join <usr> <order> <out>");
            System.exit(2);
        }

        Path usrPath = new Path(otherArgs[0]);
        Path odrPath = new Path(otherArgs[1]);
        Path outPath = new Path(otherArgs[2]);

        FileSystem fs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000/"), conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        Job job = new Job(conf, "left join");
        job.setJarByClass(LeftJoin.class);
        MultipleInputs.addInputPath(job, usrPath, TextInputFormat.class, LJUsrMapper.class);
        MultipleInputs.addInputPath(job, odrPath, TextInputFormat.class, LJOdrMapper.class);
        job.setReducerClass(LJReducer.class);
        job.setMapOutputKeyClass(PairOfStrings.class);
        job.setMapOutputValueClass(PairOfStrings.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(LJPartitioner.class);
        job.setGroupingComparatorClass(LJComparator.class);
        job.setSortComparatorClass(PairOfStrings.Comparator.class);

        FileOutputFormat.setOutputPath(job, outPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }



}
