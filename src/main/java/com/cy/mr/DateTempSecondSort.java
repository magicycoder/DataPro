package com.cy.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DateTempSecondSort {

    public class DTPartitioner extends Partitioner<DateTemperaturePair, Text> {
        @Override
        public int getPartition(DateTemperaturePair pair, Text text, int numPartitions) {

            return Math.abs(pair.getYearMonth().hashCode() % numPartitions);
        }
    }

    public class DTComparator extends WritableComparator {
        public DTComparator() {
            super(DateTemperaturePair.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            DateTemperaturePair p1 = (DateTemperaturePair) a;
            DateTemperaturePair p2 = (DateTemperaturePair) b;
            return p1.getYearMonth().compareTo(p2.getYearMonth());
        }
    }

    public static class DTMapper extends Mapper<Object, Text, DateTemperaturePair, IntWritable> {
        private DateTemperaturePair outKey = new DateTemperaturePair();
        private IntWritable outVal = new IntWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            String yearMonth = tokens[0] + tokens[1];
            String day = tokens[2];
            int temperature = Integer.parseInt(tokens[3]);
            outKey.setYearMonth(yearMonth);
            outKey.setDay(day);
            outKey.setTemperature(temperature);
            outVal.set(temperature);
            context.write(outKey, outVal);
        }
    }

    public static class DTReducer extends Reducer<DateTemperaturePair, IntWritable, DateTemperaturePair, Text> {

        private DateTemperaturePair outKey = new DateTemperaturePair();
        private Text outVal = new Text();

        @Override
        protected void reduce(DateTemperaturePair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            StringBuilder sbr = new StringBuilder();
            for (IntWritable value : values) {
                sbr.append(value.toString() + ",");
            }
            String res = null;
            if (sbr.toString().endsWith(",")) {
                res = sbr.toString().substring(0, sbr.length() - 1);
            }
            outKey.setYearMonth(key.getYearMonth().toString());
            outVal.set(res);
            context.write(outKey, outVal);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.out.println("Usage: DateTempSecondSort <in> <out>");
            System.exit(2);
        }

        Path inPath = new Path(otherArgs[0]);
        Path outPath = new Path(otherArgs[1]);
        FileSystem fs = outPath.getFileSystem(conf);
        //FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath);
        }

        Job job = new Job(conf, "DateTempSecondSort");
        job.setJarByClass(DateTempSecondSort.class);
        job.setMapperClass(DTMapper.class);
        job.setReducerClass(DTReducer.class);
        job.setOutputKeyClass(DateTemperaturePair.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(DTPartitioner.class);
        job.setGroupingComparatorClass(DTComparator.class);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);



    }

}


