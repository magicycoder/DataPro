package com.cy.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
<<<<<<< HEAD
=======
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
>>>>>>> 75ab40db41626e1e635c98f11f74062abe710499
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

<<<<<<< HEAD
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
=======
import java.io.IOException;
import java.net.URI;

public class DateTempSecondSort {


    public static class SSMapper extends Mapper<Object, Text, DateTemperaturePair, IntWritable> {

>>>>>>> 75ab40db41626e1e635c98f11f74062abe710499
        private DateTemperaturePair outKey = new DateTemperaturePair();
        private IntWritable outVal = new IntWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
<<<<<<< HEAD
=======

>>>>>>> 75ab40db41626e1e635c98f11f74062abe710499
            String[] tokens = value.toString().split(",");
            String yearMonth = tokens[0] + tokens[1];
            String day = tokens[2];
            int temperature = Integer.parseInt(tokens[3]);
<<<<<<< HEAD

=======
>>>>>>> 75ab40db41626e1e635c98f11f74062abe710499
            outKey.setYearMonth(yearMonth);
            outKey.setDay(day);
            outKey.setTemperature(temperature);
            outVal.set(temperature);
<<<<<<< HEAD
            System.out.println("===>" + yearMonth + temperature);
            context.write(outKey, outVal);
            System.out.println("===>" + yearMonth + temperature);
        }
    }

    public static class DTReducer extends Reducer<DateTemperaturePair, IntWritable, DateTemperaturePair, Text> {

        private DateTemperaturePair outKey = new DateTemperaturePair();
=======
            System.out.println("DEBUG map: " + outKey.getYearMonthDay() + ":" + outKey.getTemperature());
            context.write(outKey, outVal);

        }
    }

    public static class SSReducer extends Reducer<DateTemperaturePair, IntWritable, Text, Text> {

        private Text outKey = new Text();
>>>>>>> 75ab40db41626e1e635c98f11f74062abe710499
        private Text outVal = new Text();

        @Override
        protected void reduce(DateTemperaturePair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
<<<<<<< HEAD
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
=======

            StringBuilder sbr = new StringBuilder();
            for (IntWritable value : values) {
                sbr.append(value.toString() + ",");

            }

            String res = null;
            if (sbr.toString().endsWith(",")) {
                res = sbr.toString().substring(0, sbr.length()-1);
            }

            outKey.set(key.getYearMonth().toString());
            outVal.set(res);
            System.out.println("DEBUG reduce: " + outKey.toString() + ":" + outVal.toString());
>>>>>>> 75ab40db41626e1e635c98f11f74062abe710499
            context.write(outKey, outVal);
        }
    }

<<<<<<< HEAD
=======
    public static class SSPartitioner extends Partitioner<DateTemperaturePair, IntWritable> {

        @Override
        public int getPartition(DateTemperaturePair pair, IntWritable intWritable, int numPartitions) {
            System.out.println("DEBUG Partitioner: " + pair.getYearMonthDay() + ":" + pair.getTemperature());
            return pair.getYearMonth().hashCode() % numPartitions;
        }
    }

    public static class SSNKComparator extends WritableComparator {

        public SSNKComparator() {
            super(DateTemperaturePair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {

            DateTemperaturePair p1 = (DateTemperaturePair) a;
            DateTemperaturePair p2 = (DateTemperaturePair) b;
            System.out.println("DEBUG NKComparator: " + p1.getYearMonthDay() + ":" + p2.getTemperature());
            return p1.getYearMonth().compareTo(p2.getYearMonth());
        }
    }

    public static class SSCKComparator extends WritableComparator {
        public SSCKComparator() {
            super(DateTemperaturePair.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            DateTemperaturePair p1 = (DateTemperaturePair) a;
            DateTemperaturePair p2 = (DateTemperaturePair) b;
            System.out.println("DEBUG CKComparator: " + p1.getYearMonthDay() + ":" + p2.getTemperature());
            int cmp = p1.getYearMonth().compareTo(p2.getYearMonth());
            if (cmp == 0) {
                if (p1.getTemperature().get() > p2.getTemperature().get()) {
                    return 1;
                }
                else {
                    return -1;
                }
            }
            else {
                return cmp;
            }
        }
    }

>>>>>>> 75ab40db41626e1e635c98f11f74062abe710499
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
<<<<<<< HEAD
            System.out.println("Usage: DateTempSecondSort <in> <out>");
=======
            System.out.println("Usage: second sort <in> <out>");
>>>>>>> 75ab40db41626e1e635c98f11f74062abe710499
            System.exit(2);
        }

        Path inPath = new Path(otherArgs[0]);
        Path outPath = new Path(otherArgs[1]);
<<<<<<< HEAD
        FileSystem fs = outPath.getFileSystem(conf);
        //FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath,true);
        }

        Job job = new Job(conf, "DateTempSecondSort");
        job.setJarByClass(DateTempSecondSort.class);
        job.setMapperClass(DTMapper.class);
        job.setReducerClass(DTReducer.class);
        job.setOutputKeyClass(DateTemperaturePair.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(DTPartitioner.class);
        job.setGroupingComparatorClass(DTComparator.class);
=======

        FileSystem fs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"), conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        Job job = new Job(conf, "second sort");
        job.setJarByClass(DateTempSecondSort.class);
        job.setMapperClass(SSMapper.class);
        job.setReducerClass(SSReducer.class);
        job.setMapOutputKeyClass(DateTemperaturePair.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setPartitionerClass(SSPartitioner.class);
        job.setGroupingComparatorClass(SSNKComparator.class);
        job.setSortComparatorClass(SSCKComparator.class);
>>>>>>> 75ab40db41626e1e635c98f11f74062abe710499

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);


<<<<<<< HEAD

    }

=======
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 128*1024*1024);
        CombineTextInputFormat.addInputPath(job, inPath);
    }


>>>>>>> 75ab40db41626e1e635c98f11f74062abe710499
}


