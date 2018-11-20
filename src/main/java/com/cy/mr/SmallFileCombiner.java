package com.cy.mr;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 当遇到小文件处理时，每个文件会被当成一个split，那么资源消耗非常大，hadoop支持将小文件合并后当成一个切片处理。（默认）
 */
public class SmallFileCombiner {

    static class SmallFileCombinerMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
        NullWritable v = NullWritable.get();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //通过这种方式相当于是直接把值打印到磁盘文件中。value其实就是每一样的的文件内容
            context.write(value, v);
        }

    }

    /**
     * 如果生产环境中，小文件的数量太多，那么累计起来的数量也是很庞大的,那么这时候就要设置切片的大小了。
     *
     * 即使用：CombineTextInputFormat.setMaxInputSplitSize(job, 1024*1024*150);
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.out.println("Usage: small file combiner <in> <out>");
            System.exit(2);
        }
        Path inPath = new Path(otherArgs[0]);
        Path outPath = new Path(otherArgs[1]);

        FileSystem fs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000/"), conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }


        Job job = Job.getInstance(conf);

        job.setJarByClass(SmallFileCombiner.class);
        job.setMapperClass(SmallFileCombinerMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //下面的方式将小文件划分为一个切片。
        job.setInputFormatClass(CombineTextInputFormat.class);
        //如果小文件的总和为224M，将setMaxInputSplitSize中的第二个参数设置成300M的时候，在
        //E:\wordcount\output下只会生成一个part-m-00000这种文件
        //如果将setMaxInputSplitSize中的第二个参数设置成150M的时候，在
        //E:\wordcount\output下会生成part-m-00000 和 part-m-00001 两个文件
        CombineTextInputFormat.setMaxInputSplitSize(job, 1024*1024*150);
        CombineTextInputFormat.setInputPaths(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        job.setNumReduceTasks(0);

        job.waitForCompletion(true);
    }
}