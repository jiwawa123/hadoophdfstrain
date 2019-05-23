package com.ji.hadoop.mapReduce;/*
    user ji
    data 2019/3/7
    time 9:27 PM
    使用mapreduce开发wordCount应用输出
*/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountCombineApp {
    /*
    map:读取输入的文件
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        LongWritable one = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String words[] = line.split(" ");
            for (String str : words
                    ) {
                //通过上下文将map程序做完
                context.write(new Text(str), one);
            }
        }
    }

    /*
    reduce 归并操作
     */
    public static class MyReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable lw : values
                    ) {
                //求总和
                sum += lw.get();
            }
            //将最终结果进行输出
            context.write(key, new LongWritable(sum));
        }
    }

    /*
    定义Driver 进行封装MapReduce
     */
    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.delete(new Path(args[1]));
        Job job = Job.getInstance(configuration, "wordCount");
        //设置mapreduce的处理类
        job.setJarByClass(WordCountCombineApp.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //设置map处理
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //设置一个combine处理
        job.setCombinerClass(MyReduce.class);
        //设置reduce处理
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置输出
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        try {
            System.out.println(job.waitForCompletion(true));
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }



}
