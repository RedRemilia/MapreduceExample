package com.kiryumiya.scorecount.singleAvgScore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SingleAvgScoreCountDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 2) {
            System.err.println("Usage: SingleAvgScoreCountDriver <input path> <output path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(SingleAvgScoreCountDriver.class);
        job.setMapperClass(SingleAvgScoreCountMapper.class);
        job.setReducerClass(SingleAvgScoreCountReducer.class);

        job.setMapOutputKeyClass(AvgScoreBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(subjectPartitioner.class);
        job.setNumReduceTasks(4);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
