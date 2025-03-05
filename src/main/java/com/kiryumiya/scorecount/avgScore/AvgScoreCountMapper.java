package com.kiryumiya.scorecount.avgScore;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AvgScoreCountMapper extends Mapper<LongWritable, Text, Text, Text>{

    private final Text subject = new Text();
    private final Text scores = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] vals = line.split(",");
        subject.set(vals[0]);
        scores.set(line.substring(vals[0].length() + vals[1].length() + 2));
        context.write(subject, scores);
    }
}
