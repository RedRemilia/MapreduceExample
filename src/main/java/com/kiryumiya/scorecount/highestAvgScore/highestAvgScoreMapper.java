package com.kiryumiya.scorecount.highestAvgScore;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DecimalFormat;

public class highestAvgScoreMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text subject = new Text();
    private final Text studentScore = new Text();
    private final DecimalFormat df = new DecimalFormat("#.00");

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] vals = line.split(",");
        subject.set(vals[0]);
        String studentName = vals[1];
        int totalScore = 0;
        int examCount = 0;
        for (int i = 2; i < vals.length; i++) {
            totalScore += Integer.parseInt(vals[i]);
            examCount++;
        }
        studentScore.set(studentName + "," + df.format((float) totalScore / examCount));
        context.write(subject, studentScore);
    }
}
