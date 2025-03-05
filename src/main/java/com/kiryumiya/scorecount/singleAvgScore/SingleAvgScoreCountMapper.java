package com.kiryumiya.scorecount.singleAvgScore;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DecimalFormat;

public class SingleAvgScoreCountMapper extends Mapper<LongWritable, Text, AvgScoreBean, Text>{

    private final AvgScoreBean avgScore = new AvgScoreBean();
    private final Text outV = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] vals = line.split(",");

        outV.set(vals[1]);

        int sum = 0;
        int examNum = 0;
        for(int i = 2; i < vals.length; i++) {
            examNum += 1;
            sum += Integer.parseInt(vals[i]);
        }
        avgScore.setSubject(vals[0]);
        avgScore.setStudentName(vals[1]);
        avgScore.setAvgScore((float)sum / examNum);

        context.write(avgScore, outV);
    }
}
