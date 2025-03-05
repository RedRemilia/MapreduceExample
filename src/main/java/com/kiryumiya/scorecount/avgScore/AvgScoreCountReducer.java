package com.kiryumiya.scorecount.avgScore;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class AvgScoreCountReducer extends Reducer<Text, Text, Text, Text> {

    private final DecimalFormat df = new DecimalFormat("#.00");

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long totalScores = 0;
        int studentNums = 0;
        int examNums = 0;

        for (Text scores : values) {
            studentNums++;
            String[] scoreList = scores.toString().split(",");
            for (String scoreStr : scoreList) {
                examNums++;
                totalScores += Integer.parseInt(scoreStr);
            }
        }

        String avgScore = df.format((float)totalScores / examNums);
        context.write(key, new Text(studentNums + ", " + avgScore));
    }
}
