package com.kiryumiya.scorecount.highestAvgScore;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class highestAvgScoreReducer extends Reducer<Text, Text, Text, Text> {

    private final Text result = new Text();
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String highestStudent = "";
        float highestAvgScore = 0;

        for (Text value : values) {
            String[] studentInfo = value.toString().split(",");
            String studentName = studentInfo[0];
            float avgScore = Float.parseFloat(studentInfo[1]);

            if (avgScore > highestAvgScore) {
                highestAvgScore = avgScore;
                highestStudent = studentName;
            }
        }
        result.set(highestStudent + "\t" + highestAvgScore);
        context.write(key, result);
    }
}
