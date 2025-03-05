package com.kiryumiya.scorecount.singleAvgScore;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class SingleAvgScoreCountReducer extends Reducer<AvgScoreBean, Text, Text, Text> {

    private final DecimalFormat df = new DecimalFormat("#.0");
    private final Text studentName = new Text();
    private final Text score = new Text();

    @Override
    protected void reduce(AvgScoreBean scoreBean, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        studentName.set(scoreBean.getStudentName());
        score.set(df.format(scoreBean.getAvgScore()));
        context.write(studentName, score);
    }
}
