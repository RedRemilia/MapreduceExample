package com.kiryumiya.scorecount.singleAvgScore;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class subjectPartitioner extends Partitioner<AvgScoreBean, Text> {
    @Override
    public int getPartition(AvgScoreBean avgScore, Text text2, int i) {
        String subject = avgScore.getSubject();
        int partition;
        switch (subject) {
            case "algorithm":
                partition = 0;
                break;
            case "computer":
                partition = 1;
                break;
            case "english":
                partition = 2;
                break;
            default:
                partition = 3;
                break;
        }
        return partition;
    }


}
