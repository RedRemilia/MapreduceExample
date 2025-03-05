package com.kiryumiya.scorecount.singleAvgScore;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AvgScoreBean implements WritableComparable<AvgScoreBean> {

    private String subject;
    private String studentName;
    private float avgScore;

    public AvgScoreBean() {

    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getStudentName() {
        return studentName;
    }

    public void setStudentName(String studentName) {
        this.studentName = studentName;
    }

    public float getAvgScore() {
        return avgScore;
    }

    public void setAvgScore(float avgScore) {
        this.avgScore = avgScore;
    }

    @Override
    public int compareTo(AvgScoreBean o) {
        return Float.compare(o.avgScore, this.avgScore);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(subject);
        out.writeUTF(studentName);
        out.writeFloat(avgScore);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        subject = in.readUTF();
        studentName = in.readUTF();
        avgScore = in.readFloat();
    }
}
