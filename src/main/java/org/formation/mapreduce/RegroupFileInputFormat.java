package org.formation.mapreduce;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import org.apache.hadoop.fs.Path;

public class RegroupFileInputFormat extends FileInputFormat<Text, BytesWritable> {
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new RegroupRecordReader();
    }
    protected boolean isSplitable(FileSystem fs, Path filename){
        return false;
    }
}
