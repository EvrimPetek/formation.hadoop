package org.formation.mapreduce;

import com.google.common.primitives.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.yarn.api.records.URL;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
//import java.nio.file.Files;


public class RegroupRecordReader extends RecordReader<Text,BytesWritable>{
    private boolean next=true;
    private Text text= new Text();
    private BytesWritable bw=new BytesWritable();
    //private Mapper.Context ct;
    private FileSplit fileSplit;
    private byte[] contain;
    TaskAttemptContext taskAttemptContext;


    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        //bw.set(inputSplit.toString().getBytes(),0,inputSplit.getLength());
        //inputSplit.toString();
        fileSplit = (FileSplit) (inputSplit);

        this.taskAttemptContext=taskAttemptContext;


    }

    public boolean nextKeyValue() throws IOException{

        if(next) {

            text.set(fileSplit.getPath().getName());

            Configuration conf =taskAttemptContext.getConfiguration();
            FileSystem fs = FileSystem.get(fileSplit.getPath().toUri(), conf);
            InputStream in = null;
            contain=new byte[(int)fileSplit.getLength()];
            try {
                in = fs.open(fileSplit.getPath());
                IOUtils.readFully(in, contain,0, (int)fileSplit.getLength());
            } finally {
                IOUtils.closeStream(in);
            }
            next = false;
            bw.set(contain,0,contain.length);

            return true;
        }
        else
            return false;
    }

    public Text getCurrentKey(){
        if(next)
            return null;
        else
            return text;
    }
    public BytesWritable getCurrentValue(){
        if(next)
            return null;
        else
            return bw;
    }

    public float getProgress() throws IOException, InterruptedException {
        if(next)
            return 0;
        else
            return 1;
    }

    public void close() throws IOException {

    }
}
