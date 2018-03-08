package org.formation.mapreduce;

import javafx.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.StringTokenizer;

public class MapTfIdf extends Mapper<Object,Text,PairInput,IntWritable>{

    private IntWritable iw=new IntWritable(1);
    private PairInput pair= new PairInput();
    //private PairWord word=new PairWord();
    //private Context ct;

    @Override
    protected void map(Object key, Text value, Context ct) throws IOException, InterruptedException {
        //super.map(key, value, context);
        StringTokenizer st = new StringTokenizer(value.toString());
        pair.setMot(new Text(st.nextToken()));
        pair.setDoc_id(new Text(st.nextToken()));
        iw.set(Integer.parseInt(st.nextToken()));
        ct.write(pair, iw);
    }
}