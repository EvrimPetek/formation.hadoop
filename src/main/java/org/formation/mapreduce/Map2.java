package org.formation.mapreduce;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.StringTokenizer;


public class Map2 extends Mapper<Object,Text,IntWritable,Text>{

    //private IntWritable iw=new IntWritable(1);
    private Text txt = new Text();
    private IntWritable iw=new IntWritable(1);
    //private Context ct;

    @Override
    protected void map(Object key, Text value, Context ct) throws IOException, InterruptedException {
        //super.map(key, value, context);
        try{
        StringTokenizer st = new StringTokenizer(value.toString());
        String url=st.nextToken();
        int aux=Integer.parseInt(st.nextToken());
        //System.out.println(url+ " "+aux);

        txt.set(url);
        iw.set(aux);
        ct.write(iw,txt);

        }catch(Exception e) {
            System.out.println("bout : "+e);
        }
    }
}