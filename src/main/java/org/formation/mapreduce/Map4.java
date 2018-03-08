package org.formation.mapreduce;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.StringTokenizer;

import java.io.IOException;
import java.util.StringTokenizer;


public class Map4 extends Mapper<Object,Text,Text,LongWritable> {

    private DateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
    private Date date;
    private LongWritable lw=new LongWritable(1);
    private Text txt = new Text();
    private Text txt1=new Text();
    //private Context ct;

    @Override
    protected void map(Object key, Text value, Context ct) throws IOException, InterruptedException {
        //super.map(key, value, context);
        StringTokenizer st = new StringTokenizer(value.toString());
        String res="";
        String aux="";
        int bool=0;
        String client=st.nextToken();

        try {

            while (st.hasMoreTokens()) {
                aux = st.nextToken();

                if (bool == 0) {
                    if (aux.substring(0, 1).equals("[")) {
                        res = aux.substring(1, aux.length());
                        //date = df.parse(res);
                        bool = 1;
                        break;
                    }
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
        txt.set(client);
        txt1.set(res);
        System.out.println("laa : "+res);
        //lw.set(date.getTime());
        //date=null;
        ct.write(txt, lw);

    }
}