package org.formation.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import java.io.IOException;
        import java.util.StringTokenizer;


public class Map3 extends Mapper<Object,Text,Text,IntWritable> {

    private IntWritable iw=new IntWritable(1);
    private Text txt = new Text();
    //private Context ct;

    @Override
    protected void map(Object key, Text value, Context ct) throws IOException, InterruptedException {
        //super.map(key, value, context);
        StringTokenizer st = new StringTokenizer(value.toString());
        String res="";
        String aux="";
        int bool=0;
        String client=st.nextToken();

        while (st.hasMoreTokens()) {
            aux = st.nextToken();

            if (bool == 0) {
                if ((aux.substring(aux.length()-1, aux.length())).equals("\"")) {
                    //res = res + aux.substring(1, aux.length());
                    bool = 1;
                }
            }
            else{
                res=aux;
                bool=0;
                break;
            }
        }
        txt.set(client);
        System.out.println("laa : "+res);
        if(!res.equals("") && !res.equals("-") && !res.equals("HTTP/1.0\"") && Integer.parseInt(res)==404) {
            ct.write(txt, iw);
        }
    }
}