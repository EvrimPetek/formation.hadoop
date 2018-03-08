package org.formation.mapreduce;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;


public class Map extends Mapper<Object,Text,PairInput,IntWritable>{

    private IntWritable iw=new IntWritable(1);
    private PairInput pair= new PairInput();
    //private Context ct;
    HashSet<String> filtre;

    public void filtre() throws FileNotFoundException{
        Scanner file = new Scanner(new File("C:\\Users\\diginamic\\Downloads\\tpMapReduce\\tp2\\tp2-mp\\stopwords_en.txt"));
        String aux;
        filtre = new HashSet<String>();
// For each word in the input
        while (file.hasNext()) {
            // Convert the word to lower case, trim it and insert into the set
            // In this step, you will probably want to remove punctuation marks
            aux=file.next().trim().toLowerCase();
            if(aux.length()>=3)
                filtre.add(aux);
        }
    }
    @Override
    protected void map(Object key, Text value, Context ct) throws IOException, InterruptedException {
        //super.map(key, value, context);
        StringTokenizer st = new StringTokenizer(value.toString()," '\"!?,.()[]+-_0123456789<>;:");
        filtre();
        FileSplit fileSplit = (FileSplit)ct.getInputSplit();
        String filename = fileSplit.getPath().getName();

        pair.setDoc_id(new Text(filename));
        String res="";
        String aux="";
        int bool=0;
        while (st.hasMoreTokens()) {
            aux = st.nextToken().toLowerCase();
            if(aux.length()>=3 && !filtre.contains((String)aux)) {
                pair.setMot(new Text(aux));
                //txt.set(res);
                ct.write(pair, iw);
            }
        }
    }
}
