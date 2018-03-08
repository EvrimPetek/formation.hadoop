package org.formation.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.StringTokenizer;

public class MapFiltre extends Mapper<LongWritable,Text,PairInput,IntWritable> {

    private IntWritable iw=new IntWritable(1);
    private PairInput pair= new PairInput();
    //private Context ct;
    HashSet<String> filtre;

    public void filtre() throws FileNotFoundException {
        Scanner file = new Scanner(new File("stop"));//hdfs:///user/diginamic/input/stopwords_en.txt"));//stop"));

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
    protected void map(LongWritable key, Text value, Context ct) throws IOException, InterruptedException {
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
                ct.getCounter(COUNTERS.ERREUR_FORMAT_LINE).increment(1);
                pair.setMot(new Text(aux));
                //txt.set(res);
                ct.write(pair, iw);
            }
        }
    }
}
