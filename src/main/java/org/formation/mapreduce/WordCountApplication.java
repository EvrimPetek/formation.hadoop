package org.formation.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFilter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.fs.Path;


import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class WordCountApplication
{
    private Configuration conf;
    private Job job;

    public WordCountApplication(){

    }

    public void apply(String sin, String sout,Class outK,Class outV,Class map,Class red) throws IOException,ClassNotFoundException,URISyntaxException{

        try {
            conf = new Configuration();
            job = Job.getInstance(conf, "Arbeit");
            job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);//RegroupFileInputFormat.class);//;
            job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);//SequenceFileOutputFormat.class);//;

            job.addCacheFile(new URI("hdfs:///user/diginamic/input/stopwords_en.txt#stop"));


            Path inputFilePath = new Path(sin);
            Path outputFilePath = new Path(sout);
            FileInputFormat.addInputPath(job, inputFilePath); //new JobConf(conf),
            FileOutputFormat.setOutputPath(job, outputFilePath);

            FileSystem fs = FileSystem.newInstance(conf);
            if (fs.exists(outputFilePath)) {
                fs.delete(outputFilePath, true);
            }
            job.setOutputKeyClass(outK);//org.apache.hadoop.io.IntWritable.class);//org.apache.hadoop.io.Text.class);
            job.setOutputValueClass(outV);//org.apache.hadoop.io.Text.class);//org.apache.hadoop.io.IntWritable.class);
            job.setMapperClass(map);//org.formation.mapreduce.Map2.class);
            job.setReducerClass(red);//org.apache.hadoop.mapreduce.Reducer.class);

            job.setPartitionerClass(PartitionerPair.class);
            job.setGroupingComparatorClass(GroupingComparator.class);
            //job.setSortComparatorClass();


            job.setJarByClass(WordCountApplication.class);
            job.waitForCompletion(true);
            Counters counters=job.getCounters();
            System.out.printf("Nombre d'ajout: %d",counters.findCounter(COUNTERS.ERREUR_FORMAT_LINE).getValue());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Job getJob() {
        return job;
    }

    public void setJob(Job job) {
        this.job = job;
    }

    public void addarg(String s1, String s2){
       // job.setInputPath();
        //job.setOutputPath();

    }
    public static void main( String[] args ) throws IOException,ClassNotFoundException,URISyntaxException
    {

        System.out.println( "Hello World!" );
        WordCountApplication wca=new WordCountApplication();

        wca.apply(args[0],args[1],org.formation.mapreduce.PairInput.class,org.apache.hadoop.io.IntWritable.class,
                org.formation.mapreduce.MapFiltre.class, org.apache.hadoop.mapreduce.Reducer.class);

        //wca.apply(args[0],args[1],org.apache.hadoop.io.Text.class,org.apache.hadoop.io.BytesWritable.class,
        //        org.apache.hadoop.mapreduce.Mapper.class, org.apache.hadoop.mapreduce.Reducer.class);
        //wca.apply(args[2],args[3],org.apache.hadoop.io.IntWritable.class,org.apache.hadoop.io.Text.class,
        //        org.formation.mapreduce.Map2.class, org.apache.hadoop.mapreduce.Reducer.class);

        //



    }
}
