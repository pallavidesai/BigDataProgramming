package com.evenodd;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class EvenOdd {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
	private MultipleOutputs mos;
		   @Override
    public void setup(Context context) {
        mos = new MultipleOutputs(context);
		   }
		 @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    
    }
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
	int EvenNumber=0,OddNumber=0,Num = 0;
     int sum = 0;
	  String strNumber = " ";
	  
      for (IntWritable val : values) {
		strNumber = key.toString();
		Num = Integer.parseInt(strNumber);
		if(Num % 2 == 0){
			sum+=val.get();
			EvenNumber = Num;
		}
		else{
			sum+=val.get();   
			OddNumber=Num;
			
		}
        
      }
	  if(OddNumber!=0){
      mos.write("odd", OddNumber, new IntWritable(sum));
	  }
	  else{
	  mos.write("even", EvenNumber, new IntWritable(sum));
	  }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(EvenOdd.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	MultipleOutputs.addNamedOutput(job, "even", TextOutputFormat.class, NullWritable.class, Text.class);
   MultipleOutputs.addNamedOutput(job, "odd", TextOutputFormat.class, NullWritable.class, Text.class);
   
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}