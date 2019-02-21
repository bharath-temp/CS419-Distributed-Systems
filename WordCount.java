//Code by Bharath Padmaraju: Inspired by Apache Mapreduce Tutorial
//https://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html#Example%3A+WordCount+v2.0


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString().toUpperCase().trim());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    private int uniqueWordCount;
    private int totalWordCount;

	       
    public void setup(Context context) {
	//Setting default values to zeros.
	uniqueWordCount = 0;
	totalWordCount = 0;
    }

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
	//Here we see the total number of values as reported by map which sets each key:word to map to 1. 
	++totalWordCount;
      }
      result.set(sum);
      context.write(key, result);
      //Here we increment unique word count because the number of reduces simplifies the redundencies of map.
      ++uniqueWordCount;
    }
    
     protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("Unique Word Count: "), new IntWritable(uniqueWordCount));
	context.write(new Text("Total Word Count: "), new IntWritable(totalWordCount));

    }
    
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
	  
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    //job.setReducerClass(IntSumReducer.class);
	  
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
	  
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	  
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
