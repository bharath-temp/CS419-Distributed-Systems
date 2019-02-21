//Code by Bharath Padmaraju: Inspired by Apache Mapreduce Tutorial
//https://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html#Example%3A+WordCount+v2.0

//This code set is very similar to the WordCount program

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
import java.util.regex.Pattern;
import java.util.Arrays;

public class BeavMart {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
		String sent = value.toString();
	
		String items[] = sent.split(",");
		
		Arrays.sort(items);
		
		//it seems like map reduce processes one sentence per map, here we map pairs of values and initialize each key with value one
		for(int i = 0; i < items.length - 1; i++) {
			String itemOne = items[i];
			for(int j = i + 1; j < items.length; j++ ) {
				String itemTwo = items[j];
				Text list = new Text("(" + itemOne + ", " + itemTwo + ") : ");
				context.write(list,one);
			}
		}
      }
    }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    private int uniquePairs;
	       
   public void setup(Context context) {
	//Setting default values to zeros.
	uniquePairs = 0;
    }

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
	       
		//unique pairs can be calculated after reduce calculates the sums
		++uniquePairs;
	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(new Text("Unique Item Pairs: "), new IntWritable(uniquePairs));
        }
}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Beav Mart");
    job.setJarByClass(BeavMart.class);
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
