import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BeerReduce {

  private final static String s_SPLIT = ";";

  private final static int s_BEER_ID = 0;
  private final static int s_BEER_STYLE= 3;
  private final static int s_BEER_SUGAR= 4;

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
      
    public void map(Object _key, Text _value, Context _context) throws IOException, InterruptedException {
      String[] line = _value.toString().split(s_SPLIT);
      _context.write(new Text(line[s_BEER_STYLE]), new IntWritable(1));
    }

  }
  
  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    FileSystem fs = FileSystem.newInstance(conf);

    if (otherArgs.length != 2) {
      System.err.println("Usage: BeerReduce <in> <out>");
      System.exit(2);
    }

    // Create the job
    Job job = new Job(conf, "Beer");
    job.setJarByClass(BeerReduce.class);

    // Mapper and reducer classes
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);

    // Mapper output
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    // Reducer output
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // Set input and output data
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    // Run the job and wait for it
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
