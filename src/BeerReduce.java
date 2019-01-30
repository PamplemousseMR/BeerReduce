import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;

public class BeerReduce {

  private static final class Store implements Writable {

    public Store() {
      m_int = 0;
      m_text = "";
    }

    public Store(int _int, String _text) {
      m_int = _int;
      m_text = _text;
    }

    public int getInt() {
      return m_int;
    }

    public String getText() {
      return m_text;
    }

    public void readFields(DataInput _in) throws IOException {
        m_int = _in.readInt();
        m_text = _in.readLine();
    }

    public void write(DataOutput _out) throws IOException {
        _out.writeInt(m_int);
        _out.writeBytes(m_text);
    }

    private int m_int;
    private String m_text;

  }

  private final static String s_SPLIT = ";";
  private final static String s_INTERNAL_SPLIT = ",";

  private final static int s_BEER_STYLE = 3;
  private final static int s_BEER_SUGAR = 14;
  private final static int s_BEER_BREWING = 17;

  public static class StyleMapper extends Mapper<Object, Text, Text, Store>{
      
    public void map(Object _key, Text _value, Context _context) throws IOException, InterruptedException {
      String[] line = _value.toString().split(s_SPLIT);
      try {
        int sugar = Integer.parseInt(line[s_BEER_SUGAR]);
        _context.write(new Text(line[s_BEER_STYLE]), new Store(sugar, line[s_BEER_BREWING]));
      } catch(Throwable _e) {
      }
    }

  }
  
  public static class BrewingReducer extends Reducer<Text,Store,Text,Text> {
    
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<Store> values, Context context) throws IOException, InterruptedException {
      int max = 0;
      String brewing = "";

      for (Store val : values) {
        if(val.getInt() == max) {
          brewing += (val.getText() + s_INTERNAL_SPLIT);
        } else if(val.getInt() > max){
          max = val.getInt();
          brewing = (s_SPLIT + val.getText() + s_INTERNAL_SPLIT);
        }
      }

      context.write(key, new Text(brewing));
    }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 2) {
      System.err.println("Usage: BeerReduce <in> <out>");
      System.exit(2);
    }

    // Create the job
    Job job = new Job(conf, "Beer");
    job.setJarByClass(BeerReduce.class);

    // Mapper and reducer classes
    job.setMapperClass(StyleMapper.class);
    job.setReducerClass(BrewingReducer.class);

    // Mapper output
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Store.class);

    // Reducer output
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Set input and output data
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    // Run the job and wait for it
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
