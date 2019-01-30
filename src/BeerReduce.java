import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.File;

import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Collections;

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

  //=================================================================================================
  // Utils
  //=================================================================================================
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

  static boolean deleteDirectory(File _dir) {
    File[] allContents = _dir.listFiles();
    if (allContents != null) {
        for (File file : allContents) {
            deleteDirectory(file);
        }
    }
    return _dir.delete();
  }

  private final static int s_ARG_ERROR = 1;
  private final static int s_FIRST_JOB_ERROR = 2;
  private final static int s_SECOND_JOB_ERROR = 3;

  private final static String s_TMP_REDUCER_SUFFIX = "_tmp";

  private final static String s_SPLIT = ";";
  private final static String s_INTERNAL_SPLIT = ",";

  private final static int s_BEER_STYLE = 3;
  private final static int s_BEER_SUGAR = 14;
  private final static int s_BEER_BREWING = 17;

  public static enum BeerFamily
  {
    ALE,
    IPA,
    STOUT,
    LAGER,
    PORTER,
    BITTER,
    CIDER,
    UNKNOWN;

    public static BeerFamily isTypeOf(String _s) {
        if (_s.contains("ALE")) {
            return ALE;
        } else if (_s.contains("IPA")) {
            return IPA;
        } else if (_s.contains("STOUT")) {
            return STOUT;
        } else if (_s.contains("LAGER")) {
            return LAGER;
        } else if (_s.contains("PORTER")) {
            return PORTER;
        } else if (_s.contains("BITTER")) {
            return BITTER;
        } else if (_s.contains("CIDER")) {
            return CIDER;
        }
        return UNKNOWN;
    }

    public static String toString(BeerFamily _bf) {
        switch(_bf)
        {
          case ALE:
            return "ALE";
          case IPA:
            return "IPA";
          case STOUT:
            return "STOUT";
          case LAGER:
            return "LAGER";
          case PORTER:
            return "PORTER";
          case BITTER:
            return "BITTER";
          case CIDER:
            return "CIDER";
        }
        return "UNKNOWN";
    }
  };

  //=================================================================================================
  // First job
  //=================================================================================================
  public static class StyleMapper extends Mapper<Object, Text, Text, Store>{
      
    public void map(Object _key, Text _value, Context _context) throws IOException, InterruptedException {
      String[] line = _value.toString().split(s_SPLIT);
      try {
        int sugar = Integer.parseInt(line[s_BEER_SUGAR]);
        _context.write(new Text(BeerFamily.toString(BeerFamily.isTypeOf(line[s_BEER_STYLE].toUpperCase()))), new Store(sugar, line[s_BEER_BREWING]));
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

  //=================================================================================================
  // Second job
  //=================================================================================================
  public static class StyleRemapper extends Mapper<Object, Text, Text, Text>{
      
    public void map(Object _key, Text _value, Context _context) throws IOException, InterruptedException {
      String[] line = _value.toString().split(s_SPLIT);
      String[] brewing = line[1].split(s_INTERNAL_SPLIT);
      for(String brew : brewing)
        _context.write(new Text(line[0]), new Text(brew));
    }

  }

  public static class BestBrewingReducer extends Reducer<Text,Text,Text,Text> {
    
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      ArrayList<String> arr = new ArrayList();
      for (Text val : values) {
          arr.add(val.toString());
      }

      String bestBrewing = "";
      int max = 0;
      for(String s : arr)
      {
        int tmp = Collections.frequency(arr,s);
        if(tmp > max)
        {
          max = tmp;
          bestBrewing = s;
        }
      }

      context.write(key, new Text(bestBrewing));
    }

  }

  //=================================================================================================
  // Main
  //=================================================================================================
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 2) {
      System.err.println("Usage: BeerReduce <in> <out>");
      System.exit(s_ARG_ERROR);
    }

    Path tmpDir = new Path(otherArgs[1]+s_TMP_REDUCER_SUFFIX);
    boolean success;
    {
      // Create the job
      Job job = new Job(conf, "BeerBrewingList");
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
      FileOutputFormat.setOutputPath(job, tmpDir);

      // Run the job and wait for it
      success = job.waitForCompletion(true);
    }

    if(success)
    {
      // Create the job
      Job job = new Job(conf, "BeerBrewingBest");
      job.setJarByClass(BeerReduce.class);

      // Mapper and reducer classes
      job.setMapperClass(StyleRemapper.class);
      job.setReducerClass(BestBrewingReducer.class);

      // Mapper output
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);

      // Reducer output
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      // Set input and output data
      FileInputFormat.addInputPath(job, tmpDir);
      FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

      // Run the job and wait for it
      success = job.waitForCompletion(true);
    } else {
      deleteDirectory(java.nio.file.Paths.get(tmpDir.toString()).toFile());
      System.exit(s_FIRST_JOB_ERROR);
    }

    deleteDirectory(java.nio.file.Paths.get(tmpDir.toString()).toFile());

    if(success) {
      System.exit(0);
    } else {
      System.exit(s_SECOND_JOB_ERROR);
    }
  }

}
