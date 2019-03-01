import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;

public class BestIPA {

  //=================================================================================================
  // Utils
  //=================================================================================================
  private static final class Store implements Writable {

    public Store() {
      m_sugar = 0;
      m_brew = "";
      m_beer = "";
    }

    public Store(int _int, String _text, String _beer) {
      m_sugar = _int;
      m_brew = _text;
      m_beer = _beer;
    }

    public int getSugar() {
      return m_sugar;
    }

    public String getBrew() {
      return m_brew;
    }

    public String getBeer() {
      return m_beer;
    }

    public void readFields(DataInput _in) throws IOException {
        m_sugar = _in.readInt();
        m_brew = _in.readLine();
        m_beer = _in.readLine();
    }

    public void write(DataOutput _out) throws IOException {
        _out.writeInt(m_sugar);
        _out.writeBytes(m_brew + "\n");
        _out.writeBytes(m_beer);
    }

    private int m_sugar;
    private String m_brew;
    private String m_beer;

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

  private final static String s_CSV_SPLIT = ";";
  private final static String s_SPLIT = ";;";
  private final static String s_INTERNAL_SPLIT = ",,";
  private final static String s_SUB_SPLIT = "//";

  private final static int s_BEER_NAME = 1;
  private final static int s_BEER_STYLE = 3;
  private final static int s_BEER_SUGAR = 14;
  private final static int s_BEER_BREWING = 17;

  public  enum BeerFamily {
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
  }

  //=================================================================================================
  // First job
  //=================================================================================================
  private static class StyleMapper extends Mapper<Object, Text, Text, Store> {

    public void map(Object _key, Text _value, Context _context) throws IOException, InterruptedException {
      String[] line = _value.toString().split(s_CSV_SPLIT);
      try {
        int sugar = Integer.parseInt(line[s_BEER_SUGAR]);
        _context.write(new Text(BeerFamily.toString(BeerFamily.isTypeOf(line[s_BEER_STYLE].toUpperCase()))), new Store(sugar, line[s_BEER_BREWING], line[s_BEER_NAME]));
      } catch(Throwable _e) {
      }
    }

  }

  private static class BrewingReducer extends Reducer<Text,Store,Text,Text> {

    public void reduce(Text _key, Iterable<Store> _values, Context _context) throws IOException, InterruptedException {
      int max = 0;
      String brewing = "";

      for (Store val : _values) {
        if(val.getSugar() == max) {
          brewing += (val.getBrew() + s_SUB_SPLIT + val.getBeer() + s_INTERNAL_SPLIT);
        } else if(val.getSugar() > max){
          max = val.getSugar();
          brewing = (s_SPLIT + val.getBrew() + s_SUB_SPLIT + val.getBeer() + s_INTERNAL_SPLIT);
        }
      }

      _context.write(_key, new Text(brewing));
    }

  }

  //=================================================================================================
  // Second job
  //=================================================================================================
  private static class StyleRemapper extends Mapper<Object, Text, Text, Text>{
      
    public void map(Object _key, Text _value, Context _context) throws IOException, InterruptedException {
      String[] line = _value.toString().split(s_SPLIT);
      String[] brewing = line[1].split(s_INTERNAL_SPLIT);
      for(String brew : brewing)
        _context.write(new Text(line[0]), new Text(brew));
    }

  }

  private static class BestBrewingReducer extends Reducer<Text,Text,Text,Text> {
    
    public void reduce(Text _key, Iterable<Text> _values, Context _context) throws IOException, InterruptedException {
      ArrayList<String> brew = new ArrayList();
      ArrayList<String> beer = new ArrayList();
      for (Text val : _values) {
          brew.add(val.toString().split(s_SUB_SPLIT)[0]);
          beer.add(val.toString().split(s_SUB_SPLIT)[1]);
      }

      String bestBrewing = "";
      int max = 0;
      for(int i=0 ; i<brew.size() ; ++i)
      {
        int tmp = Collections.frequency(brew, brew.get(i));
        if(tmp > max)
        {
          max = tmp;
          bestBrewing = brew.get(i) + " : " + beer.get(i);
        } else if(tmp == max) {
          bestBrewing += ", " + beer.get(i);
        }
      }

      _context.write(_key, new Text(bestBrewing));
    }

  }

  //=================================================================================================
  // Main
  //=================================================================================================
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 2) {
      System.err.println("Usage: BestIPA <in> <out>");
      System.exit(s_ARG_ERROR);
    }

    Path tmpDir = new Path(otherArgs[1]+s_TMP_REDUCER_SUFFIX);
    boolean success;
    {
      // Create the job
      Job job = new Job(conf, "BeerBrewingList");
      job.setJarByClass(BestIPA.class);

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
      job.setJarByClass(BestIPA.class);

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
      deleteDirectory(java.nio.file.Paths.get(tmpDir.toString()).toFile());
    } else {
      deleteDirectory(java.nio.file.Paths.get(tmpDir.toString()).toFile());
      System.exit(s_FIRST_JOB_ERROR);
    }

    if(success) {
      System.exit(0);
    } else {
      System.exit(s_SECOND_JOB_ERROR);
    }
  }

}
