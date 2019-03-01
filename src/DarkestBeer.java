import java.io.File;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;

public class DarkestBeer {

  //=================================================================================================
  // Utils
  //=================================================================================================
  private static final class Store implements Writable {

    public Store() {
      m_name = "";
      m_mash = 0;
      m_beerFamily = BeerFamily.toString(BeerFamily.UNKNOWN);
    }

    public Store(String _name, double _mash, BeerFamily _beerFamily) {
      m_name = _name;
      m_mash = _mash;
      m_beerFamily = BeerFamily.toString(_beerFamily);
    }

    public String getName() {
      return m_name;
    }

    public double getMash() {
      return m_mash;
    }

    public BeerFamily getBeerFamily() {
      return BeerFamily.isTypeOf(m_beerFamily);
    }

    public void readFields(DataInput _in) throws IOException {
        m_name = _in.readLine();
        m_mash = _in.readDouble();
        m_beerFamily = _in.readLine();
    }

    public void write(DataOutput _out) throws IOException {
        _out.writeBytes(m_name+"\n");
        _out.writeDouble(m_mash);
        _out.writeBytes(m_beerFamily);
    }

    private String m_name;
    private double m_mash;
    private String m_beerFamily;

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
      _s = _s.toUpperCase();
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
  private final static int s_BEER_COLOR = 10;
  private final static int s_BEER_MASH = 15;

  //=================================================================================================
  // First job
  //=================================================================================================
  public static class ColorMapper extends Mapper<Object, Text, DoubleWritable, Store> {

    public void map(Object _key, Text _value, Context _context) throws IOException, InterruptedException {
      String[] line = _value.toString().split(s_CSV_SPLIT);
      try {
        double color = Double.parseDouble(line[s_BEER_COLOR]);
        double mash = Double.parseDouble(line[s_BEER_MASH]);

        int colorInt = (int) color;
        double colorFloat = color - (double)colorInt;

        if (colorFloat == 0.00){
          _context.write(new DoubleWritable(colorInt + 0.00), new Store(line[s_BEER_NAME], mash, BeerFamily.isTypeOf(line[s_BEER_STYLE])));
        }
        else if (colorFloat <= 0.25)
        {
          _context.write(new DoubleWritable(colorInt + 0.25), new Store(line[s_BEER_NAME], mash, BeerFamily.isTypeOf(line[s_BEER_STYLE])));
        }
        else if (colorFloat <= 0.50)
        {
          _context.write(new DoubleWritable(colorInt + 0.50), new Store(line[s_BEER_NAME], mash, BeerFamily.isTypeOf(line[s_BEER_STYLE])));
        }
        else if (colorFloat <= 0.75)
        {
          _context.write(new DoubleWritable(colorInt + 0.75), new Store(line[s_BEER_NAME], mash, BeerFamily.isTypeOf(line[s_BEER_STYLE])));
        }
        else
        {
          _context.write(new DoubleWritable(colorInt + 1.00), new Store(line[s_BEER_NAME], mash, BeerFamily.isTypeOf(line[s_BEER_STYLE])));
        }
      } catch(Throwable _e) {
      }
    }

  }

  public static class MashReducer extends Reducer<DoubleWritable,Store,Text,Text> {

    public void reduce(DoubleWritable _key, Iterable<Store> _values, Context _context) throws IOException, InterruptedException {
      double min = Double.POSITIVE_INFINITY;
      String names = "";

      for (Store val : _values) {
        if(val.getMash() < min) {
          min = val.getMash();
          names = val.getMash() + s_SPLIT + val.getName() + s_SUB_SPLIT + BeerFamily.toString(val.getBeerFamily());
        } else if (val.getMash() == min) {
          names += s_INTERNAL_SPLIT + val.getName() + s_SUB_SPLIT + BeerFamily.toString(val.getBeerFamily());
        }
      }

      _context.write(new Text(String.format("%.2f",_key.get())), new Text(names));
    }

  }

  //=================================================================================================
  // Second job
  //=================================================================================================
  public static class FamilyMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object _key, Text _value, Context _context) throws IOException, InterruptedException {
      String[] line = _value.toString().split(s_SPLIT);
      String[] brews = line[1].split(s_INTERNAL_SPLIT);

      for(String brew : brews){
        String[] args = brew.split(s_SUB_SPLIT);
        _context.write(new Text(args[1]), new Text(args[0]));
      }
    }

  }

  public static class CountReducer extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text _key, Iterable<Text> _values, Context _context) throws IOException, InterruptedException {
      String result = "";
      int nb = 0;

      for (Text val : _values) {
        if(nb != 0){
          result += ", ";
        }
        result += val.toString();
        nb++;
      }

      _context.write(_key, new Text("" + nb + " : " + result));
    }

  }

  //=================================================================================================
  // Main
  //=================================================================================================
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 2) {
      System.err.println("Usage: DarkestBeer <in> <out>");
      System.exit(s_ARG_ERROR);
    }

    Path tmpDir = new Path(otherArgs[1] + s_TMP_REDUCER_SUFFIX);
    boolean success;
    {
      // Create the job
      Job job = new Job(conf, "BeerBrewingList");
      job.setJarByClass(DarkestBeer.class);

      // Mapper and reducer classes
      job.setMapperClass(ColorMapper.class);
      job.setReducerClass(MashReducer.class);

      // Mapper output
      job.setMapOutputKeyClass(DoubleWritable.class);
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
      job.setJarByClass(DarkestBeer.class);

      // Mapper and reducer classes
      job.setMapperClass(FamilyMapper.class);
      job.setReducerClass(CountReducer.class);

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
