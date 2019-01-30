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
    }

    public Store(String _name, double _mash) {
      m_name = _name;
      m_mash = _mash;
    }

    public String getName() {
      return m_name;
    }

    public double getMash() {
      return m_mash;
    }

    public void readFields(DataInput _in) throws IOException {
        m_name = _in.readLine();
        m_mash = _in.readDouble();
    }

    public void write(DataOutput _out) throws IOException {
        _out.writeBytes(m_name+"\n");
        _out.writeDouble(m_mash);
    }

    private String m_name;
    private double m_mash;

  }

  private final static int s_ARG_ERROR = 1;
  private final static int s_FIRST_JOB_ERROR = 2;

  private final static String s_SPLIT = ";";

  private final static int s_BEER_NAME = 1;
  private final static int s_BEER_COLOR = 10;
  private final static int s_BEER_MASH = 15;

  //=================================================================================================
  // First job
  //=================================================================================================
  public static class ColorMapper extends Mapper<Object, Text, DoubleWritable, Store> {
      
    public void map(Object _key, Text _value, Context _context) throws IOException, InterruptedException {
      String[] line = _value.toString().split(s_SPLIT);
      try {
        double color = Double.parseDouble(line[s_BEER_COLOR]);
        double mash = Double.parseDouble(line[s_BEER_MASH]);
        _context.write(new DoubleWritable(color), new Store(line[s_BEER_NAME], mash));
      } catch(Throwable _e) {
      }
    }

  }
  
  public static class MashReducer extends Reducer<DoubleWritable,Store,Text,Text> {
    
    public void reduce(DoubleWritable _key, Iterable<Store> _values, Context _context) throws IOException, InterruptedException {
      double min = Double.POSITIVE_INFINITY;;
      String names = "";

      for (Store val : _values) {
        if(val.getMash() < min) {
          min = val.getMash();
          names = val.getMash() + " : " + val.getName();
        } else if (val.getMash() == min) {
          names += ", " + val.getName();
        }
      }

      _context.write(new Text(""+_key), new Text(names));
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
      FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

      // Run the job and wait for it
      success = job.waitForCompletion(true);
    }

    if(success) {
      System.exit(0);
    } else {
      System.exit(s_FIRST_JOB_ERROR);
    }
  }

}
