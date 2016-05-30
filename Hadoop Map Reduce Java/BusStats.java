import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class BusStats
{
  public static class Map extends Mapper<LongWritable, Text, Text, Text>
  {
    public void map(LongWritable key, Text value, Context context) throws
            IOException, InterruptedException
    {
      String[] data = value.toString().toLowerCase().split(",");
      if((data.length == 17) && !data[0].equals("time"))
      {
         context.write(new Text(data[1] + ";" + data[0].substring(11, 13)),
                       new Text(data[8]));
      }
    }

  }

  public static class Reduce extends Reducer<Text, Text, Text, Text>
  {

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
    {
      ArrayList<Double> delays = new ArrayList<Double>();
      Double mean = 0.0;
      int i = 0;
      for(Text value : values)
      {
        Double delay = Double.parseDouble(value.toString());
        delays.add(delay);
        mean += delay;
        i += 1;
      }
      if(i > 0)
      {
        mean = mean / i;
        Double var = 0.0;
        for(Double delay : delays)
        {
          var += (delay - mean)*(delay - mean);
        }
        var = var / i;
        context.write(null, new Text(key.toString() + ";" + mean + ";" + var));
      }
    }

  }

  public static void main(String[] args) throws Exception
  {
    Job job = Job.getInstance(new Configuration(), "BusStats");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setJarByClass(BusStats.class);
    job.waitForCompletion(true);
  }

}

