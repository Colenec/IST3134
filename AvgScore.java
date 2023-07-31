import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgScore {

  public static class ScoreMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private Text word = new Text();
    private DoubleWritable score = new DoubleWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] tokens = value.toString().split(",");
      if (tokens.length >= 7) {
        String column7 = tokens[6].trim();
        try {
          double scoreValue = Double.parseDouble(column7);
          word.set("average_score");
          score.set(scoreValue);
          context.write(word, score);
        } catch (NumberFormatException e) {
          // Ignore the record if the score is not a valid double value
        }
      }
    }
  }

  public static class AverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
      int count = 0;
      double sum = 0.0;
      for (DoubleWritable val : values) {
        sum += val.get();
        count++;
      }
      if (count > 0) {
        double average = sum / count;
        result.set(average);
        context.write(key, result);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Average Score");

    job.setJarByClass(AvgScore.class);
    job.setMapperClass(ScoreMapper.class);
    job.setReducerClass(AverageReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

    FileInputFormat.setInputPaths(job, new Path(args[0])); // Input file path
    FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output directory path

    // Record start time of the job execution
    long startTime = System.currentTimeMillis();

    boolean jobResult = job.waitForCompletion(true);

    // Record end time of the job execution
    long endTime = System.currentTimeMillis();

    long executionTime = endTime - startTime;
    System.out.println("MapReduce Job Execution Time: " + executionTime + " milliseconds");

    System.exit(jobResult ? 0 : 1);
  }
}
