import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
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

  // Mapper class
  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] columns = value.toString().split(","); // Split the input line by comma
      if (columns.length >= 10) { // Check if the desired column exists
        String column10 = columns[9]; // Get the value of the 10th column

        // Remove punctuation and numbers from the input string using regular expression
        String cleanedColumn10 = column10.replaceAll("[^a-zA-Z\\s]", "");

        StringTokenizer tokenizer = new StringTokenizer(cleanedColumn10);
        while (tokenizer.hasMoreTokens()) {
          String token = tokenizer.nextToken().toLowerCase(); // Convert to lowercase
          if (token.length() > 1 && !hasRepeatedCharacters(token)) { // Skip single characters and repeated characters
            word.set(token);
            context.write(word, one);
          }
        }
      }
    }

    private boolean hasRepeatedCharacters(String str) {
      int repeatedCount = 0;
      for (int i = 0; i < str.length() - 1; i++) {
        if (str.charAt(i) == str.charAt(i + 1)) {
          repeatedCount++;
          if (repeatedCount >= 2) {
            return true;
          }
        } else {
          repeatedCount = 0;
        }
      }
      return false;
    }
  }

  // Reducer class
  public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private Map<Text, IntWritable> countMap = new HashMap<>();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      countMap.put(new Text(key), new IntWritable(sum));
    }

    // Cleanup method to sort the countMap and write sorted results to the output
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      // Use TreeMap to sort the countMap in descending order
      TreeMap<Text, IntWritable> sortedMap = new TreeMap<>((a, b) -> {
        int cmp = countMap.get(b).compareTo(countMap.get(a));
        if (cmp == 0) {
          return a.compareTo(b);
        }
        return cmp;
      });

      sortedMap.putAll(countMap);

      // Write the sorted results to the output
      for (Text key : sortedMap.keySet()) {
        context.write(key, sortedMap.get(key));
      }
    }
  }

  // Main method
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Word Count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.setInputPaths(job, new Path(args[0])); // Input file path
    FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output directory path

    long startTime = System.currentTimeMillis();
    boolean jobResult = job.waitForCompletion(true);
    long endTime = System.currentTimeMillis();

    long executionTime = endTime - startTime;
    System.out.println("MapReduce Job Execution Time: " + executionTime + " milliseconds");

    System.exit(jobResult ? 0 : 1);
  }
}
