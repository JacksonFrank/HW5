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
import java.util.*;

public class WordFrequencyCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable result = new IntWritable();
    private Text word = new Text();
    private final ArrayList<String> bannedWords = new ArrayList<String>(Arrays.asList("he", "she", "they", "the", "a", "an", "are", "you", "of", "is", "and", "or"));
    private HashMap<String, Integer> frequencies = new HashMap<String, Integer>();
    private ArrayList<String> topFiveWords = new ArrayList<String>();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        String currentWord = itr.nextToken().toLowerCase();
        if (!bannedWords.contains(currentWord)) {
            if (!frequencies.containsKey(currentWord)) {
                frequencies.put(currentWord, 1);
            } else {
                frequencies.put(currentWord, frequencies.get(currentWord) + 1);
            }
        }
      }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        for (String currentWord : frequencies.keySet()) {
             if (topFiveWords.size() == 0) {
                    topFiveWords.add(currentWord);
            } else {
                int i;
                int sum = frequencies.get(currentWord);
                for (i = topFiveWords.size() - 1; i >= 0; i--) {
                    if (sum < frequencies.get(topFiveWords.get(i))) {
                        break;
                    } else if (sum == frequencies.get(topFiveWords.get(i)) && currentWord.compareTo(topFiveWords.get(i)) > 0) {
                        break;
                    }
                }
                i += 1;
                if (i < 5) {
                    topFiveWords.add(i, currentWord);
                    if (topFiveWords.size() > 5) {
                        topFiveWords.remove(5);
                    }
                }
            }
        }
        for (String strWord : topFiveWords) {
            word.set(strWord);
            result.set(frequencies.get(strWord));
            context.write(word, result);
        }
    } 
  }

  public static class IntTopFiveReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    private HashMap<String, Integer> frequencies = new HashMap<String, Integer>();
    private ArrayList<String> topFiveWords = new ArrayList<String>();
    private Text word = new Text();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException { 
        String currentWord = key.toString();
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        frequencies.put(currentWord, sum);
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        for (String currentWord : frequencies.keySet()) {
             if (topFiveWords.size() == 0) {
                    topFiveWords.add(currentWord);
            } else {
                int i;
                int sum = frequencies.get(currentWord);
                for (i = topFiveWords.size() - 1; i >= 0; i--) {
                    if (sum < frequencies.get(topFiveWords.get(i))) {
                        break;
                    } else if (sum == frequencies.get(topFiveWords.get(i)) && currentWord.compareTo(topFiveWords.get(i)) > 0) {
                        break;
                    }
                }
                i += 1;
                if (i < 5) {
                    topFiveWords.add(i, currentWord);
                    if (topFiveWords.size() > 5) {
                        topFiveWords.remove(5);
                    }
                }
            }
        }
        for (String strWord : topFiveWords) {
            word.set(strWord);
            result.set(frequencies.get(strWord));
            context.write(word, result);
        }
    }
  }



  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordFrequencyCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntTopFiveReducer.class);
    job.setReducerClass(IntTopFiveReducer.class);
    job.setNumReduceTasks(1);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}