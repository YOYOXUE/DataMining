import java.io.IOException;
import java.util.StringTokenizer;
import java.text.DecimalFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.*;
public class Yidan_Xue_Average {
  public static class TokenizerMapper
       extends Mapper<LongWritable, Text, Text, IntWritable>{

    //private final static IntWritable one = new IntWritable(1);
    // private Text word = new Text();
    private static IntWritable data=new IntWritable();
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      if (key.get()>0){
        try{
          StringTokenizer itr = new StringTokenizer(value.toString(),",");
          String[] list=value.toString().split(",");
          String evt=list[3];
          String page_count=list[18];
          String event ="";
          // String y=itr.nextToken();
          // System.out.println(y);
          if (evt.length()>0){
            event=evt.toLowerCase().trim();
            event=event.replaceAll("-","");
            event=event.replaceAll("'","");
            event=event.replaceAll("[^\\w]"," ");
            event=event.replaceAll("\\s+"," ");
            event=event.trim();
            if (!event.equals("")){
              data.set(Integer.parseInt(page_count));
              context.write(new Text(event),data);
            }
          }
        }
        catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }
  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      int count=0;
      DecimalFormat df=new DecimalFormat("0.000");
      for (IntWritable val : values) {
        sum += val.get();
        count+=1;
      }
      Double su= new Double((double)sum);
      Double c= new Double((double)count);
      String s=String.valueOf(count)+"\t"+String.valueOf(df.format((su/c)));
      result.set(s);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "Average");
    job.setJarByClass(Yidan_Xue_Average.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
