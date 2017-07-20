import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Wordcount extends Configured implements Tool{

  public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		int exitCode = ToolRunner.run(conf, new Wordcount(),args);
		System.exit(exitCode);

		
	
  }

@Override
public int run(String[] args) throws Exception {
	if (args.length != 2) {
	      System.out.printf(
	          "Usage: WordCount <input dir> <output dir>\n");
	      System.exit(-1);
	    }
	    Job job = new Job(getConf());
	    job.setJarByClass(Wordcount.class);
	    job.setJobName("Word Count");
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    job.setMapperClass(WordcountMapper.class);
	    job.setReducerClass(WordcountReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    boolean success = job.waitForCompletion(true);
	    System.exit(success ? 0 : 1);	return 0;
}
}