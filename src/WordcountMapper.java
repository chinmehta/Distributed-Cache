import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	
	
	ArrayList<String> listwords = new ArrayList<String>();

@Override
protected void setup(Context context)
		throws IOException, InterruptedException {
	
	Configuration conf = context.getConfiguration();
	
	// Getting the path of temporary cache file 
    Path [] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
   
    if (null != cacheFiles && cacheFiles.length > 0) {
    	for (Path cachePath : cacheFiles) {
    		
            BufferedReader wordReader = new BufferedReader(
                    new FileReader(cachePath.toString()));
            String line;                       
       
            //Insert the words to list
            while ((line = wordReader.readLine()) != null) {
            	listwords.add(line);
            }
           
            wordReader.close();            
          }
    }
}


@Override
public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
	  
	
	  
  String line = value.toString();
  for (String word : line.split("\\W+")) {
    if (word.length() > 0) {
  	  //cross check the words you wanted analyse which are in list
  	  if(listwords.contains(word))    	  
      context.write(new Text(word), new IntWritable(1));
    }
  }
}
}