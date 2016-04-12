package invertedindex;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;  

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private IntWritable one = new IntWritable(1);
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Text keyInfo = new Text();      // Save Term+FileName  
    	Text valueInfo = new Text("1"); // Save Term frequency        
		// we simply use StringTokenizer to split the words for us.
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			
			String fn = ((FileSplit) context.getInputSplit()).getPath().getName();  
            keyInfo.set(String.format("%s:%s", iter.nextToken(), fn));  
            context.write(keyInfo, valueInfo); 

		}
		
	}
	
}
