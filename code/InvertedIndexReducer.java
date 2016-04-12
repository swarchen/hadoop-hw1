package invertedindex;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	Text result = new Text();  
      
    @Override  
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {  
        StringBuffer fileList = new StringBuffer();  
        for(Text value:values)  
        {  
            fileList.append(String.format("%s;", value));  
        }  
        result.set(fileList.toString());  
        context.write(key, result);  
    }  
}
