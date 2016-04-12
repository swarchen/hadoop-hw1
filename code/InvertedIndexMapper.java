package invertedindex;

import java.io.IOException;  
import java.util.StringTokenizer;  
  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.lib.input.FileSplit;  
  
public class InvertedIndexMapper extends Mapper{      
    Text keyInfo = new Text();      // Save Term+FileName  
    Text valueInfo = new Text("1"); // Save Term frequency        
      
    @Override  
    public void map(Object key, Text value, Context context)  
            throws IOException, InterruptedException {        
        StringTokenizer iter = new StringTokenizer(value.toString());  
        while(iter.hasMoreTokens())  
        {  
            String fn = ((FileSplit) context.getInputSplit()).getPath().getName();  
            keyInfo.set(String.format("%s:%s", iter.nextToken(), fn));  
            context.write(keyInfo, valueInfo);  
        }  
    }  
}  