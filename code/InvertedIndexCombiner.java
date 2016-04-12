package invertedindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;  

public class InvertedIndexCombiner extends Reducer<Text, IntWritable> {
	
	Text info = new Text();  
      
    @Override  
    public void reduce(Text key, Iterable values, Context context)  
            throws IOException, InterruptedException {  
        // 計算詞頻  
        int sum = 0;  
        for(Text value:values) sum+=Integer.valueOf(value.toString());  
  
        // Term+FileName  
        String items[] = key.toString().split(":");  
          
        // 重新設置 value 值為由 文檔名稱+詞頻  
        info.set(String.format("%s:%d", items[1], sum));  
          
        // 重新設置 key 值為單詞  
        key.set(items[0]);  
        context.write(key, info);  
    }
}