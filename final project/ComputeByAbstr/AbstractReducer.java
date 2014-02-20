import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AbstractReducer extends MapReduceBase implements 
Reducer<Text, Text, Text, Text>{

	@Override
	public void reduce(Text key, Iterator<Text> value,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		
		while (value.hasNext()) {
			Text word = value.next();
			output.collect(key, word);
		}
	}

}
