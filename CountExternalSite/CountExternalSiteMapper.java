
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CountExternalSiteMapper extends MapReduceBase implements
Mapper <LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		
		String line = value.toString();
		boolean start = false;
		
		if(line.trim() == "") {
			return;
		}
		
		if(line.contains("<text")) {
			start = true;
		}
		
		if(start) {
			Pattern pattern = Pattern.compile("\\[(http.*?[^\\[\\]])\\]");
			Matcher matcher = pattern.matcher(line);
			
			while(matcher.find()) {
				//String[] strs = matcher.group(1).split(" ");
				
				String[] strs = matcher.group(1).split("/");

//				if(strs.length != 0) {
//					String website = strs[0];
//					
//					output.collect(new Text(website), new IntWritable(1));
//				}
//
				if(strs.length > 2) {
					String website = strs[0] + "//" + strs[2];
					output.collect(new Text(website), new IntWritable(1));

				}


			}	
		}
		
		if(line.contains("</text>")) {
			start = false;
		}
		
		
		
		
		
	}

}
