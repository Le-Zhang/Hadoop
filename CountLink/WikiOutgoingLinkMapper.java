
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

public class WikiOutgoingLinkMapper extends MapReduceBase implements
	Mapper <LongWritable, Text, Text, IntWritable>{
	
	String title = "";

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		
		boolean start = false;
		
		String line = value.toString();
		Pattern title_pattern = Pattern.compile("<title>.*</title>");
		Matcher title_matcher = title_pattern.matcher(line);
		
		while (title_matcher.find()) {
			title = title_matcher.group();
			title = title.substring(7, title.length() - 8)
				.replaceAll("\\&amp;", "&")
				.replaceAll("\\&quot;", "\"");
			return;
		}
		
		if(line.contains(new String("<text"))) {
			start = true;
		}

		if(start) {
			Pattern pattern = Pattern.compile("\\[\\[[^\\[\\]]+\\]\\]");
			Matcher matcher = pattern.matcher(line);
			int count = 0;
			while (matcher.find()) 
				count ++;
			
			output.collect(new Text(title), new IntWritable(count));
		}

		if(line.contains(new String("</text>"))) {
                        start = false;
                }
			
	}

}
