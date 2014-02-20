
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class AuthMapper extends MapReduceBase implements
Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		
		String doc_id; 
		String line = value.toString();
		
		//matching the content between <IDNo agency="handle"></IDNo>, to get doc id
		Pattern docId_ptn = Pattern.compile("<IDNo agency=\"handle\">([^<>]*)</IDNo>");
		Matcher docId_mchr = docId_ptn.matcher(line);
		
		if (docId_mchr.find()) {
			doc_id = docId_mchr.group(1);
		} else {
			return;
		}
		
		Pattern auth_ptn = Pattern.compile("<AuthEnty[^>]*>([^<>]*)</AuthEnty>");
		Matcher auth_mchr = auth_ptn.matcher(line);
		
		while (auth_mchr.find()) {
			String author = auth_mchr.group(1).toLowerCase();
			
			if (author != null && !author.isEmpty() && author != "N/A") {
				output.collect(new Text(doc_id), new Text(author));
			}
			
			
			
		}
		
	}

}
