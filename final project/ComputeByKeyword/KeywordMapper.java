
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class KeywordMapper extends MapReduceBase implements
Mapper<LongWritable, Text, Text, Text> {
	
	
	

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		String docId;
		String line = value.toString();
		
		Pattern docId_ptn = Pattern.compile("<IDNo agency=\"handle\">([^<>]*)</IDNo>");
		Matcher docId_mchr = docId_ptn.matcher(line);
		
		if (docId_mchr.find()) {
			docId = docId_mchr.group(1);	
		} else {
			return;
		}
		
		Pattern kwd_ptn = Pattern.compile("<keyword[^>]*>([^<>]*)</keyword>");
		Matcher kwd_mchr = kwd_ptn.matcher(line);
		
		while (kwd_mchr.find()) {
			String keyword_content = kwd_mchr.group(1).toLowerCase();
			
			
			
			if (keyword_content != null && !keyword_content.isEmpty()) {
				String[] keywords = keyword_content.split("[,;]");
				
				if (keywords != null) {
					
					List<String> kwd_list = Arrays.asList(keywords);
					Set<String> kwd_set = new HashSet<String>();
					kwd_set.addAll(kwd_list);
					
					for (String s: kwd_set) {
						if (!s.isEmpty()) {
							output.collect(new Text(docId), new Text(s.trim()));
						}
					}
					
				
			}
		}
		
	}

	}	

}
