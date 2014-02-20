import java.io.IOException;
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

public class AbstractMapper extends MapReduceBase implements
Mapper<LongWritable, Text, Text, Text>{

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		
		String doc_id; 
		String line = value.toString();
		
		Pattern docId_ptn = Pattern.compile("<IDNo agency=\"handle\">([^<>]*)</IDNo>");
		Matcher docId_mchr = docId_ptn.matcher(line);
		
		if (docId_mchr.find()) {
			doc_id = docId_mchr.group(1);
		} else {
			return;
		}
		
		Pattern abs_ptn = Pattern.compile("<abstract>([^<>]*)</abstract>");
		Matcher abs_mchr = abs_ptn.matcher(line);
		
		while (abs_mchr.find()) {
			String raw_content = abs_mchr.group(1);
			String clean_data = raw_content.replaceAll("&lt"," ").replaceAll("&gt"," ").replaceAll("[,.;?!/()\"\']", " ");
			
			if (!clean_data.isEmpty() && clean_data != null) {
				String[] words = clean_data.toLowerCase().split(" +");
				List<String> words_list = Arrays.asList(words);
				Set<String> words_set = new HashSet<String>();
				words_set.addAll(words_list);
				
				for (String s: words_set) {
					if (!s.isEmpty()) {
						output.collect(new Text(doc_id), new Text(s.trim()));
					}
				}
			}
		}
		
		
		
		
	}
	

}
