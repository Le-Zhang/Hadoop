import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class Keyword {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		if(args.length != 2) {
			System.err.println("Usage: MaxTemperature <input path> <output path>");
		    System.exit(-1);
		}
		
		JobConf conf = new JobConf(Keyword.class);
		conf.setJobName("Count outgoing link per document");
		
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		
		conf.setMapperClass(KeywordMapper.class);
		conf.setReducerClass(KeywordReducer.class);
		conf.setNumReduceTasks(50);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		JobClient.runJob(conf);

	}

}
