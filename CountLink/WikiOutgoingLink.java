import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;



public class WikiOutgoingLink {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		if(args.length != 2) {
			System.err.println("Usage: MaxTemperature <input path> <output path>");
		    System.exit(-1);
		}
		
		JobConf conf = new JobConf(WikiOutgoingLink.class);
		conf.setJobName("Compute incoming link");
		
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		
		conf.setMapperClass(WikiOutgoingLinkMapper.class);
		conf.setReducerClass(WikiOutgoingLinkReducer.class);
		conf.setNumReduceTasks(50);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		JobClient.runJob(conf);

	}

}
