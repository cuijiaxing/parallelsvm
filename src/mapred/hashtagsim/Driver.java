package mapred.hashtagsim;

import java.io.IOException;

import mapred.job.Optimizedjob;
import mapred.util.FileUtil;
import mapred.util.SimpleParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import java.io.IOException;

public class Driver {
	
	public static int iterationNum = 0;
	public static void main(String args[]) throws Exception {
		
		SimpleParser parser = new SimpleParser(args);

		String input = parser.get("input");
		String output = parser.get("output");
		long prevNum = 0;
		long supportVectorNum = 0;
		String tempOutput = output;// + "/" + iterationNum;
		String tempInput = input;
		do{
			prevNum = supportVectorNum;
			System.out.println("out:" + tempOutput);
			System.out.println("in:" + tempInput);
			supportVectorNum = getHashtagFeatureVector(tempInput, tempOutput);
			tempInput = tempOutput;
			tempOutput = output + "/" + iterationNum;
			++iterationNum;
			System.out.println("Fuck-------------" + (supportVectorNum - prevNum) + "--------");
		}while(iterationNum < 5);

	}


	/**
	 * Same as getJobFeatureVector, but this one actually computes feature
	 * vector for all hashtags.
	 * 
	 * @param input
	 * @param output
	 * @throws Exception
	 */
	private static long getHashtagFeatureVector(String input, String output)
			throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.task.timeout", "1800000");
		conf.setLong("support_vector_num", 0);
		Optimizedjob job = new Optimizedjob(conf, input, output,
				"Get feature vector for all hashtags");
		job.setClasses(SVMMapper.class, SVMReducer.class, null);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		try{
				//if(!job.waitForCompletion(true)){
				//	throw new Exception("Job being interupted" + iterationNum);
				//}
			job.run();
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("job being interrupted :" + iterationNum);
		}
		conf = job.getConfiguration();
		long supportVectorNum = conf.getLong("support_vector_num", 0);
		return supportVectorNum;
	}
}
