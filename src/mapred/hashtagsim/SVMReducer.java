package mapred.hashtagsim;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

public class SVMReducer extends Reducer<LongWritable, Text, Text, Text>{
	
	long supportVectorNum = 0;
	@Override
	protected void reduce(LongWritable arg0, Iterable<Text> arg1,
			Context context)
			throws IOException, InterruptedException {
		for(Text line : arg1){
			String[] values = line.toString().split(":");
			String key = values[0];
			StringBuilder sb = new StringBuilder();
			for(int i = 1; i < values.length; ++i){
				sb.append(values[i]);
				sb.append(" ");
			}
			context.write(new Text(String.valueOf(Double.valueOf(key).intValue())), new Text(sb.toString()));
			++supportVectorNum;
		}
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			long prev = conf.getLong("support_vector_num", 0);
			conf.setLong("support_vector_num", prev + supportVectorNum);
			System.out.println("*********************" + conf.getLong("support_vector_num", 0));
	}
}
