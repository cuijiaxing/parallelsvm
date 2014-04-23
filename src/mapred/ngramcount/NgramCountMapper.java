package mapred.ngramcount;

import java.io.IOException;

import mapred.util.Tokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.math.*;

public class NgramCountMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		int n = Integer.parseInt(context.getConfiguration().get("n"));
		String[] words = Tokenizer.tokenize(line);
		int lenSentence = words.length;
		int numWords = Math.max(lenSentence - n,-1);
		for (int i = 0; i <= numWords; i++) {
		    String conWords = "";
		    for (int j=i;j < i+n;j++) {
			conWords = conWords + " " + words[j];
		    }
		    context.write(new Text(conWords), NullWritable.get());
		}

	}
}
