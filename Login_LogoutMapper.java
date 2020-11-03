import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class Login_LogoutMapper extends MapReduceBase implements  Mapper<LongWritable, Text, Text, IntWritable> 
{
	private Text ip = new Text();
	private  IntWritable timestamp = new IntWritable();
	
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
	{
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		ip.set(tokenizer.nextToken());
		timestamp.set(Integer.parseInt(tokenizer.nextToken()));
		output.collect(ip, timestamp);
	}
}
