import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Login_Logout extends Configured implements Tool{
	
	public int run(String[] args) throws Exception
	{
		JobConf conf = new JobConf(getConf(), Login_Logout.class);
		conf.setJobName("Login_Logout");
		
		//Setting configuration object with the Data Type of output Key and Value
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        
		conf.setMapperClass(Login_LogoutMapper.class);
		conf.setReducerClass(Login_LogoutReducer.class);
		
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		
		FileInputFormat.addInputPath(conf, in);
		FileOutputFormat.setOutputPath(conf, out);
		
		JobClient.runJob(conf);
		return 0;
	}
	
	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new Login_Logout(), args);
		System.exit(res);
	}
}

