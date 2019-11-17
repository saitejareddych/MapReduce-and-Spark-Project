import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import java.util.StringTokenizer;
import java. util. Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class adjList {

	public static class adjMapper
			extends Mapper<Object, Text, Text, Text>{

		//private final static IntWritable one = new IntWritable(1);
		private Text outVal = new Text();
		private Text outKey = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String inline = value.toString();
			if (!inline.startsWith("#"))
			{
				String [] inVals = inline.split("\t");
				outKey.set(inVals[0]);
				outVal.set(inVals[1]);
				context.write(outKey, outVal);
			}
		}
	}

	public static class adjMapper2
			extends Mapper<Object, Text, Text, Text>{

		//private final static IntWritable one = new IntWritable(1);
		private Text outVal2 = new Text();
		private Text outKey2 = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String inline = value.toString();
			if (!inline.startsWith("#"))
			{
				String [] inVals = inline.split("\t");
				outKey2.set(inVals[0]);
				outVal2.set(inVals[1]);
				context.write(outKey2, outVal2);
				context.write(outVal2,outKey2);
			}
		}
	}

	public static class adjReducer extends Reducer<Text,Text,Text,Text> {
		Text result = new Text();
		int count_max=0;
		int count_min=999999;
		Text result_min = new Text();
		Text result_max= new Text();
		Text temp_key = new Text("");
		Text temp_key_min = new Text("");
		@Override
		public void reduce(Text key, Iterable<Text> values,
						   Context context
		) throws IOException, InterruptedException {
			int cntr = 0;
			String adjlst = new String("");
			String c ="";
			for (Text val : values)
			{
				adjlst = adjlst+","+val.toString();
				cntr++;
			}
			adjlst = adjlst.substring(1);
			if(cntr>count_max){
				count_max=cntr;
				result.set(adjlst);
				result_max.set(Integer.toString(count_max));
				temp_key.set(key);
			}
			if(count_min>cntr){
				count_min= cntr;
				result_min.set(Integer.toString(count_min));
				temp_key_min.set(key);
			}
		}
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(temp_key, result_max);
			context.write(temp_key, new Text("--> This node has Max Connectivity"));
			context.write(temp_key_min, new Text("--> This node has Min Connectivity"));
		}
	}

	public static class adjReducer2 extends Reducer<Text,Text,Text,Text> {
		Text result = new Text();
		int count_max=0;
		int count_min=999999;
		Text result_min = new Text();
		Text result_max= new Text();
		Text temp_key = new Text("");
		Text temp_key_min = new Text("");
		@Override
		public void reduce(Text key, Iterable<Text> values,
						   Context context
		) throws IOException, InterruptedException {
			int cntr = 0;
			String adjlst = new String("");
			String c = "";
			for (Text val : values) {
				adjlst = adjlst + "," + val.toString();
				cntr++;
			}
			adjlst = adjlst.substring(1);

			if(cntr>count_max){
				count_max=cntr;
				result.set(adjlst);
				result_max.set(Integer.toString(count_max));
				temp_key.set(key);
			}
			if(count_min>cntr){
				count_min= cntr;
				result_min.set(Integer.toString(count_min));
				temp_key_min.set(key);
			}

		}
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(temp_key, result_max);
			context.write(temp_key, new Text("--> This node has Max Connectivity in UD graph"));
			context.write(temp_key_min, new Text("--> This node has Min Connectivity in UD graph"));
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.printf("Usage: hadoop jar <jar name> <file name without extension> <input directory> <output directory for Directed Graph> <output directory of UnDirected Graph>\n");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		Job job =Job.getInstance(conf, "Cloud Computing Adj List");
		job.setJarByClass(adjList.class);
		job.setMapperClass(adjMapper.class);
//    job.setCombinerClass(adjReducer.class);
		job.setReducerClass(adjReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf, "Cloud Computing Adj List");
		job2.setJarByClass(adjList.class);
		job2.setMapperClass(adjMapper2.class);
//    job.setCombinerClass(adjReducer.class);
		job2.setReducerClass(adjReducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
