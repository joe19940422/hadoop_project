/**
 * 
 */
/**
 * @author joe
 *
 */
package mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class tencent_clear {

	public static class mobileMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 2017-01-24
			// 01:00:00;2232,10918,1,2786,12079,5,-1464,4068,16,3441,11792,1,3057,10404,11,2930,11334,1
			// 2018-08-02
			// 06:03:16;3379,1088,2,3054,12040,1,3179,11333,7,4090,12065,1,2259,12055,3,2284,11209,1,3621,11667,3,3377,11164,5
			String line = new String(value.getBytes(), 0, value.getLength(), "GBK");
			String[] valArr = line.toString().split(";");

			if (valArr.length > 1) {

				String time = valArr[0];// 2017-01-24 01:00:00

				String content = valArr[1];// 2232,10918,1,2786,12079,5,-1464,4068,16,3441,11792,1,3057,10404,11,2930,11334,1
				String[] valcontent = content.toString().split(",");

				if (valcontent.length % 3 == 0) {
					for (int i = 0; i < valcontent.length; i = i + 3) {
						// context.write(new Text(time), new
						// Text(valcontent[i]+","+valcontent[i+1]+","+valcontent[i+2]));
						boolean isNum = valcontent[i].matches("\\d+");
						boolean isNum2 = valcontent[i + 1].matches("\\d+");
						if (isNum && isNum2) {
							double one = Double.parseDouble(valcontent[i]);
							double two = Double.parseDouble(valcontent[i + 1]);
							double s1 = one / 100;
							double s2 = two / 100;
							String sjing = "" + s1;
							String swei = "" + s2;

							context.write(NullWritable.get(),
									new Text(time + "," + sjing + "," + swei + "," + valcontent[i + 2]));
						}
					}
				}

			}
		}
	}

	
	public static class mobileReduce extends Reducer<NullWritable, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text v2 : values) {
				context.write(key, v2);
			}
		}

	}

	/**
	 * main 
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		// String day = args[0];
		// String outputpath =
		// "hdfs://172.30.10.229:9000/user/hdfs/population/tencent_/"+day;
		String outputpath = args[1];
		Configuration conf = new Configuration();
		// conf.set("hour", Hour);
		Path path = new Path(outputpath);
		FileSystem fileSystem = path.getFileSystem(conf);
		if (fileSystem.isDirectory(path)) {
			fileSystem.delete(path, true);
		}
		Job job = Job.getInstance(conf);
		job.setJarByClass(tencent_clear.class);
		conf.set("mapred.textoutputformat.separator", ",");

		// FileInputFormat.addInputPath(job, new
		// Path("hdfs://172.30.10.229:9000/user/hdfs/population/tencent/"+day));
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));

		job.setMapperClass(mobileMapper.class);
		job.setReducerClass(mobileReduce.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);

	}

}
