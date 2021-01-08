package com.buaa;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import org.apache.hadoop.io.LongWritable;

import com.buaa.Temperature.TemperatureMapper.TemperatureReducer;

/** 
* @ProjectName StatisticalAverageTemperature
* @PackageName com.buaa
* @ClassName Temperature
* @Description 统计美国各个气象站30年来的平均气温
* @Author Fei joe
* @Date 2016-04-05 22:00:19
*/
@SuppressWarnings("deprecation")
public class Temperature extends Configured implements Tool {
	
	public static class TemperatureMapper extends Mapper< LongWritable, Text, Text, IntWritable> {
		/**
		 * 解析气象站数据
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 每行气象数据
			String line = value.toString(); 
			// 每小时气温值
			int temperature = Integer.parseInt(line.substring(14, 19).trim());
			// 过滤无效数据	
			if (temperature != -9999) { 		
				FileSplit fileSplit = (FileSplit) context.getInputSplit();
				// 通过文件名称提取气象站id
				String weatherStationId = fileSplit.getPath().getName().substring(5, 10);
				context.write(new Text(weatherStationId), new IntWritable(temperature));
			}
		}
		
		
		public static class TemperatureReducer extends Reducer< Text, IntWritable, Text, IntWritable> {
			
			/**
			 * 统计美国各个气象站的平均气温
			 */
			public void reduce(Text key, Iterable< IntWritable> values,Context context) throws IOException, InterruptedException {
				IntWritable result = new IntWritable();
				int sum = 0;
				int count = 0;
				
				// 统计每个气象站的气温值总和
				for (IntWritable val : values) {
					sum += val.get();
					count++;
				}
				
				// 求每个气象站的气温平均值
				result.set(sum / count);

				context.write(key, result);
			}
		}
		
		/**
		 * main 方法
		 * 
		 * @param args
		 * @throws Exception
		 */
		public static void main(String[] args) throws Exception {
			// 数据输入路径和输出路径
			String[] args0 = {
								"hdfs://hadoop01:9000/data/weather/",
								"hdfs://hadoop01:9000/data/weatherout/"
							};
			int ec = ToolRunner.run(new Configuration(), new Temperature(), args0);
			System.exit(ec);
		}
	}
	
	/**
	 * 任务驱动方法
	 * 
	 * @param arg0
	 * @throws Exception
	 */
	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		// 读取配置文件
		Configuration conf = new Configuration();

		Path mypath = new Path(arg0[1]);
		FileSystem hdfs = mypath.getFileSystem(conf);
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}
		
		// 新建一个任务
		Job job = new Job(conf, "temperature");
		// 设置主类
		job.setJarByClass(Temperature.class);
		
		// 输入路径
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		// 输出路径
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
		// Mapper
		job.setMapperClass(TemperatureMapper.class);
		// Reducer
		job.setReducerClass(TemperatureReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);	
		//提交任务
		return job.waitForCompletion(true)?0:1;
	}
}