package hbase_mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
 
/***
 * ʹ��MapReduce��HBase�е�������
 * @author joe
 * 
 */
public class HadoopConnectTest3
{
	public static class HBaseHFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>
	{
		@Override
		protected void map(LongWritable key, Text value, Context context)
		{
			String value_str = value.toString();
			//��Text������תΪ�ַ�����
			//1080x1920|Vivo X9|a8622b8a26ae679f0be82135f6529902|�й�|����|����|wifi|2017-12-31 11:55:58
			String values[] = value_str.split(",");
		//	-1000000170699773791,2017-09-01 07:33:42,2017-09-01 20:03:07,15015403456115280,1,1,110106,N,011,V0110000,20170901	
			String uid =values[0];
			String stime=values[1];
			String etime=values[2];
			String date=values[10];
			String time = stime.split(" ")[1];//07:33:42
			String time2 = etime.split(" ")[1];//20:03:07
		//	time.replace(time, ":");
			String rowkey = uid+"-"+date+time.replace(":", "")+'-'+time2.replace(":", "");;
			
			Put p1 = new Put(Bytes.toBytes(rowkey));
			//ʹ���м��½�Put����
		//	p1.addColumn(Bytes.toBytes("stay_month"), Bytes.toBytes(hms), Bytes.toBytes(value_str));
			
			   
            p1.addColumn("stay_month".getBytes(), "stime".getBytes(), values[1].getBytes());
            p1.addColumn("stay_month".getBytes(), "etime".getBytes(), values[2].getBytes());
            p1.addColumn("stay_month".getBytes(), "grid_id".getBytes(), values[3].getBytes());
            p1.addColumn("stay_month".getBytes(), "poi_id".getBytes(), values[4].getBytes());
            p1.addColumn("stay_month".getBytes(), "ptype".getBytes(), values[5].getBytes());
            p1.addColumn("stay_month".getBytes(), "zone_id".getBytes(), values[6].getBytes());
            p1.addColumn("stay_month".getBytes(), "is_core".getBytes(), values[7].getBytes());
            p1.addColumn("stay_month".getBytes(), "province".getBytes(), values[8].getBytes());          
            p1.addColumn("stay_month".getBytes(), "date".getBytes(), values[10].getBytes());
			
			
			//��put������һ�У�����Ϊd������Ϊʱ���룬ֵΪԭ�ַ���
			if (!p1.isEmpty())
			//���Put����Ϊ�գ����У�
			{
				ImmutableBytesWritable ib = new ImmutableBytesWritable();
				//�½�һ��ImmutableBytesWritable����
				ib.set(Bytes.toBytes("unicom"));
				//��ImmutableBytesWritable�����ֵ��Ϊ����
				try
				{
					context.write(ib, p1);
					//���Խ��˼�ֵ����Ϊmapper�����
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
				catch (InterruptedException e)
				{
					e.printStackTrace();
				}
			}
		}
	}
 
	private static final String HDFS = "hdfs://172.30.10.229:9000";// HDFS·��
	private static final String INPATH = HDFS + "/user/hdfs/population/phone/stay_month/20170901";// �����ļ�·��
 
	public int run() throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = HBaseConfiguration.create();
		//������������ã�configuration��һ����������ö��󣬷�װ�������������Ϣ
		conf.set("hbase.zookeeper.quorum", "172.30.10.241:2181,172.30.10.242:2181,172.30.10.243:2181");
		//����zookeeper
		conf.set("hbase.rootdir", "hdfs://172.30.10.229:9000/hbase-2.1.1");
		//����hbase��Ŀ¼
		conf.set("zookeeper.znode.parent", "/hbase-2.1.1");
 
		Job job = Job.getInstance(conf, "HFile bulk load test");
		// ����һ���µ��������
		job.setJarByClass(HadoopConnectTest3.class);
		//����driver��
		job.setMapperClass(HBaseHFileMapper.class); 
		// ���������map���,map����������ImmutableBytesWritable��put����
		TableMapReduceUtil.initTableReducerJob("unicom", null, job);
		// TableMapReduceUtil��HBase�ṩ�Ĺ����࣬���Զ�����mapreuce�ύ��hbase����ĸ������ã���װ�˲�����ֻ��Ҫ�򵥵����ü���
		//���ñ���Ϊclientdata_test5��reducer��Ϊ�գ�jobΪ��ǰ���úŵ�job
		job.setNumReduceTasks(0);
		// ����reduce���̣�������map�˵�����ֱ���ύ����Ҫʹ��reduce�࣬������ó�null,������reduce�ĸ���Ϊ0
		FileInputFormat.addInputPath(job, new Path(INPATH));
		// ���������ļ�·��
		return (job.waitForCompletion(true) ? 0 : -1);
	}
 
	public static void main(String[] args)
	{
		try
		{
			new HadoopConnectTest3().run();
		}
		catch (ClassNotFoundException | IOException | InterruptedException e)
		{
			e.printStackTrace();
		}
	}
 
}
