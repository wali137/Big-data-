import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class VehicleCount {

	public class CombinationKey implements WritableComparable<CombinationKey> {
		private Text location;
		private Text hour;
		private Text dateTime;
		private Text offset;
		private Text value;

		public Text getLocation() {
			return this.location;
		}

		public void setLocation(Text location) {
			this.location = location;
		}

		public Text getHour() {
			return this.hour;
		}

		public void setHour(Text hour) {
			this.hour = hour;
		}

		public Text getDateTime() {
			return this.dateTime;
		}

		public void setDateTime(Text dateTime) {
			this.dateTime = dateTime;
		}

		public Text getOffset() {
			return this.offset;
		}

		public void setOffset(Text offseet) {
			this.offset = offseet;
		}

		public Text getValue() {
			return this.value;
		}

		public void setValue(Text value) {
			this.value = value
		}

		@Override
		public void readFields(DataInput dateInput) throws IOException {
			this.location.readFields(dateInput);
			this.hour.readFields(dateInput);
			this.time.readFields(dateInput);
		}

		@Override
		public void write(DataOutput outPut) throws IOException {
			this.location.write(outPut);
			this.hour.write(outPut);
			this.time.write(outPut);
		}

		@Override
		public int compareTo(CombinationKey combinationKey) {
			return this.time.compareTo(combinationKey.getTime());
		}
	}

	public class DefinedPartition extends Partitioner<CombinationKey, IntWritable> {
		@Override
		public int getPartition(CombinationKey key, IntWritable value, int numPartitions) {
			String baseKey = key.getLocation().toString() + ";" + key.getHour().toString();
			int part = baseKey.hashCode() % numPartitions;
			return part;
		}
	}

	public static class KeyComprator extends WritableComparator {
		public KeyComprator() {
			super(CombinationKey.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			CombinationKey k1 = (CombinationKey) w1;
			CombinationKey k2 = (CombinationKey) w2;

			int comp = k1.getDateTime().compareTo(k2.getDateTime());

			if (comp == 0) {
				comp = k1.getOffset().compareTo(k2.getOffset());
			}

			return comp;
		}
	}

	public static class GroupComprator extends WritableComparator {
		protected GroupComprator() {
			super(CombinationKey.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			CombinationKey k1 = (CombinationKey) w1;
			CombinationKey k2 = (CombinationKey) w2;

			String baseKey1 = k1.getLocation().toString() + ";" + k1.getHour().toString();
			String baseKey2 = k2.getLocation().toString() + ";" + k2.getHour().toString();

			int comp = baseKey1.compareTo(baseKey2);
			return comp;
		}
	}

	public static class Map extends Mapper<LongWritable, Text, CombinationKey, Text> {
		CombinationKey combinationKey = new CombinationKey();
		Text location;
		Text dateTime;
		Text hour;
		Text sensorData;
		Text offset;

		Text mapOutputKey = new Text;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] datas = value.toString().split(",", 7);
			if ("location_name".equals(datas[0])) {
				return;
			}

			location.set(datas[0]);
			dateTime.set(datas[2]);

			offset.set(datas[3];
			
			String h = getHour(dateTime.toString());
			hour.set(h);

			sensorData.Set(datas[7]);
			
			combinationKey.setDateTime(dateTime);
			combinationKey.setLocation(location);
			combinationKey.setHour(hour);
			combinationKey.setValue(sensorData);
			combinationKey.setOffset(offseet);
			context.write(combinationKey, sensorData);
		}

	}

	public static class Reduce extends Reducer<CombinationKey, Text, Text, Text> {

		public void reduce(CombinationKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			int[] results = null;

			ArrayList<Integer> posList = new ArrayList<Integer>();
			for (Text val : values) {
				String str = val.toString();
				str = str.split("\"")[1];
				str = str.substring(0, str.length() - 1);

				String[] sensorValues = str.split(",");
				
				if(results == null){
					results = new int[sensorValues.length];
					Arrays.fill(results,0);
				}
				
				for(Integer integer:posList){
					int num = Integer.parseInt(sensorValues[integer]);
					if(num > 30){
						results[integer]++;
					}
				}
				posList.clear();
				for (int i = 0; i < sensorValues.length; i++) {
					int num = Integer.parseInt(sensorValues[i]);
					if (num < 30) {
						posList.add(i);
					}
				}
			}
			Text outputKey = new Text();
			outputKey.set(key.getLocation()+";"+key.getHour());
			
			Text outputValue = new Text();
			outputValue.set(results.toString());
			context.write(outputKey, outputValue);
		}

	}

	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration(), "vehiclecount");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setPartitionerClass(DefinedPartition.class);
		job.setGroupingComparatorClass(GroupComprator.class);
		job.setSortComparatorClass(KeyComprator.class);

		job.setMapOutputKeyClass(CombinationKey.class);
		job.setMapOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(WordCount.class);
		job.waitForCompletion(true);
	}


	private static String getHour(String time) {
		String pattern = "yyyy-MM-dd HH:mm:ss.S";
		DateFormat dateFormat = new SimpleDateFormat(pattern, Locale.ENGLISH);
		Date date;
		try {
			date = dateFormat.parse(time);
		} catch (ParseException e) {
			e.printStackTrace();
			return "";
		}

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);

		return "" + calendar.get(Calendar.HOUR_OF_DAY);
	}

}
