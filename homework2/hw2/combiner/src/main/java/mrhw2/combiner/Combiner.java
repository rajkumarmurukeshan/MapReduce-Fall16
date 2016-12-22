package mrhw2.combiner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Combiner {

	public static class CombinerMapper extends Mapper<LongWritable, Text, Text, StationWritable> {

		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split(",");

			// if the field type is TMAX, we will create a Station object with tmax value and tmaxcount
			// and 0 for tmin value and tmin count, else vice-versa and emit station ID as key and Station
			// object as value
			if (tokens[2].equalsIgnoreCase("TMAX")) {
				context.write(new Text(tokens[0]), new StationWritable(Integer.parseInt(tokens[3]), 1, 0, 0));
			} else if (tokens[2].equalsIgnoreCase("TMIN")) {
				context.write(new Text(tokens[0]), new StationWritable(0, 0, Integer.parseInt(tokens[3]), 1));
			}
		}

	}


	// Implemented a Combiner that extends Reducer's interface and implements reduce method
	public static class StationCombiner extends Reducer<Text, StationWritable, Text, StationWritable> {

		public void reduce(Text key, Iterable<StationWritable> values,
				Context context
				) throws IOException, InterruptedException {
			int tminSum = 0;
			int tminCount = 0;
			int tmaxSum = 0;
			int tmaxCount = 0;
			// perform aggregation of tmin and tmax values
			for (StationWritable station : values) {
				tminSum += station.tminValue;
				tminCount += station.tminCount;
				tmaxSum += station.tmaxValue;
				tmaxCount += station.tmaxCount;
			}
			context.write(new Text(key), new StationWritable(tmaxSum, tmaxCount, tminSum, tminCount));
		}
	}

	public static class CombinerReducer extends Reducer<Text, StationWritable, Text, Text> {

		public void reduce(Text key, Iterable<StationWritable> values,Context context) 
				throws IOException, InterruptedException {
			int tminSum = 0;
			int tminCount = 0;
			int tmaxSum = 0;
			int tmaxCount = 0;
			for (StationWritable station : values) {
				tminSum += station.tminValue;
				tminCount += station.tminCount;
				tmaxSum += station.tmaxValue;
				tmaxCount += station.tmaxCount;
			}
			float meanMaxTemp = (float)tmaxSum/tmaxCount;
			float meanMinTemp = (float)tminSum/tminCount;
			String reducerOutputValue = meanMinTemp + "," + meanMaxTemp ;
			context.write(new Text(key), new Text(reducerOutputValue));
		}
	}


	public static void main(String[] args) throws Exception {
		String inputPath = args[0];
		String outputPath = args[1];
		System.setProperty("hadoop.home.dir", "/");
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ",");
		Job job = Job.getInstance(conf, "combiner");	        
		job.setJarByClass(Combiner.class);
		job.setMapperClass(CombinerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StationWritable.class);
		job.setCombinerClass(StationCombiner.class);
		job.setReducerClass(CombinerReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
