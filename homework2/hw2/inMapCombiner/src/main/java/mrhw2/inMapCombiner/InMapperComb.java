package mrhw2.inMapCombiner;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class InMapperComb {

	public static class InMapper extends Mapper<LongWritable, Text, Text, StationWritable> {

		//create a mapper class variable for in mapper combiners, that will aggregate the values emmited
		// by all mapper calls in a single mapper task
		HashMap<String, StationWritable> inMapStations = new HashMap<String, StationWritable>();

		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split(",");

			// perform aggregation for TMAX with same StationId
			if (tokens[2].equalsIgnoreCase("TMAX")) {
				if (inMapStations.containsKey(tokens[0])) {
					StationWritable st = inMapStations.get(tokens[0]);
					st.setTmaxValue(st.getTmaxValue() + Integer.parseInt(tokens[3]));
					st.setTmaxCount(st.getTmaxCount() + 1);
				} else {
					inMapStations.put(tokens[0], new StationWritable(Integer.parseInt(tokens[3]), 1, 0, 0));
				}
			}

			// perform aggregation for TMIN with same StationId
			if (tokens[2].equalsIgnoreCase("TMIN")) {
				if (inMapStations.containsKey(tokens[0])) {
					StationWritable st = inMapStations.get(tokens[0]);
					st.setTminValue(st.getTminValue() + Integer.parseInt(tokens[3]));
					st.setTminCount(st.getTminCount() + 1);
				} else {
					inMapStations.put(tokens[0], new StationWritable(0, 0, Integer.parseInt(tokens[3]), 1));
				}
			}
		}

		// Clean Up method is used to emit the aggregated key value pair from the mapper output
		public void cleanup(Context context) throws IOException, InterruptedException {
			for(String stationId: inMapStations.keySet()){
				context.write(new Text(stationId), inMapStations.get(stationId));
			}
		}	

	}

	public static class InMapperReducer extends Reducer<Text, StationWritable, Text, Text> {

		public void reduce(Text key, Iterable<StationWritable> values, Context context) 
				throws IOException, InterruptedException {
			int tminSum = 0;
			int tminCount = 0;
			int tmaxSum = 0;
			int tmaxCount = 0;
			
			// perform aggregation of all values and calculate mean minimum and mean maximum temperature
			// for each stations with unique stationID
			for (StationWritable station : values) {
				tminSum += station.tminValue;
				tminCount += station.tminCount;
				tmaxSum += station.tmaxValue;
				tmaxCount += station.tmaxCount;
			}
			float meanMinTemp = (float)tminSum/tminCount;
			float meanMaxTemp = (float)tmaxSum/tmaxCount;
			String outputLine = meanMinTemp + "," + meanMaxTemp ;
			context.write(new Text(key), new Text(outputLine));
		}

	}


	public static void main(String[] args) throws Exception {
		String inputPath = args[0];
		String outputPath = args[1];
		System.setProperty("hadoop.home.dir", "/");
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ",");
		Job job = Job.getInstance(conf, "in mapper combining");
		job.setJarByClass(InMapperComb.class);
		job.setMapperClass(InMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StationWritable.class);
		job.setReducerClass(InMapperReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
