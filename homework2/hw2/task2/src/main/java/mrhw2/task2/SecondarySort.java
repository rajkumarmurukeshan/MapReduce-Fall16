package mrhw2.task2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondarySort {

	public static class SecondarySortMapper 
	extends Mapper<LongWritable, Text, CompositeKey, StationWritable> {

		HashMap<CompositeKey, StationWritable> inMapStations = new HashMap<CompositeKey, StationWritable>();

		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split(",");

			if (tokens[2].equalsIgnoreCase("TMAX")) {
				String year= tokens[1].substring(0, 4);
				CompositeKey cKey= new CompositeKey(tokens[0], year);
				if (inMapStations.containsKey(cKey)) {
					StationWritable st = inMapStations.get(cKey);
					st.setTmaxValue(st.getTmaxValue() + Integer.parseInt(tokens[3]));
					st.setTmaxCount(st.getTmaxCount() + 1);
				} else {
					inMapStations.put(cKey, new StationWritable(year, Integer.parseInt(tokens[3]), 1, 0, 0));
				}
			}

			if (tokens[2].equalsIgnoreCase("TMIN")) {
				String year= tokens[1].substring(0, 4);
				CompositeKey cKey= new CompositeKey(tokens[0], year);
				if (inMapStations.containsKey(cKey)) {
					StationWritable st = inMapStations.get(cKey);
					st.setTminValue(st.getTminValue() + Integer.parseInt(tokens[3]));
					st.setTminCount(st.getTminCount() + 1);
				} else {
					inMapStations.put(cKey, new StationWritable(year, 0, 0, Integer.parseInt(tokens[3]), 1));
				}
			}

		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			for(CompositeKey ckey: inMapStations.keySet()){
				context.write(ckey, inMapStations.get(ckey));
			}
		}	

	}

	public static class SecondarySortPartitioner
	extends Partitioner<CompositeKey, StationWritable> {

		@Override
		public int getPartition(CompositeKey cKey, StationWritable station, int numberOfPartitions) {
			return Math.abs(cKey.getStationId().hashCode() % numberOfPartitions);
		}
		
	}
	
	
	public static class SecondarySortReducer extends Reducer<CompositeKey, StationWritable, Text, Text> {
		
		
		
		public void reduce(CompositeKey cKey, Iterable<StationWritable> values, Context context) 
				throws IOException, InterruptedException {
			int tminSum= 0;
			int tminCount= 0;
			int tmaxSum= 0;
			int tmaxCount= 0;
			int prevYear;
			int currYear = -1;
			ArrayList<String> resultList = new ArrayList<String>();
			for(StationWritable station: values){
				prevYear = currYear;
				currYear = Integer.parseInt(station.getYear());
				if(currYear != prevYear){
					if (prevYear != -1) {
                        StringBuilder str = new StringBuilder();
                        String tmaxMean= null;
                        String tminMean= null;
                        if (tmaxCount != 0) {
                        	tmaxMean = String.valueOf(tmaxSum/(float)tmaxCount);
                        }
                        if (tminCount != 0) {
                        	tminMean = String.valueOf(tminSum/(float)tminCount);
                        }
                        str.append("(");
                        str.append(prevYear).append(",").append(tminMean).append(",").append(tmaxMean).append(")");
                        str.toString();
                        resultList.add(str.toString());
                    }
					tminSum= station.getTminValue();
					tminCount= station.getTminCount();
					tmaxSum= station.getTmaxValue();
					tmaxCount= station.getTmaxCount();									
				} else if(currYear == prevYear){
					tminSum += station.getTminValue();
					tminCount += station.getTminCount();
					tmaxSum += station.getTmaxValue();
					tmaxCount += station.getTmaxCount();
				}			
			}
			StringBuilder str1 = new StringBuilder();
            String tmaxMean= null;
            String tminMean= null;
            if (tmaxCount != 0) {
            	tmaxMean = String.valueOf((float)tmaxSum/tmaxCount);
            }
            if (tminCount != 0) {
            	tminMean = String.valueOf((float)tminSum/tminCount);
            }
            str1.append("(");
            str1.append(currYear).append(",").append(tminMean).append(",").append(tmaxMean).append(")");
            resultList.add(str1.toString());
            String outputValue = Arrays.toString(resultList.toArray());
            context.write(new Text(cKey.getStationId()), new Text(outputValue));	
		}

	}



	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.setProperty("hadoop.home.dir", "/");
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "secondary sort");
        job.setJarByClass(SecondarySort.class);
        job.setMapperClass(SecondarySortMapper.class);
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(StationWritable.class);
        job.setReducerClass(SecondarySortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(10);
        job.setPartitionerClass(SecondarySortPartitioner.class);
        job.setGroupingComparatorClass(GroupingComparator.class);
        job.setSortComparatorClass(CompositeKeyComparator.class);
        FileInputFormat.addInputPaths(job, args[0]);
        Path outPutPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outPutPath);
        outPutPath.getFileSystem(conf).delete(outPutPath, true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
