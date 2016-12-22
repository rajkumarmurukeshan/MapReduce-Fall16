package mr.hw3_PageRank;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PageRank {

	public static final double dampingFactor= 0.85;

	public enum Counter{
		NodeCounter,
		Dangling_PR_Sum
	}

	public static class PreProcessingMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			String line = value.toString();
			String preProcessedResult = Bz2WikiParser.performPreProcessing(line);
			if(preProcessedResult != null){
				int delimLoc = preProcessedResult.indexOf(':');
				String pageName = preProcessedResult.substring(0, delimLoc);
				String linkedPages = preProcessedResult.substring(delimLoc + 1);
				context.write(new Text(pageName), new Text(linkedPages));
				context.getCounter(Counter.NodeCounter).increment(1L);
			}
		}
	}

	public static class DefaultPageRankMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			String line = value.toString();
			int delimLoc = line.indexOf(':');
			String pageName = line.substring(0, delimLoc);
			String linkedPages = line.substring(delimLoc + 1);
			long nodeCount = context.getConfiguration().getLong("nodeCount", 0);
			double defaultPageRank = 1/ (double)nodeCount;
			context.write(new Text(pageName), new Text(Double.toString(defaultPageRank) + ":" + linkedPages));	
			if(linkedPages.length() == 0){
				long danglingPR = (long)(defaultPageRank * Math.pow(10, 8));
				context.getCounter(Counter.Dangling_PR_Sum).increment(danglingPR);
			}
		}
	}


	public static class PageRankMapper extends Mapper<LongWritable, Text, Text, Node> {

		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			String line= value.toString();
			String[] val= line.split(":");
			String pageName= val[0];
			double prevPageRank= Double.parseDouble(val[1]);
			String[] adjacencyList;
			if(val.length == 3){
				adjacencyList= val[2].split("~");
			} else adjacencyList= new String[0];
			context.write(new Text(pageName), new Node(adjacencyList, prevPageRank));
			double pageRankDistribution = prevPageRank/(double)adjacencyList.length;
			for(String adjacentNode: adjacencyList){
				context.write(new Text(adjacentNode), new Node(pageRankDistribution));
			}
		}
	}

	public static class PageRankReducer extends Reducer<Text, Node, Text, Text> {

		public void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {
			double pageRankSum= 0.00;
			long nodeCount = context.getConfiguration().getLong("nodeCount", 0);
			long danglingPRSumTemp = context.getConfiguration().getLong("danglingPRSum", 0);
			double danglingPRSum = (double)danglingPRSumTemp/(Math.pow(10, 8));
			Node node = null;
			for(Node value : values){
				if (value.isContribution()){
					pageRankSum += value.getPageRank();
				} else {
					node = new Node(value.getAdjacencyList(), value.getPageRank());
				}
			}
			if(node != null){
				double constant1 = (1 - dampingFactor)/(double) nodeCount;
				double constant2 = dampingFactor * (danglingPRSum/(double) nodeCount);
				double constant3 = dampingFactor * (pageRankSum);
				double pageRank= constant1 + constant2 + constant3;
				StringBuilder outputValue = new StringBuilder(pageRank+":");
				boolean isFirst= true;
				for(String link: node.getAdjacencyList()){
					if(isFirst){
						outputValue.append(link);
						isFirst= false;
					} else {
						outputValue.append("~" + link);
					}
				}
				context.write(key, new Text(outputValue.toString()));
				if(node.getAdjacencyList().length == 0){
					long danglingPR = (long)(pageRank * Math.pow(10, 8));
					context.getCounter(Counter.Dangling_PR_Sum).increment(danglingPR);
				}		
			} else {
				long danglingPR = (long)(pageRankSum * Math.pow(10, 8));
				context.getCounter(Counter.Dangling_PR_Sum).increment(danglingPR);
			}
		}

	}

	public static class PageComparator implements Comparator<Page>{
		public int compare(Page o1, Page o2) {
			if (o2.getPageRank() < o1.getPageRank()) {
				return -1;
			} else if (o2.getPageRank() > o1.getPageRank()) {
				return 1;
			} else {
				return 0;
			}
		}	
	}

	public static class TopKMapper extends Mapper<LongWritable, Text, Page, DoubleWritable>{

		PriorityQueue<Page> mapperOutput = new PriorityQueue<Page>(100, new PageComparator());

		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			String line= value.toString();
			String[] val= line.split(":");
			String pageName= val[0];
			double pageRank= Double.parseDouble(val[1]);
			mapperOutput.add(new Page(pageName, pageRank));
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			int count = 1;
			while (!mapperOutput.isEmpty() && count <= 100) {
				Page page = mapperOutput.poll();
				context.write(page, new DoubleWritable(page.getPageRank()));
			}
		}

	}

	public static class PagePartitioner extends Partitioner<Page, DoubleWritable> {
		public int getPartition(Page rankNode, DoubleWritable writable, int numberOfPartitions) {
			return Math.abs(rankNode.getPageName().hashCode() %  numberOfPartitions);
		}
	}

	public static class PageRankComparator extends WritableComparator {

		public PageRankComparator() {
			super(Page.class, true);
		}

		@Override
		public int compare(WritableComparable wc1, WritableComparable wc2) {
			Page page1 = (Page) wc1;
			Page page2 = (Page) wc2;
			return page2.getPageRank().compareTo(page1.getPageRank());
		}
	}

	public static class TopKReducer extends Reducer<Page, DoubleWritable, Text, DoubleWritable> {

		int count = 0;
		
		public void reduce(Page key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			for (DoubleWritable pageRank : values) {
				if(count<100){
					context.write(new Text(key.getPageName()), pageRank);
					count++;
				} else {
					break;
				}
			}
		}

	}


	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "/");
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ":");
		
		Path pathIn = new Path(args[0]);
		Path pathOut = new Path(args[1]);

		// Pre-Processing job 
		Job preProcessing = Job.getInstance(conf, "pre-processing");
		preProcessing.setJarByClass(PageRank.class);
		preProcessing.setMapperClass(PreProcessingMapper.class);
		preProcessing.setMapOutputKeyClass(Text.class);
		preProcessing.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(preProcessing, pathIn);
		Path outputPath = new Path(pathOut + "preProcessed");
		FileOutputFormat.setOutputPath(preProcessing, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		preProcessing.waitForCompletion(true);

		// get the total number of nodes from the counter
		long nodeCount = preProcessing.getCounters().findCounter(Counter.NodeCounter).getValue();
		conf.setLong("nodeCount", nodeCount);

		// Job to get the default PageRank
		Job defaultPR = Job.getInstance(conf, "defaultPageRank");
		defaultPR.setJarByClass(PageRank.class);
		defaultPR.setMapperClass(DefaultPageRankMapper.class);
		defaultPR.setMapOutputKeyClass(Text.class);
		defaultPR.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(defaultPR, outputPath);
		Path pageRankInPath = new Path(pathOut + "pageRankInputFile");
		FileOutputFormat.setOutputPath(defaultPR, pageRankInPath);
		pageRankInPath.getFileSystem(conf).delete(pageRankInPath, true);
		defaultPR.waitForCompletion(true);

		long danglingPRSum = defaultPR.getCounters().findCounter(Counter.Dangling_PR_Sum).getValue();
		conf.setLong("danglingPRSum", danglingPRSum);
		defaultPR.getCounters().findCounter(Counter.Dangling_PR_Sum).setValue(0);


		Path inPath= pageRankInPath;
		Path outPath;
		Job pageRankCalculation;

		// 10 iterations for PageRank Calculation
		for(int i=0; i<10; i++){
			pageRankCalculation = Job.getInstance(conf, "pageRank" + i);
			pageRankCalculation.setJarByClass(PageRank.class);
			pageRankCalculation.setMapperClass(PageRankMapper.class);
			pageRankCalculation.setMapOutputKeyClass(Text.class);
			pageRankCalculation.setMapOutputValueClass(Node.class);
			pageRankCalculation.setReducerClass(PageRankReducer.class);
			pageRankCalculation.setOutputKeyClass(Text.class);
			pageRankCalculation.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(pageRankCalculation, inPath);
			outPath= new Path(pathOut+"pageRankOutputFile"+(i+1));
			inPath= outPath;
			FileOutputFormat.setOutputPath(pageRankCalculation, outPath);
			outPath.getFileSystem(conf).delete(outPath, true);
			pageRankCalculation.waitForCompletion(true);
			danglingPRSum = preProcessing.getCounters().findCounter(Counter.Dangling_PR_Sum).getValue();
			conf.setLong("danglingPRSum", danglingPRSum);
			pageRankCalculation.getCounters().findCounter(Counter.Dangling_PR_Sum).setValue(0);
		}
		
		Path finalOutput = new Path(pathOut + "top-kOutput");

		// Top-k Job
		Job topK = Job.getInstance(conf, "topK");
		topK.setJarByClass(PageRank.class);
		topK.setMapperClass(TopKMapper.class);
		topK.setMapOutputKeyClass(Page.class);
		topK.setMapOutputValueClass(DoubleWritable.class);
		topK.setNumReduceTasks(1);
		topK.setReducerClass(TopKReducer.class);
		topK.setPartitionerClass(PagePartitioner.class);
		topK.setSortComparatorClass(PageRankComparator.class);
		topK.setOutputKeyClass(Text.class);
		topK.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(topK, inPath);
		FileOutputFormat.setOutputPath(topK, finalOutput);
		finalOutput.getFileSystem(conf).delete(finalOutput, true);
		System.exit(topK.waitForCompletion(true) ? 0 : 1);

	}

}
