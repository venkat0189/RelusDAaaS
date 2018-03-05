
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.gson.Gson;

public class CleanupDriver extends Configured implements Tool {

	public static final Log log = LogFactory.getLog(CleanupDriver.class);
	
	public static void main(String[] args) throws Exception {		
		Configuration conf = new Configuration();
		conf.set("out_base_dir", args[0]);
		conf.set("crt_by", args[1]);
		conf.set("obj_name", args[2]);
		conf.set("cyc_id", args[3]);
		conf.set("event_id", args[4]);
		conf.set("obj_id", args[5]);
		
		int exitCode = ToolRunner.run(conf, new CleanupDriver(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception {

		// Only start process if all arguments are provided
		if (args.length < 6) {
			System.err.println("Missing arguments! Please provide (in order): [Input File Path] [Created By] [Object Name] [Cycle ID] [Event ID] [Object ID]");
			System.exit(1);
		}

		// Display arguments
		log.info("Arguments: " + new Gson().toJson(args));

		log.info("Creating job...");
		// Create a job instance for object
		Job job = new Job(getConf());
		job.setJobName(args[2]);
		log.info("Setting jar class...");
		job.setJarByClass(CleanupDriver.class);
		
		log.info("Setting mapper class...");
		job.setMapperClass(CleanupMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(0);
		
		// Path of input file
		FileInputFormat.addInputPath(job, new Path(args[0] + "/SFL"));
		
		// Get schema file
		
		// YARN (MR2)
		//job.addCacheFile(new URI(args[0] + "/conf/" + args[2] + ".schema#schema"));
		
		// MR1
		// /user/limandy/omni_fn_extract/conf
		DistributedCache.addCacheFile(new Path(args[0]+"/conf/"+args[2]+".schema").toUri(), job.getConfiguration());
		
		// Temporary output path
		FileOutputFormat.setOutputPath(job, new Path(args[0] + "/TEMP"));		
		
		// Add outputs
		MultipleOutputs.addNamedOutput(job, "Good", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "Error", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "Orig", TextOutputFormat.class, NullWritable.class, Text.class);
		
		// Job end
	    boolean success = job.waitForCompletion(true);	    
	    return success ? 0 : 1;
	}
}
