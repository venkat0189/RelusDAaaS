
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CleanupMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	public static final Log log = LogFactory.getLog(CleanupMapper.class);
	
	private SchemaService schemaService;
	private ValidationService vService;

	public void setup(Context context) throws IOException, InterruptedException {
		schemaService = new SchemaService();
		
		try {
//			if (context.getCacheFiles() != null
//		            && context.getCacheFiles().length > 0) {				
//		        File schemaFile = new File("./schema");
//		        BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream(schemaFile)));				
//				List<DataSchema> schemaList = schemaService.readSchemaFile(rdr);
//				vService = new ValidationService(context, schemaList, schemaService.delimiter);
//				rdr.close();
//			}
//			else {
//				throw new RuntimeException("Schema information is not set in job cache.");
//			}
			
			Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			if (files == null || files.length == 0) {
				throw new RuntimeException(
						"Schema information is not set in DistributedCache");
			}
			
			// Read all files in the DistributedCache
			for (Path p : files) {
				BufferedReader rdr = new BufferedReader(
						new InputStreamReader(new FileInputStream(
										new File(p.toString()))));
				List<DataSchema> schemaList = schemaService.readSchemaFile(rdr);
				vService = new ValidationService(context, schemaList, schemaService.delimiter);
				rdr.close();
			}
			
//			BufferedReader rdr = new BufferedReader(
//					new InputStreamReader(new FileInputStream(
//									new File(context.getConfiguration().get("out_base_dir") + "/conf/omni_su_extract.schema"))));
//			List<DataSchema> schemaList = schemaService.readSchemaFile(rdr);
//			vService = new ValidationService(context, schemaList, schemaService.delimiter);
//			log.info("delimiter: " + schemaService.delimiter);
		} catch (IOException e) {
			System.out.println("Unknown Exception occured during reading cache file");
			e.printStackTrace();
			throw e;
		}
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		boolean delimited = !schemaService.delimiter.equals("");
		String linefeed[] = value.toString().split(new String("\\|~\\|"));
		String rowNumber = linefeed[1];
		
		if (delimited) {
			System.out.println("delimited value: " + linefeed[0]);
			try {
				List<String> record = vService.getDelimitedRecord(linefeed[0]);	
				vService.validateDelimitedRecord(context, record, rowNumber);
			} catch (ArrayIndexOutOfBoundsException e) {
				//vService.outputLog.close();
			}
			
						
		}
		else {
			System.out.println("value: " + value.toString());
			String record = linefeed[0];
			vService.validateRecord(context, record, rowNumber);
		}		
	}

	public void cleanup(Context ctx) throws IOException {
		try {
			vService.outputLog.close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
