
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ValidationService {

	public static Logger log = LoggerFactory.getLogger(ValidationService.class);

	public static final int DATA_FMT_ERR_CD = 21;
	public static final int DATA_LTH_ERR_CD = 41;
	public static final int DATA_NULL_ERR_CD = 22;

	private Map<String, String> config;
	public String delimiter;
	public List<DataSchema> schemaList;
	public OutputLog outputLog;
	public Context context;

	public ValidationService(Context context, List<DataSchema> schemaList,
			String delimiter) {
		this.delimiter = delimiter;
		this.schemaList = schemaList;
		config = new HashMap<String, String>();

		if (context != null) {
			outputLog = new OutputLog(new MultipleOutputs<NullWritable, Text>(
					context));
			config.put("crt_by", context.getConfiguration().get("crt_by"));
			config.put("cyc_id", context.getConfiguration().get("cyc_id"));
			config.put("event_id", context.getConfiguration().get("event_id"));
			config.put("obj_name", context.getConfiguration().get("obj_name"));
			config.put("obj_id", context.getConfiguration().get("obj_id"));
			config.put("out_base_dir",
					context.getConfiguration().get("out_base_dir"));
			config.put("out_base_dir_rsl", config.get("out_base_dir") + "/RSL/");
			config.put("out_base_dir_err", config.get("out_base_dir") + "/ERR/");
			config.put("out_base_dir_ssl", config.get("out_base_dir") + "/SSL/");
			config.put("orig_file_name",
					config.get("obj_name") + "_" + config.get("cyc_id") + "_"
							+ config.get("event_id"));
		} else
			outputLog = new OutputLog(null);
	}

	public List<String> getDelimitedRecord(String line)
			throws UnsupportedEncodingException {
		List<String> record = Arrays.asList(line.split(Pattern.quote(delimiter)));
		return record;
	}

	public void validateDelimitedRecord(Context context, List<String> record,
			String rowNumber) throws IOException, InterruptedException {
		outputLog.clear();

		for (int i = 0; i < schemaList.size(); i++) {

			DataSchema schema = schemaList.get(i);
			String item = record.get(i);

			switch (schema.getType()) {
				case DataSchema.STRING_TYPE: {
					StringSchema stringSchema = (StringSchema) schema;
	
					// if the string is less than the schema expected length
					if (!stringSchema.validateNull(item)) {
						outputLog.appendFinalString(schema.getDefaultValue());
	
						String errorFile = config.get("out_base_dir_err")
								+ config.get("orig_file_name");
						String error = outputLog.createErrorString(context,
								ValidationService.DATA_NULL_ERR_CD, schema, config,
								rowNumber, item);
						outputLog.write("Error", NullWritable.get(),
								new Text(error), errorFile);
					} else if (!stringSchema.validateLength(item)) {
						outputLog.appendFinalString(schema.getDefaultValue());
	
						String errorFile = config.get("out_base_dir_err")
								+ config.get("orig_file_name");
						String error = outputLog.createErrorString(context,
								ValidationService.DATA_LTH_ERR_CD, schema, config,
								rowNumber, item);
						outputLog.write("Error", NullWritable.get(),
								new Text(error), errorFile);
					} else {
						outputLog.appendFinalString(item);
					}
	
					outputLog.appendOriginalString(item);
	
					break;
				}
				case DataSchema.DATE_TYPE: {
					DateSchema dateSchema = (DateSchema) schema;
	
					if (dateSchema.checkValidValues(item.trim())) {
						outputLog.appendFinalString(item.trim());
					} else {
						if (!dateSchema.validateFormat(item.trim())) {
							outputLog.appendFinalString(dateSchema
									.getDefaultValue());
	
							String errorFile = config.get("out_base_dir_err")
									+ config.get("orig_file_name");
							String error = outputLog.createErrorString(context,
									ValidationService.DATA_FMT_ERR_CD, schema,
									config, rowNumber, item);
							outputLog.write("Error", NullWritable.get(), new Text(
									error), errorFile);
						} else {
							//String formattedDate = dateSchema.formatDate(item.trim());
							outputLog.appendFinalString(item.trim());
						}
					}
	
					outputLog.appendOriginalString(item);
	
					break;
				}
				case DataSchema.DECIMAL_TYPE: {
					DecimalSchema decimalSchema = (DecimalSchema) schema;
	
					if (decimalSchema.checkValidValues(item.trim())) {
						outputLog.appendFinalString(item.trim());
					} else {
						if (!decimalSchema.validatedelimitedFormat(item.trim())) {
							outputLog.appendFinalString(decimalSchema
									.getDefaultValue());
	
							String errorFile = config.get("out_base_dir_err")
									+ config.get("orig_file_name");
							String error = outputLog.createErrorString(context,
									ValidationService.DATA_FMT_ERR_CD, schema,
									config, rowNumber, item);
							outputLog.write("Error", NullWritable.get(), new Text(
									error), errorFile);
						} else {
							outputLog.appendFinalString(decimalSchema
									.removeLeadingZeroes(item.trim()));
						}
					}
	
					outputLog.appendOriginalString(item);
	
					break;
				}
				case DataSchema.INTEGER_TYPE: {
					IntegerSchema integerSchema = (IntegerSchema) schema;
	
					if (integerSchema.checkValidValues(item.trim())) {
						outputLog.appendFinalString(item.trim());
					} else {
						if (!integerSchema.validatedelimitedFormat(item.trim())) {
							outputLog.appendFinalString(integerSchema
									.getDefaultValue());
	
							String errorFile = config.get("out_base_dir_err")
									+ config.get("orig_file_name");
							String error = outputLog.createErrorString(context,
									ValidationService.DATA_FMT_ERR_CD, schema,
									config, rowNumber, item);
							outputLog.write("Error", NullWritable.get(), new Text(
									error), errorFile);
						} else {
							outputLog.appendFinalString(integerSchema
									.removeLeadingZeroes(item.trim()));
						}
					}
	
					outputLog.appendOriginalString(item);
				}
			}
		}

		String sslFile = config.get("out_base_dir_ssl")
				+ config.get("orig_file_name");
		String finalString;

		if (!outputLog.hasError())
			finalString = outputLog.getValidFinalString(context, config,
					rowNumber);
		else
			finalString = outputLog.getInvalidFinalString(context, config,
					rowNumber);
		outputLog.write("Good", NullWritable.get(),
				new Text(finalString.toString()), sslFile);

		String rslFile = config.get("out_base_dir_rsl")
				+ config.get("orig_file_name");
		String originalString = outputLog.getOriginalString(context, config,
				rowNumber);
		outputLog.write("Orig", NullWritable.get(),
				new Text(originalString.toString()), rslFile);
	}

	public void validateRecord(Context context, String record, String rowNumber)
			throws IOException, InterruptedException {
		outputLog.clear();
		int pos = 0;

		for (DataSchema schema : schemaList) {
			if (record.length() < (pos + schema.getLength())) {
				outputLog.appendFinalString(schema.getDefaultValue());
				outputLog.appendOriginalString(record.substring(pos));

				String errorFile = config.get("out_base_dir_err")
						+ config.get("orig_file_name");
				String error = outputLog.createErrorString(context,
						ValidationService.DATA_LTH_ERR_CD, schema, config,
						rowNumber, record.substring(pos));
				outputLog.write("Error", NullWritable.get(), new Text(error),
						errorFile);
			} else {
				switch (schema.getType()) {
					case DataSchema.STRING_TYPE: {
						StringSchema stringSchema = (StringSchema) schema;
	
						String item = record.substring(pos,
								pos + stringSchema.getLength());
						outputLog.appendFinalString(item);
						outputLog.appendOriginalString(item);
	
						pos = pos + stringSchema.getLength();
	
						break;
					}
					case DataSchema.DATE_TYPE: {
						DateSchema dateSchema = (DateSchema) schema;
	
						String item = record.substring(pos,
								pos + dateSchema.getLength());
	
						if (dateSchema.checkValidValues(item.trim())) {
							outputLog.appendFinalString(item.trim());
						} else {
							if (dateSchema.validateFWNull(item)) {
								outputLog.appendFinalString(item.trim());
							} else if (!dateSchema.validateFormat(item)) {
								outputLog.appendFinalString(dateSchema
										.getDefaultValue());
	
								String errorFile = config.get("out_base_dir_err")
										+ config.get("orig_file_name");
								String error = outputLog.createErrorString(context,
										ValidationService.DATA_FMT_ERR_CD, schema,
										config, rowNumber, item);
								outputLog.write("Error", NullWritable.get(),
										new Text(error), errorFile);
							} else {
								//String formattedDate = dateSchema.formatDate(item);
								outputLog.appendFinalString(item.trim());
							}
						}
						outputLog.appendOriginalString(item);
						pos = pos + dateSchema.getLength();
	
						break;
					}
					case DataSchema.DECIMAL_TYPE: {
						DecimalSchema decimalSchema = (DecimalSchema) schema;
	
						String item = record.substring(pos,
								pos + decimalSchema.getLength());
	
						if (decimalSchema.checkValidValues(item.trim())) {
							outputLog.appendFinalString(item.trim());
						} else {
							if (decimalSchema.validateFWNull(item)) {
								outputLog.appendFinalString(item.trim());
							} else if (!decimalSchema.validateFormat(item.trim())) {
								outputLog.appendFinalString(decimalSchema
										.getDefaultValue());
	
								String errorFile = config.get("out_base_dir_err")
										+ config.get("orig_file_name");
								String error = outputLog.createErrorString(context,
										ValidationService.DATA_FMT_ERR_CD, schema,
										config, rowNumber, item);
								outputLog.write("Error", NullWritable.get(),
										new Text(error), errorFile);
							} else {
								outputLog.appendFinalString(decimalSchema
										.removeLeadingZeroes(item));
							}
						}
						outputLog.appendOriginalString(item);
						pos = pos + decimalSchema.getLength();
	
						break;
					}
					case DataSchema.INTEGER_TYPE: {
						IntegerSchema integerSchema = (IntegerSchema) schema;
	
						String item = record.substring(pos,
								pos + integerSchema.getLength());
	
						if (integerSchema.checkValidValues(item.trim())) {
							outputLog.appendFinalString(item.trim());
						} else {
							if (integerSchema.validateFWNull(item)) {
								outputLog.appendFinalString(item.trim());
							} else if (!integerSchema.validateFormat(item.trim())) {
								outputLog.appendFinalString(integerSchema
										.getDefaultValue());
	
								String errorFile = config.get("out_base_dir_err")
										+ config.get("orig_file_name");
								String error = outputLog.createErrorString(context,
										ValidationService.DATA_FMT_ERR_CD, schema,
										config, rowNumber, item);
								outputLog.write("Error", NullWritable.get(),
										new Text(error), errorFile);
							} else {
								outputLog.appendFinalString(integerSchema
										.removeLeadingZeroes(item));
							}
						}
						outputLog.appendOriginalString(item);
						pos = pos + integerSchema.getLength();
	
						break;
					}
				}
			}
		}

		String sslFile = config.get("out_base_dir_ssl")
				+ config.get("orig_file_name");
		String finalString;

		if (!outputLog.hasError())
			finalString = outputLog.getValidFinalString(context, config,
					rowNumber);
		else
			finalString = outputLog.getInvalidFinalString(context, config,
					rowNumber);
		outputLog.write("Good", NullWritable.get(),
				new Text(finalString.toString()), sslFile);

		String rslFile = config.get("out_base_dir_rsl")
				+ config.get("orig_file_name");
		String originalString = outputLog.getOriginalString(context, config,
				rowNumber);
		outputLog.write("Orig", NullWritable.get(),
				new Text(originalString.toString()), rslFile);
	}
}
