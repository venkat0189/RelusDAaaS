
import java.io.BufferedReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.gson.Gson;

public class SchemaService {
	
	public static final Log log = LogFactory.getLog(SchemaService.class);
	public static final String SCHEMA_ATTRIBUTES_REGEX = "^\\{.*\\}$";
	
	public String delimiter = "";
	public String finalDelimiter = "";
	public String quote = "";
	public int prefix = 3;
	
	public List<DataSchema> readSchemaFile(BufferedReader br) {
		List<DataSchema> schemaList = new LinkedList<DataSchema>();
		
		try {
			String line;			
			
			int lineCount = 0;
			boolean parse = false;
			while ((line = br.readLine()) != null) {
				line = line.trim();
				
				if (line.matches(SCHEMA_ATTRIBUTES_REGEX))
					setSchemaParameters(line);
				
				if (line.equals(")")) {
					log.info("End of file reached. " + lineCount + " column definitions parsed.");
					return schemaList;
				}
				
				if (!parse)
					parse = line.equals("(") ? true : false;
				else if (parse) {
					DataSchema dataSchema = parseColumnDefinition(line);
					
					if (dataSchema == null) {
						log.info("Failed to completely parse schema file! Attempted to parse: '" + line + "'");
						return new LinkedList<DataSchema>();
					}
					
					schemaList.add(dataSchema);
					lineCount++;
				}
				
			}					
			br.close();
					
		} catch (IOException e) {
			log.error(e);
		}
		
		return schemaList;
	}
	
	public static DataSchema parseColumnDefinition(String line) {
		DataSchema dataSchema = null;

		Pattern pattern = Pattern.compile(DataSchema.SCHEMA_REGEX);
		Matcher matcher = pattern.matcher(line);
		
		while(matcher.find()) {
            String columnName = matcher.group(1);
            boolean nullable = (matcher.group(2) != null) ? true : false;	            
            String dataTypeName = matcher.group(3);
            int dataType = DataSchema.DATATYPE_MAP.get(dataTypeName);
            String lengthDefinition = matcher.group(5);
            String attributes = matcher.group(7);
            
            switch (dataType) {
	            case DataSchema.DATE_TYPE: {
	            	dataSchema = new DateSchema();
	            	
	            	dataSchema.setAttributes(attributes, nullable);
	            	dataSchema.setColumnName(columnName);
	            	dataSchema.setLength(lengthDefinition);	
	            	
	            	break;
	            }
	            case DataSchema.DECIMAL_TYPE: {
	            	dataSchema = new DecimalSchema();

	            	dataSchema.setAttributes(attributes, nullable);
	            	dataSchema.setColumnName(columnName);	            	
	            	dataSchema.setLength(lengthDefinition);	
	            	
	            	break;
	            }
	            case DataSchema.STRING_TYPE: {
	            	dataSchema = new StringSchema();
	            	
	            	dataSchema.setAttributes(attributes, nullable);
	            	dataSchema.setColumnName(columnName);
	            	dataSchema.setLength(lengthDefinition);		
	            	
	            	break;
	            }
	            case DataSchema.INTEGER_TYPE: {
	            	dataSchema = new IntegerSchema();
	            	
	            	dataSchema.setAttributes(attributes, nullable);
	            	dataSchema.setColumnName(columnName);
	            	
	            	break;
	            }
	            default: {
	            	log.info("This is a unknown type!");
	            	break;
	            }
            }
        }		
		
		return dataSchema;
	}
	
	public void setSchemaParameters(String paramsString) {		
		
		Pattern pattern = Pattern.compile("\\{(.*)\\}");
		Matcher matcher = pattern.matcher(paramsString);
		
		if (!matcher.find())
			return;
		
		paramsString = matcher.group(1);
		String[] params = paramsString.split(",");
		
		for (String param : params) {
			pattern = Pattern.compile("([a-z_]+)='?([^']+)'?");
			matcher = pattern.matcher(param);
			while(matcher.find()) {
				if ("final_delim".equals(matcher.group(1)))
					finalDelimiter = matcher.group(2);
				else if ("delim_string".equals(matcher.group(1))) {
					delimiter = matcher.group(2);
					delimiter = ("none".equals(delimiter)) ? "" : delimiter;
				}
				else if ("delim".equals(matcher.group(1))) {
					delimiter = matcher.group(2);
					delimiter = ("none".equals(delimiter)) ? "" : delimiter;
				}
				else if ("quote".equals(matcher.group(1))) {
					quote = matcher.group(2);
				}
			}
		}
	}
	
	@Override
	public String toString() {
		return new Gson().toJson(this);
	}
}
