
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.Gson;

public class StringSchema extends DataSchema {

	private String columnName;
	private int length;
	private boolean nullable = false;
	private boolean variable = false;
	private boolean isFiller = false;
	
	public StringSchema() {}
	
	public StringSchema(String length, String columnName) {
		this.setLength(length);
		this.setColumnName(columnName);
	}
	
	public StringSchema(int length, String columnName) {
		this.setLength(String.valueOf(length));
		this.setColumnName(columnName);
	}
	
	public void setAttributes(String attributes, boolean nullable) {
		this.nullable = nullable;
	}
	
	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	
	public String getDefaultValue() {
		if (nullable)
			return "";
		else {
			if (variable)
				return " ";
			else {
				StringBuilder str = new StringBuilder();
				for (int i = 0; i < length; i++)
					str.append(" ");
				return str.toString();
			}	
		}
	}
	
	public int getLength() {
		return length;
	}
	
	public void setLength(String length) {
		if (length == null) {
			this.isFiller = true;
			return;
		}
		
		if (length.contains("max")) {
			Pattern pattern = Pattern.compile("max=([0-9]+)");
			Matcher matcher = pattern.matcher(length);
			
			while(matcher.find()) {
				this.variable = true;
				this.length = Integer.parseInt(matcher.group(1));
			}
		}
		else {
		  this.length = Integer.parseInt(length);			
		}		
	}
	
	public boolean isVarChar() {
		return variable;
	}
	
	public int getType() {
		return DataSchema.STRING_TYPE;
	}

	public String getTypeName() {
		return "STRING";
	}
	
	@Override
	public String toString() {
		return new Gson().toJson(this);
	}
	
	public boolean isNullable() {
		return nullable;
	}
	
	public boolean validateLength(String str) {
		if (this.isFiller == true) {
			return true;
		}
		else {
		    return (variable) ? str.length() <= length : str.length() == length;
		}
	}
	
	public boolean validateNull(String str) {
		if (!nullable && str.equals(""))			
			return false;
		else 
			return true;
	}
}
