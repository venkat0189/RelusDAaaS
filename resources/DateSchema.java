package com.tiaa.esp.hadoop.model.schema;

import java.text.ParseException;
import java.text.SimpleDateFormat;
//import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.Gson;

public class DateSchema extends DataSchema {

	private String columnName;
	private String defaultValue;
	private String format;
	private boolean nullable;
	private String[] validValues;
	
	public DateSchema() {}
	
	public DateSchema(String format, String defaultValue, String columnName){
		this.setColumnName(columnName);		
		this.setDefaultValue(defaultValue);
		this.setFormat(format);		
	}
	
	public void setAttributes(String attributes, boolean nullable) {
		this.nullable = nullable;
		
		if (attributes == null)
			return;	
		
		try {
			Pattern attributesPattern = Pattern.compile(DataSchema.ATTRIBUTES_REGEX_NG);
			Matcher attributesMatch = attributesPattern.matcher(attributes);
			
			while (attributesMatch.find()) {
				String attribute = attributesMatch.group(0);
				Pattern pattern = Pattern.compile(DataSchema.ATTRIBUTES_REGEX);
				Matcher matcher = pattern.matcher(attribute);
				
				while(matcher.find()) {
					if ("date_format".equals(matcher.group(1)))
						this.setFormat(matcher.group(3));
					else if ("default".equals(matcher.group(1)))
						this.setDefaultValue(matcher.group(3));
					else if ("valid_value".equals(matcher.group(1)))
						this.setValidValues(matcher.group(3));
				}
			}
		} catch (Exception e) {
			System.out.println("DateSchema.setAttributes() error: " + e);					
		}
	}
	
	public String getColumnName() {
		return columnName;
	}
	
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	
	public String getDefaultValue() {
		return defaultValue;
	}

	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}
	
	public String getFormat() {
		return format;
	}
	
	public void setFormat(String format) {
		this.format = format.replace("%", "").replace("m", "M");
	}
	
	public int getLength() {
		return format.length();
	}	
	
	public void setLength(String length) {
		
	}

	public int getType() {
		return DataSchema.DATE_TYPE;
	}

	public String getTypeName() {
		return "DATE";
	}
	
	public String getValidValues() {
		return new Gson().toJson(validValues);
	}
	
	public void setValidValues(String validValues) {
		this.validValues = validValues.split(",");
		
	}
	
	@Override
	public String toString() {
		return new Gson().toJson(this);
	}
	
	public boolean isNullable() {
		return nullable;
	}

	/***Date formatting should not be done so commenting out the format function
	public String formatDate(String dateStr) {
		try {
			SimpleDateFormat parseFormat = new SimpleDateFormat(format);
			Date date = parseFormat.parse(dateStr);
			SimpleDateFormat convertFormat = new SimpleDateFormat("yyyy-MM-dd");
			
			return convertFormat.format(date);
		} catch (Exception e) {
			System.out.println("DateSchema.formatDate() error: " + e);
			return dateStr;
		}		
	}
	
	*/
	
	public boolean validateFormat(String str) {
		try {
			SimpleDateFormat sdformat = new SimpleDateFormat(format);
			sdformat.setLenient(false);			
			sdformat.parse(str);
			
	        return true;
	    }
	    catch(ParseException e){
	    	return false;
	    }
	}
	
	public boolean validateFWNull(String str) {
		return (nullable) ? str.trim().equals("") : false;
	}
	
	public boolean validateLength(String str) {
		return str.length() == format.length();
	}
	
	public boolean checkValidValues(String str) {
		if (validValues == null || validValues.length == 0)
			return false;
		
		for (String validitem: validValues)
		{
			if (str.equals(validitem)) {
				return true;
			}
		}
		
		return false;
	}
}
