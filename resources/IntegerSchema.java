package com.tiaa.esp.hadoop.model.schema;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.Gson;

public class IntegerSchema extends DataSchema {
//
	private int bitType;
	private String columnName;
	private String defaultValue;
	private int length;
	private boolean nullable;
	private boolean signed = true;
	private String[] validValues; // Change for Valid values req
	
	public IntegerSchema() {}
	
	public IntegerSchema(String columnName, String defaultValue) {
		this.setColumnName(columnName);
		this.setDefaultValue(defaultValue);		
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
					if ("default".equals(matcher.group(1)))
						this.setDefaultValue(matcher.group(3));
					else if ("width".equals(matcher.group(1)))
						this.setLength(matcher.group(3));
					else if ("unsigned".equals(matcher.group(1)))
						this.setSigned(false);
					// Change for Valid values req	
					else if ("valid_value".equals(matcher.group(1)))
						this.setValidValues(matcher.group(3));
				}
			}
		} catch (Exception e) {
			System.out.println("IntegerSchema.setAttributes() error: " + e);					
		}
	}
	
	public void setBitType(String bitType) {
		Pattern pattern = Pattern.compile("int([0-9]+)");
		Matcher matcher = pattern.matcher(bitType);
		
		while(matcher.find()) {
			this.bitType = Integer.parseInt(matcher.group(1));
		}
	}
	
	public int getBitType() {
		return bitType;
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
		this.defaultValue = (defaultValue == null) ? "0" : defaultValue;
	}
	
	public int getLength() {		
		return length;
	}
	
	public void setLength(String length) {
		this.length = Integer.parseInt(length);
	}
	
	public void setSigned(boolean signed) {
		this.signed = signed;
	}
	
	public int getType() {
		return DataSchema.INTEGER_TYPE;
	}

	public String getTypeName() {
		return "INTEGER";
	}
	
	// Change for Valid values req
	public String getValidValues() {
		return new Gson().toJson(validValues);
	}
		
	public void setValidValues(String validValues) {
		this.validValues = validValues.split(",");
	}
	// Change for Valid values req	
		
	@Override
	public String toString() {
		return new Gson().toJson(this);
	}
	
	public boolean hasSign(String str) {
		Pattern p = Pattern.compile("^[-+ ]");
	    Matcher m = p.matcher(str);
	    
	    return m.find();
	}
	
	public boolean isNullable() {
		return nullable;
	}
	
	public boolean isSigned() {
		return this.signed;
	}
	
	public String removeLeadingZeroes(String integer) {
		boolean leadingRead = false;
		char signedChar;
		int pos = 0;
		
		for (int i = 0; i < integer.length(); i++) {
			if (Character.isDigit(integer.charAt(i)) && !leadingRead) {
				leadingRead = true;
			}
			if (leadingRead) {
				pos = i;
				if (integer.charAt(i) != '0')
					break;
			}				
		}
		
		if (isSigned()) {
			 signedChar = (integer.indexOf("-") > -1) ? '-' : ' ';
			 return signedChar + integer.substring(pos, integer.length()).trim();
		}
		else
			return integer.substring(pos, integer.length()).trim();
	}
	
	public boolean validateFormat(String str) {
		
		if (str.indexOf("-") > -1 && !isSigned())
			return false;
		
		// Test if string is numeric
		try {
			Integer.parseInt(str);
			return true;
		}
		catch (NumberFormatException e) {
			return false;
		}
	}
	
    public boolean validatedelimitedFormat(String str) {
		
		// Test if string is numeric
		try {
			Integer.parseInt(str);
			return true;
		}
		catch (NumberFormatException e) {
			return false;
		}
	}

	public boolean validateFWNull(String str) {
		return (nullable) ? str.trim().equals("") : false;
	}
	
	public boolean validateLength(String str) {
		return str.length() == length;
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