
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.Gson;

public class DecimalSchema extends DataSchema {

	private String columnName;
	private String defaultValue;
	private int length;
	private boolean nullable;	
	private int precision;
	private int scale;	
	private char separator;
	private boolean signed = true;
	private String[] validValues; // Change for Valid values req	
	
	public DecimalSchema() {}
	
	public DecimalSchema(String columnName, String defaultValue, int scale, int precision, char separator) {
		this.setColumnName(columnName);
		this.setDefaultValue(defaultValue);		
		this.setPrecision(precision);
		this.setScale(scale);
		this.setSeparator(separator);
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
						this.length = Integer.parseInt(matcher.group(3));	
					else if ("unsigned".equals(matcher.group(1)))
						this.setSigned(false);
					// Change for Valid values req	
					else if ("valid_value".equals(matcher.group(1)))
						this.setValidValues(matcher.group(3));
					
				}
			}
		} catch (Exception e) {
			System.out.println("DecimalSchema.setAttributes() error: " + e);					
		}
	}
	
	public String getColumnName() {
		return columnName;
	}
	
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	
	public String getDecimalRegex() {
		return "[-]?\\d{1," + (precision - scale) + "}(\\.\\d{1," + scale + "})?";
	}
	
	public String getDefaultValue() {
		return defaultValue;
	}
	
	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}
	
	public int getLength() {
		return length;
	}
	
	public void setLength(String length) {
		
		try {			
			Pattern pattern = Pattern.compile("([0-9]+),([0-9]+)");
			Matcher matcher = pattern.matcher(length);
			
			while(matcher.find()) {
				this.setPrecision(Integer.parseInt(matcher.group(1)));
	            this.setScale(Integer.parseInt(matcher.group(2)));
			}
		} catch (Exception e) {
			System.out.println("DecimalSchema.setLength() error: " + e);
		}
	}
	
	public int getPrecision() {
		return precision;
	}	
	
	public void setPrecision(int precision) {
		this.precision = precision;
	}
	
	public void setScale(int scale) {
		this.scale = scale;
	}
	
	public char getSeparator() {
		return separator;
	}
	
	public void setSeparator(char separator) {
		this.separator = separator;
	}
	
	public void setSigned(boolean signed) {
		this.signed = signed;
	}
	
	public int getType() {
		return DataSchema.DECIMAL_TYPE;
	}

	public String getTypeName() {
		return "DECIMAL";
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
		Pattern p = Pattern.compile("^[- ]");
	    Matcher m = p.matcher(str);
	    
	    return m.find();
	}
	
	public boolean isNullable() {
		return nullable;
	}
	
	public boolean isSigned() {
		return signed;
	}
	
	public String removeLeadingZeroes(String decimal) {
		boolean leadingRead = false;
		int pos = 0;
		
		for (int i = 0; i < decimal.length(); i++) {
			if (Character.isDigit(decimal.charAt(i)) && !leadingRead) {
				leadingRead = true;
			}
			if (leadingRead) {
				pos = i;
				if (decimal.charAt(i) != '0') {
					if (decimal.charAt(i) == '.')
						pos--;
					break;
				}
			}				
		}
		
		if (isSigned()) {
			 return (decimal.indexOf("-") > -1) ? '-' + decimal.substring(pos, decimal.length()).trim() : decimal.substring(pos, decimal.length()).trim();
		}
		else
			return decimal.substring(pos, decimal.length()).trim();
	}
	
	public boolean validateFormat(String str) {
		
		// Test if decimal is unsigned by has a sign return false
		if (str.indexOf("-") > -1 && !isSigned())
			return false;
		
		// Test if string is numeric
		try {
			Double.parseDouble(str);
		}
		catch (NumberFormatException e) {
			return false;
		}
		
		// Test if string abides by defined precision and scale
		if (str.indexOf(".") > -1) {
			try {
				return Pattern.matches(getDecimalRegex(), str);
			} catch (Exception e) {
				return false;
			}
		}
		else
			return true;
	}
	
public boolean validatedelimitedFormat(String str) {
		
		// Test if string is numeric
		try {
			Double.parseDouble(str);
		}
		catch (NumberFormatException e) {
			return false;
		}
		
		// Test if string abides by defined precision and scale
		if (str.indexOf(".") > -1) {
			try {
				return Pattern.matches(getDecimalRegex(), str);
			} catch (Exception e) {
				return false;
			}
		}
		else
			return true;
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
