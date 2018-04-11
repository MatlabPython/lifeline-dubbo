package com.gsafety.bigdata.lifeline.avro;

import java.io.Serializable;
import java.util.List;

public class ResultBean implements Serializable {

	/**
	* 
	*/
	private static final long serialVersionUID = -4741976063252370966L;

	public String dataType;
	public Long time;
	public Integer level;
	public List<java.lang.Float> values;

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public Long getTime() {
		return time;
	}

	public void setTime(Long time) {
		this.time = time;
	}

	public Integer getLevel() {
		return level;
	}

	public void setLevel(Integer level) {
		this.level = level;
	}

	public List<java.lang.Float> getValues() {
		return values;
	}

	public void setValues(List<java.lang.Float> values) {
		this.values = values;
	}

}
