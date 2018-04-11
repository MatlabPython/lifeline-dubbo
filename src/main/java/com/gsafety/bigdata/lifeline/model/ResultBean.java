package com.gsafety.bigdata.lifeline.model;

import java.io.Serializable;
import java.util.List;

public class ResultBean implements Serializable {

	/**
	* 
	*/
	private static final long serialVersionUID = -4741976063252370966L;

	public String dataType;
	public Long time;
	public int level;
	public int getLevel() {
		return level;
	}

	public void setLevel(int level) {
		this.level = level;
	}

	public List<Double> values;

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


	public List<Double> getValues() {
		return values;
	}

	public void setValues(List<Double> values) {
		this.values = values;
	}

	@Override
	public String toString() {
		return "ResultBean{" +
				"dataType='" + dataType + '\'' +
				", time=" + time +
				", level=" + level +
				", values=" + values +
				'}';
	}
}
