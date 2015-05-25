package cz.vutbr.fit.util;

import java.io.Serializable;

public class Output implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 3988866133631029163L;
	private String type;
	private Object data;
	private String id;
	
	public Output(String type,Object data,String id){
		this.type=type;
		this.data=data;
		this.id=id;
	}
	
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public Object getData() {
		return data;
	}
	public void setData(Object data) {
		this.data = data;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}

}
