package com.loy.e.core.data;

/**
 * 
 * @author Loy Fu qq群 540553957
 * @since 1.7
 * @version 1.0.0
 * 
 */
public class Response {
	private Boolean success;
	public Response(){
		
	}
	public Response(Boolean success){
		this.success = success;
	}
	public Boolean getSuccess() {
		return success;
	}

	public void setSuccess(Boolean success) {
		this.success = success;
	}
	
	
}