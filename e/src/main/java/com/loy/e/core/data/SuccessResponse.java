package com.loy.e.core.data;

/**
 * 
 * @author Loy Fu qq群 540553957
 * @since 1.7
 * @version 1.0.0
 * 
 */
public class SuccessResponse extends Response{
	
	public SuccessResponse(){
		super(true);
	}
	public static SuccessResponse newInstance(){
		return new SuccessResponse();
	}
	
}