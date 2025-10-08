package com.action.kafka0common.exception;

/**
 * 业务异常类
 * 
 * @author action
 * @since 2024
 */
public class BusinessException extends RuntimeException {
    
    private static final long serialVersionUID = 1L;
    
    private String code;
    
    public BusinessException(String message) {
        super(message);
        this.code = "9999";
    }
    
    public BusinessException(String code, String message) {
        super(message);
        this.code = code;
    }
    
    public BusinessException(String message, Throwable cause) {
        super(message, cause);
        this.code = "9999";
    }
    
    public BusinessException(String code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }
    
    public String getCode() {
        return code;
    }
    
    public void setCode(String code) {
        this.code = code;
    }
}
