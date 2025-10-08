package com.action.kafka0common.exception;

import com.action.kafka0common.common.Constants;
import com.action.kafka0common.common.R;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.BindingResult;
import org.springframework.validation.FieldError;
import org.springframework.validation.ObjectError;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolationException;
import java.util.List;

/**
 * 全局异常处理器
 *
 * @author action
 * @since 2025
 */
@ControllerAdvice
@Slf4j
@SuppressWarnings("rawtypes")
public class GlobalExceptionHandler {

    /**
     * Form 校验异常处理
     */
    @ExceptionHandler({MethodArgumentNotValidException.class})
    @ResponseBody
    public R bindExceptionHandler(MethodArgumentNotValidException e) {
        BindingResult bindingResult = e.getBindingResult();
        StringBuilder message = new StringBuilder();
        List<FieldError> errors = bindingResult.getFieldErrors();
        for (ObjectError error : errors) {
            message.append("[").append(error.getDefaultMessage()).append("]");
        }
        return R.fail(Constants.ERROR_CODE, message.toString());
    }

    /**
     * RequestParam参数缺少校验
     */
    @ExceptionHandler({MissingServletRequestParameterException.class})
    @ResponseBody
    public R missingParameterExceptionHandler(HttpServletRequest req, MissingServletRequestParameterException e) {
        R res = R.fail(Constants.ERROR_CODE, e.getMessage());
        String errorMsg = "系统出现异常，URI:" + req.getRequestURI() + "，参数信息: " + req.getParameterMap().toString();
        log.warn(errorMsg, e);
        return res;
    }

    @ExceptionHandler({ConstraintViolationException.class})
    @ResponseBody
    public R constraintViolationExceptionHandler(HttpServletRequest req, ConstraintViolationException e) {
        R res = R.fail(Constants.ERROR_CODE, e.getMessage());
        String errorMsg = "请求参数异常，URI:" + req.getRequestURI() + "，参数信息: " + e.getMessage();
        log.warn(errorMsg, e);
        return res;
    }

    /**
     * 业务异常处理
     */
    @ExceptionHandler(value = BusinessException.class)
    @ResponseBody
    public R businessExceptionHandler(HttpServletRequest req, BusinessException e) {
        String errorMsg = "业务异常，URI:" + req.getRequestURI() + "，参数信息: " + req.getParameterMap().toString();
        log.warn(errorMsg, e);
        return R.fail(e.getCode(), e.getMessage());
    }

    @ExceptionHandler(value = Exception.class)
    @ResponseBody
    public R systemError(HttpServletRequest req, Exception e) {
        String errorMsg = "系统出现异常，URI:" + req.getRequestURI() + "，参数信息: " + req.getParameterMap().toString();
        if (e instanceof BusinessException) {
            StackTraceElement[] stackTrace = e.getStackTrace();
            if (stackTrace != null && stackTrace.length > 0) {
                log.warn("BusinessException，URI : {}, message : {} ", req.getRequestURI(), e.getMessage(), e);
            }
            BusinessException be = (BusinessException) e;
            return R.fail(be.getCode(), be.getMessage());
        }

        if (e instanceof HttpRequestMethodNotSupportedException || e instanceof HttpMediaTypeNotSupportedException
                || e instanceof HttpMessageNotReadableException) {
            log.warn(errorMsg + e.getMessage());
        } else {
            log.error(errorMsg, e);
        }

        return R.fail(Constants.ERROR_CODE, "未知错误");
    }
}
