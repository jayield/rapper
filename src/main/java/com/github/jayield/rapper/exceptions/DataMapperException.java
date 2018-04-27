package com.github.jayield.rapper.exceptions;

public class DataMapperException extends RuntimeException {
    public DataMapperException(){
        super();
    }

    public DataMapperException(String message){
        super(message);
    }

    public DataMapperException(Throwable cause){
        super(cause);
    }

    public DataMapperException(String message, Throwable cause){
        super(message, cause);
    }

    public DataMapperException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace){
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
