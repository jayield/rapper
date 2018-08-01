package com.github.jayield.rapper.connections;

public enum DBsPath {
    DEFAULTDB ("DB_CONNECTION_STRING"),
    TESTDB ("DBTEST_CONNECTION_STRING");

    private String value;
    DBsPath(String value) {
        this.value = value;
    }

    @Override
    public String toString(){
        return value;
    }
}
