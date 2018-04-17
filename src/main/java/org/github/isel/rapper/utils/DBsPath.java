package org.github.isel.rapper.utils;

public enum DBsPath {
    DEFAULTDB ("DB_CONNECTION_STRING"),
    TESTDB ("DBTEST_CONNECTION_STRING");

    String value;
    DBsPath(String value) {
        this.value = value;
    }

    @Override
    public String toString(){
        return value;
    }
}
