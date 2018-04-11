package org.github.isel.rapper;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface ColumnName {
    //If embeddedId separate the Ids with "|" ex: name = "Id|SomeOtherId"
    String name();
    String table() default "";
    //Name of the other column(s) with the Id of the other Entity
    //If embeddedId separate the Ids with "|" ex: name = "Id|SomeOtherId"
    //The names must be in the same order as the constructor
    String foreignName() default "";
}
