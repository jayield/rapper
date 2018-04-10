package org.github.isel.rapper;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface ColumnName {
    //If embeddedId separate the Ids with "|" ex: name = "Id|SomeOtherId"
    String name();
    String table() default "";
}
