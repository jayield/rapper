package org.github.isel.rapper;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface Id {
    boolean isIdentity() default false;
}
