package com.github.jayield.rapper.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ColumnName {

    //Name of the columns in the table of the current DomainObject where the ID of the external is.
    //Overwriting this method means the field annotated must be a CompletableFuture<DomainObject>
    String [] name() default {};

    //Name of the columns in the table of the DomainObject in the annotated field where the ID of the current object is.
    //Overwriting this method means the field annotated must be a CompletableFuture<List<DomainObject>>
    String[] foreignName() default {};

    //Name of the table of the relation NN
    //Overwriting this method means the field annotated must be a CompletableFuture<List<DomainObject>>
    String table() default "";

    //Name of the other column(s) with the Id of the other Entity
    //The names must be in the same order as the constructor
    String[] externalName() default {};
}
