package com.github.jayield.rapper.utils;

import com.github.jayield.rapper.DataRepository;
import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.EmbeddedId;
import com.github.jayield.rapper.exceptions.DataMapperException;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public interface Populate<T extends DomainObject> {
    CompletableFuture<Void> execute(T t, SqlField.SqlFieldExternal sqlFieldExternal);
}

