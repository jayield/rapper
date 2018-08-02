package com.github.jayield.rapper.mapper.externals;

import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.sql.SqlFieldExternal;
import java.util.concurrent.CompletableFuture;

public interface Populate<T extends DomainObject> {
    CompletableFuture<Void> execute(T t, SqlFieldExternal sqlFieldExternal);
}

