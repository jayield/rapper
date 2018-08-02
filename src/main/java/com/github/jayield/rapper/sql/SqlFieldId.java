package com.github.jayield.rapper.sql;

import com.github.jayield.rapper.DomainObject;

import java.lang.reflect.Field;
import java.util.stream.Stream;

public class SqlFieldId extends SqlField{
    private final boolean identity;
    private final boolean embeddedId;
    private boolean isFromParent = false;

    public SqlFieldId(Field field, String name, String queryValue, boolean identity, boolean embeddedId) {
        super(field, name, queryValue);
        this.identity = identity;
        this.embeddedId = embeddedId;
    }

    @Override
    public Stream<Object> getValuesForStatement(Object obj) {
        Object key = null;
        if(obj != null) {
            if (DomainObject.class.isAssignableFrom(obj.getClass()))
                key = ((DomainObject) obj).getIdentityKey();
            else
                key = obj;
        }
        if(embeddedId)
            return super.getValuesForStatement(key);
        else
            return Stream.of(key);
    }

    @Override
    public int byUpdate() {
        return 1;
    }

    @Override
    public int byInsert() {
        return identity && !isFromParent ? 3 : 0;
    }

    public boolean isFromParent() {
        return isFromParent;
    }

    public void setFromParent() {
        isFromParent = true;
    }

    public boolean isIdentity() {
        return identity;
    }
}
