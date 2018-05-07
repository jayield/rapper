package com.github.jayield.rapper.utils;

import com.github.jayield.rapper.exceptions.DataMapperException;

import java.lang.reflect.Field;
import java.util.Objects;

public abstract class EmbeddedIdClass {

    private static final Field objectsField;

    static {
        try {
            objectsField = EmbeddedIdClass.class.getDeclaredField("objects");
            objectsField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new DataMapperException(e);
        }
    }

    private final Object[] objects;

    private EmbeddedIdClass() {
        objects = null;
    }

    protected EmbeddedIdClass(Object... objects) {
        this.objects = objects;
    }

    public static Field getObjectsField() {
        return objectsField;
    }

    @Override
    public int hashCode() {
        return Objects.hash(objects);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        EmbeddedIdClass other = (EmbeddedIdClass) obj;
        for (int i = 0; i < this.objects.length; i++) {
            if(!this.objects[i].equals(other.objects[i]))
                return false;
        }
        return true;
    }
}
