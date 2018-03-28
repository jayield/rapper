package org.github.isel.rapper.utils;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ReflectionUtils {

    public static Class getGenericType(Type t){
        t = ((ParameterizedType) t).getActualTypeArguments()[0];
        try {
            return (Class<?>) t;
        }catch(ClassCastException e){
            return getGenericType(t);
        }
    }

    public static Stream<Field> allFieldsFor(Class c){
        return walkInheritanceTreeFor(c).flatMap(k -> Arrays.stream(k.getDeclaredFields()));
    }

    public static Stream<Class> walkInheritanceTreeFor(Class c) {
        return iterate(c, k -> Optional.ofNullable(k.getSuperclass()));
    }

    private static  <T> Stream<T> iterate(T seed, Function<T, Optional<T>> fetchNext){
        Objects.requireNonNull(fetchNext);

        return StreamSupport.stream(new Spliterators.AbstractSpliterator<T>(Long.MAX_VALUE, Spliterator.ORDERED) {
            private Optional<T> t = Optional.ofNullable(seed);
            @Override
            public boolean tryAdvance(Consumer<? super T> action) {
                if(!t.isPresent()) return false;
                T v = t.get();
                t = fetchNext.apply(v);
                action.accept(v);
                return true;
            }
        }, false);
    }

//    public static void queryBuilder(Field[] fieds, Consumer<Field> first, Consumer<Field> second, Consumer<Field> third){
//        for(Field f : fieds){
//            if(f.isAnnotationPresent(ID.class)){
//                if(!f.getAnnotation(ID.class).isIdentity()) {
//                    first.accept(f);
//                }
//                second.accept(f);
//            }
//            else
//                third.accept(f);
//        }
//    }
}
