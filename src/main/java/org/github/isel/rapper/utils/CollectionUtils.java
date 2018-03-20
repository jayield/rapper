package org.github.isel.rapper.utils;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CollectionUtils {

    /**
     * Zips the specified stream with its indices.
     */
    public static <T> Stream<Map.Entry<Integer, T>> zipWithIndex(Stream<? extends T> stream) {
        return StreamSupport.stream(new Spliterators.AbstractSpliterator<Map.Entry<Integer, T>>(Long.MAX_VALUE, Spliterator.ORDERED | Spliterator.IMMUTABLE) {
            int[] index = {0};
            Spliterator<? extends T> spliterator = stream.spliterator();
            @Override
            public boolean tryAdvance(Consumer<? super Map.Entry<Integer, T>> action) {
                return spliterator.tryAdvance(i -> action.accept(new AbstractMap.SimpleImmutableEntry<>(index[0]++, i)));
            }
        }, false);
    }

    /**
     * Returns a stream consisting of the results of applying the given two-arguments function to the elements of this stream.
     * The first argument of the function is the element index and the second one - the element value.
     */
    public static <T, R> Stream<R> mapWithIndex(Stream<? extends T> stream, BiFunction<Integer, ? super T, ? extends R> mapper) {
        return zipWithIndex(stream).map(entry -> mapper.apply(entry.getKey(), entry.getValue()));
    }


    public static void main(String[] args) {
        String[] names = {"Sam", "Pamela", "Dave", "Pascal", "Erik"};

        System.out.println("Test zipWithIndex");
        zipWithIndex(Arrays.stream(names)).forEach(System.out::println);

        System.out.println();
        System.out.println("Test mapWithIndex");
        mapWithIndex(Arrays.stream(names), (Integer index, String name) -> index+"="+name).forEach(System.out::println);
    }
}
