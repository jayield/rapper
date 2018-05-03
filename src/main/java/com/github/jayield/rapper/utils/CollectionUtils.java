package com.github.jayield.rapper.utils;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CollectionUtils {

    /**
     * Zips the specified stream with its indices.
     */
    public static <T> Stream<Indexer<T>> zipWithIndex(Stream<? extends T> stream) {
        return StreamSupport.stream(new Spliterators.AbstractSpliterator<Indexer<T>>(Long.MAX_VALUE, Spliterator.ORDERED | Spliterator.IMMUTABLE) {
            int[] index = {0};
            Spliterator<? extends T> spliterator = stream.spliterator();

            @Override
            public boolean tryAdvance(Consumer<? super Indexer<T>> action) {
                return spliterator.tryAdvance(i -> action.accept(new Indexer<T>(i, index[0]++)));
            }
        }, false);
    }

    /**
     * Returns a stream consisting of the results of applying the given two-arguments function to the elements of this stream.
     * The first argument of the function is the element index and the second one - the element value.
     */
    public static <T, R> Stream<R> mapWithIndex(Stream<? extends T> stream, BiFunction<Integer, ? super T, ? extends R> mapper) {
        return zipWithIndex(stream).map(entry -> mapper.apply(entry.index, entry.item));
    }

    public static class Indexer<T> {
        public final T item;
        public final int index;

        public Indexer(T item, int index) {
            this.item = item;
            this.index = index;
        }
    }


    public static void main(String[] args) {
        String[] names = {"Sam", "Pamela", "Dave", "Pascal", "Erik"};

        System.out.println("Test zipWithIndex");
        zipWithIndex(Arrays.stream(names)).forEach(stringIndexer -> System.out.println(stringIndexer.index + "=" + stringIndexer.item));

        System.out.println();
        System.out.println("Test mapWithIndex");
        mapWithIndex(Arrays.stream(names), (Integer index, String name) -> index + "=" + name).forEach(System.out::println);
    }
}
