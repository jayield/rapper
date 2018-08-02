package com.github.jayield.rapper.utils;

import io.vertx.core.json.JsonArray;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CollectionUtils {

    private CollectionUtils() {
    }

    public static <T> Collector<T, JsonArray, JsonArray> toJsonArray(){
        return Collector.of(JsonArray::new, (prev, curr) -> {
            if(curr == null)
                prev.addNull();
            else
                prev.add(curr);
        }, JsonArray::addAll);
    }

    public static <T> Collector<T, JsonArray, JsonArray> toJsonArray(BiConsumer<T, JsonArray> action){
        return Collector.of(JsonArray::new, (prev, curr) -> action.accept(curr, prev), JsonArray::addAll);
    }

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

    /**
     * Converts a List<CompletableFuture<L>> into a CompletableFuture<List<L>>
     *
     * @param futureList
     * @return
     */
    public static <L> CompletableFuture<List<L>> listToCompletableFuture(List<CompletableFuture<L>> futureList) {
        return CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]))
                .thenApply(v -> futureList
                        .stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList()));
    }

    public static class Indexer<T> {
        public final T item;
        public final int index;

        public Indexer(T item, int index) {
            this.item = item;
            this.index = index;
        }
    }
}
