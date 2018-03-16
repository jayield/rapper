package org.github.isel.rapper;

import org.github.isel.rapper.utils.ReflectionUtils;

import java.lang.reflect.Field;
import java.sql.Date;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.System.out;

public class DataMapper<T, K> implements Mapper<T, K> {

    private final ConcurrentMap<K,T> identityMap = new ConcurrentHashMap<>();
    private final String selectQuery;
    private final String insertQuery;
    private final String updateQuery;
    private final String deleteQuery;
    private final Class<T> type;
    private final List<Class> subClass;
    private final Class<K> keyType;

    public DataMapper(Class<T> type, Class<K> keyType){
        this.type = type;
        this.keyType = keyType;
        this.subClass = ReflectionUtils.walkInheritanceTreeFor(type).collect(Collectors.toList());

        List<Field> fields = Arrays.stream(type.getDeclaredFields())
                .filter(f ->
                        f.isAnnotationPresent(EmbeddedId.class) ||
                        f.getType().isPrimitive() ||
                        f.getType().isAssignableFrom(String.class) ||
                        f.getType().isAssignableFrom(Date.class))
                .collect(Collectors.toList());

        StringJoiner select = new StringJoiner(", ", "select ", " from "+type.getSimpleName());
        StringJoiner delete = new StringJoiner(" and ", "delete from "+type.getSimpleName()+" where ", "");
        StringJoiner insert = new StringJoiner(", ", "insert into "+type.getSimpleName()+" ( ", " )");
        StringJoiner insertAuxOutput = new StringJoiner(", ");
        StringJoiner insertAuxValues = new StringJoiner( ", ", " values ( ", " )");
        StringJoiner update = new StringJoiner(" and ", "update "+type.getSimpleName()+" set ", " where ");
        StringJoiner updateAuxIds = new StringJoiner(", ");

        queryBuilder(fields, f-> {
            insertAuxOutput.add("inserted."+f.getName());
        }, f -> {
            select.add(f.getName());
            delete.add(f.getName()+" = ?");
            insert.add(f.getName());
            insertAuxValues.add("?");
            updateAuxIds.add(f.getName() +" = ?");
        }, f -> {
            select.add(f.getName());
            insert.add(f.getName());
            insertAuxValues.add("?");
            update.add(f.getName() + " = ?");
        });

        selectQuery = select.toString();
        insertQuery = insert.toString() + (insertAuxOutput.length()==0? "": " output "+insertAuxOutput.toString()) + insertAuxValues.toString();
        updateQuery = update.toString() + updateAuxIds.toString();
        deleteQuery = delete.toString();

    }

    private void queryBuilder(List<Field> fields, Consumer<Field> identity, Consumer<Field> ids, Consumer<Field> others){
        fields.forEach(f -> {
            if (f.isAnnotationPresent(EmbeddedId.class)){ //chaves compostas
                Arrays.stream(f.getType().getDeclaredFields()).forEach(ids);
            } else if(f.isAnnotationPresent(Id.class)){
                if(f.getAnnotation(Id.class).isIdentity()){
                    identity.accept(f);
                    return;
                }
                ids.accept(f);
            } else{
                others.accept(f);
            }
        });
    }

    @Override
    public CompletableFuture<T> getById(K id) {
        return null;
    }

    @Override
    public CompletableFuture<List<T>> getAll() {
        return null;
    }

    @Override
    public void insert(T obj) {

    }

    @Override
    public void update(T obj) {

    }

    @Override
    public void delete(T obj) {

    }

    public ConcurrentMap<K, T> getIdentityMap() {
        return identityMap;
    }

    public String getSelectQuery() {
        return selectQuery;
    }

    public String getInsertQuery() {
        return insertQuery;
    }

    public String getUpdateQuery() {
        return updateQuery;
    }

    public String getDeleteQuery() {
        return deleteQuery;
    }
}
