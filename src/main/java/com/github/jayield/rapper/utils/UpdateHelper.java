package com.github.jayield.rapper.utils;

import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.exceptions.DataMapperException;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static com.github.jayield.rapper.utils.MapperRegistry.*;

public class UpdateHelper extends AbstractCommitHelper {
    private final Queue<DomainObject> clonedObjects;
    private final Queue<DomainObject> removedObjects;

    protected UpdateHelper(Queue<DomainObject> dirtyObjects, Queue<DomainObject> clonedObjects, Queue<DomainObject> removedObjects) {
        super(dirtyObjects);
        this.clonedObjects = clonedObjects;
        this.removedObjects = removedObjects;
    }

    @Override
    CompletableFuture<Void> commitNext(UnitOfWork unit) {
        if (objectIterator == null) objectIterator = list.iterator();
        if (objectIterator.hasNext()) {
            DomainObject domainObject = objectIterator.next();
            if (removedObjects.contains(domainObject)) return commitNext(unit);
            return getMapper(domainObject.getClass()).update(unit, domainObject);
        }
        return null;
    }

    @Override
    CompletableFuture<Void> identityMapUpdateNext() {
        if (objectIterator.hasNext()) {
            DomainObject object = objectIterator.next();
            getRepository(object.getClass()).validate(object.getIdentityKey(), object);

            DomainObject prevDomainObj = clonedObjects.stream()
                    .filter(domainObject1 -> domainObject1.getIdentityKey().equals(object.getIdentityKey()))
                    .findFirst()
                    .orElseThrow(() -> new DataMapperException("Previous state of the updated domainObject not found"));
            return getExternal(object.getClass()).updateReferences(prevDomainObj, object);
        }
        return null;
    }
}
