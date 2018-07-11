package com.github.jayield.rapper.utils.helpers;

import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.utils.UnitOfWork;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static com.github.jayield.rapper.utils.MapperRegistry.*;

public class UpdateHelper extends AbstractCommitHelper {
    private final Queue<DomainObject> clonedObjects;
    private final Queue<DomainObject> removedObjects;

    public UpdateHelper(UnitOfWork unit, Queue<DomainObject> dirtyObjects, Queue<DomainObject> clonedObjects, Queue<DomainObject> removedObjects) {
        super(unit, dirtyObjects);
        this.clonedObjects = clonedObjects;
        this.removedObjects = removedObjects;
    }

    @Override
    public CompletableFuture<Void> commitNext() {
        if (objectIterator == null) objectIterator = list.iterator();
        if (objectIterator.hasNext()) {
            DomainObject domainObject = objectIterator.next();
            if (removedObjects.contains(domainObject)) return commitNext();
            return getMapper(domainObject.getClass()).update(unit, domainObject);
        }
        return null;
    }

    @Override
    public CompletableFuture<Void> identityMapUpdateNext() {
        if (objectIterator == null) objectIterator = list.iterator();
        if (objectIterator.hasNext()) {
            DomainObject object = objectIterator.next();
            unit.validate(object.getIdentityKey(), object);

            DomainObject prevDomainObj = clonedObjects.stream()
                    .filter(domainObject1 -> domainObject1.getIdentityKey().equals(object.getIdentityKey()))
                    .findFirst()
                    .orElseThrow(() -> new DataMapperException("Previous state of the updated domainObject not found"));
            return getExternal(object.getClass()).updateReferences(prevDomainObj, object);
        }
        return null;
    }

    @Override
    public void rollbackNext() {
        if (objectIterator == null) objectIterator = list.iterator();
        if (objectIterator.hasNext()) {
            DomainObject obj = objectIterator.next();
            clonedObjects.stream()
                    .filter(domainObject -> domainObject.getIdentityKey().equals(obj.getIdentityKey()))
                    .findFirst()
                    .ifPresent(clone -> unit.validate(clone.getIdentityKey(), clone));
        }
    }
}
