package com.github.jayield.rapper.utils.helpers;

import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.utils.UnitOfWork;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static com.github.jayield.rapper.utils.MapperRegistry.*;

public class DeleteHelper extends AbstractCommitHelper {
    private final Queue<DomainObject> dirtyObjects;

    public DeleteHelper(Queue<DomainObject> list, Queue<DomainObject> dirtyObjects) {
        super(list);
        this.dirtyObjects = dirtyObjects;
    }

    @Override
    public CompletableFuture<Void> commitNext(UnitOfWork unit) {
        if (objectIterator == null) objectIterator = list.iterator();
        if (objectIterator.hasNext()) {
            DomainObject domainObject = objectIterator.next();
            return getMapper(domainObject.getClass()).delete(unit, domainObject);
        }
        return null;
    }

    @Override
    public CompletableFuture<Void> identityMapUpdateNext() {
        if (objectIterator == null) objectIterator = list.iterator();
        if (objectIterator.hasNext()) {
            DomainObject object = objectIterator.next();
            getRepository(object.getClass()).invalidate(object.getIdentityKey());
            return getExternal(object.getClass()).removeReferences(object);
        }
        return null;
    }

    @Override
    public void rollbackNext() {
        if (objectIterator == null) objectIterator = list.iterator();
        if (objectIterator.hasNext()) {
            DomainObject domainObject = objectIterator.next();
            if (dirtyObjects.contains(domainObject)) rollbackNext();
            getRepository(domainObject.getClass()).validate(domainObject.getIdentityKey(), domainObject);
        }
    }
}
