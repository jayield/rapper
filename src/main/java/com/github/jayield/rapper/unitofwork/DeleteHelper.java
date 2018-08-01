package com.github.jayield.rapper.unitofwork;

import com.github.jayield.rapper.DomainObject;

import java.util.Queue;

public class DeleteHelper extends AbstractCommitHelper {
    private final Queue<DomainObject> dirtyObjects;

    public DeleteHelper(UnitOfWork unit, Queue<DomainObject> list, Queue<DomainObject> dirtyObjects) {
        super(unit, list);
        this.dirtyObjects = dirtyObjects;
    }

//    @Override
//    public CompletableFuture<Void> commitNext() {
//        if (objectIterator == null) objectIterator = list.iterator();
//        if (objectIterator.hasNext()) {
//            DomainObject domainObject = objectIterator.next();
//            return getMapper(domainObject.getClass()).delete(unit, domainObject);
//        }
//        return null;
//    }

    @Override
    public Object identityMapUpdateNext() {
        if (objectIterator == null) objectIterator = list.iterator();
        if (objectIterator.hasNext()) {
            DomainObject object = objectIterator.next();
            unit.invalidate(object.getClass(), object.getIdentityKey());
            return true;
        }
        return null;
    }

    @Override
    public Object rollbackNext() {
        if (objectIterator == null) objectIterator = list.iterator();
        if (objectIterator.hasNext()) {
            DomainObject domainObject = objectIterator.next();
            if (dirtyObjects.contains(domainObject)) rollbackNext();
            unit.validate(domainObject.getIdentityKey(), domainObject);
            return true;
        }
        return null;
    }
}
