package com.github.jayield.rapper.utils.helpers;

import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.utils.UnitOfWork;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static com.github.jayield.rapper.utils.MapperRegistry.*;

public class CreateHelper extends AbstractCommitHelper {
    public CreateHelper(UnitOfWork unit, Queue<DomainObject> list) {
        super(unit, list);
    }

    @Override
    public CompletableFuture<Void> commitNext() {
        if (objectIterator == null) objectIterator = list.iterator();
        if (objectIterator.hasNext()) {
            DomainObject domainObject = objectIterator.next();
            return getMapper(domainObject.getClass()).create(unit, domainObject);
        }
        return null;
    }

    @Override
    public Object identityMapUpdateNext() {
        if (objectIterator == null) objectIterator = list.iterator();
        if (objectIterator.hasNext()) {
            DomainObject object = objectIterator.next();
            unit.validate(object.getIdentityKey(), object);
            return true;
        }
        return null;
    }

    @Override
    public Object rollbackNext() {
        if (objectIterator == null) objectIterator = list.iterator();
        if (objectIterator.hasNext()) {
            DomainObject domainObject = objectIterator.next();
            unit.invalidate(domainObject.getClass(), domainObject.getIdentityKey());
            return true;
        }
        return null;
    }
}
