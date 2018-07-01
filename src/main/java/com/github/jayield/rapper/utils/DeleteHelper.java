package com.github.jayield.rapper.utils;

import com.github.jayield.rapper.DomainObject;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static com.github.jayield.rapper.utils.MapperRegistry.*;

public class DeleteHelper extends AbstractCommitHelper {
    protected DeleteHelper(Queue<DomainObject> list) {
        super(list);
    }

    @Override
    CompletableFuture<Void> commitNext(UnitOfWork unit) {
        if (objectIterator == null) objectIterator = list.iterator();
        if (objectIterator.hasNext()) {
            DomainObject domainObject = objectIterator.next();
            return getMapper(domainObject.getClass()).delete(unit, domainObject);
        }
        return null;
    }

    @Override
    CompletableFuture<Void> identityMapUpdateNext() {
        if (objectIterator.hasNext()) {
            DomainObject object = objectIterator.next();
            getRepository(object.getClass()).invalidate(object.getIdentityKey());
            return getExternal(object.getClass()).removeReferences(object);
        }
        return null;
    }
}
