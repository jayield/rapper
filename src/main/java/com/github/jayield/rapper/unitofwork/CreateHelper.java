package com.github.jayield.rapper.unitofwork;

import com.github.jayield.rapper.DomainObject;

import java.util.Queue;

public class CreateHelper extends AbstractCommitHelper {
    public CreateHelper(UnitOfWork unit, Queue<DomainObject> list) {
        super(unit, list);
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
