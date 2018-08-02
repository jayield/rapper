package com.github.jayield.rapper.unitofwork;

import com.github.jayield.rapper.DomainObject;

import java.util.Iterator;
import java.util.Queue;

public abstract class AbstractCommitHelper {
    protected final Queue<DomainObject> list;
    protected Iterator<DomainObject> objectIterator;
    protected final UnitOfWork unit;

    protected AbstractCommitHelper(UnitOfWork unit, Queue<DomainObject> list) {
        this.list = list;
        this.unit = unit;
    }

    public void reset() {
        objectIterator = list.iterator();
    }

    /**
     * It will get the next Object from the list and update the Identity Map
     * @return
     */
    public abstract Object identityMapUpdateNext();
    public abstract Object rollbackNext();
}
