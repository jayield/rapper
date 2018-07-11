package com.github.jayield.rapper.utils.helpers;

import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.utils.UnitOfWork;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

public abstract class AbstractCommitHelper {
    protected final Queue<DomainObject> list;
    protected Iterator<DomainObject> objectIterator;

    protected AbstractCommitHelper(Queue<DomainObject> list) {
        this.list = list;
    }

    public void reset() {
        objectIterator = list.iterator();
    }

    /**
     * It will get the next Object from the list and call the DataMapper methods
     * @param unit
     * @return
     */
    public abstract CompletableFuture<Void> commitNext(UnitOfWork unit);

    /**
     * It will get the next Object from the list and update the Identity Map
     * @return
     */
    public abstract Object identityMapUpdateNext();
    public abstract Object rollbackNext();
}
