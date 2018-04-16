package org.github.isel.rapper;

import org.github.isel.rapper.utils.UnitOfWork;

public interface DomainObject<K> {

    K getIdentityKey();
    long getVersion();

    /**
     * Always called when creating a new object to insert in the database
     */
    default void markNew() {
        UnitOfWork.getCurrent().registerNew(this);
    }

    /**
     * Always called before making any changes to the object and calling markDirty()
     */
    default void markToBeDirty(){
        UnitOfWork.getCurrent().registerClone(this);
    }

    /**
     * Always called when altering an object
     */
    default void markDirty() {
        UnitOfWork.getCurrent().registerDirty(this);
    }

    /**
     * Always called when removing an object
     */
    default void markRemoved() {
        UnitOfWork.getCurrent().registerRemoved(this);
    }
}
