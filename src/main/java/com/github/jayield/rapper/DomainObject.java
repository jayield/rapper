package com.github.jayield.rapper;

import com.github.jayield.rapper.utils.UnitOfWork;

public interface DomainObject<K> {

    K getIdentityKey();
    long getVersion();

    /**
     * Always called when creating a new object to insert in the database
     */
    default void markNew(UnitOfWork unit) {
        unit.registerNew(this);
    }

    /**
     * Always called before making any changes to the object and calling markDirty()
     */
    default void markToBeDirty(UnitOfWork unit){
        unit.registerClone(this);
    }

    /**
     * Always called when altering an object
     */
    default void markDirty(UnitOfWork unit) {
        unit.registerDirty(this);
    }

    /**
     * Always called when removing an object
     */
    default void markRemoved(UnitOfWork unit) {
        unit.registerRemoved(this);
    }
}
