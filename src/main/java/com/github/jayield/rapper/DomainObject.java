package com.github.jayield.rapper;

public interface DomainObject<K> {
    K getIdentityKey();
    long getVersion();
}
