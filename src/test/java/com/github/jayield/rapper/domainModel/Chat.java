package com.github.jayield.rapper.domainModel;

import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.Id;

public class Chat implements DomainObject<Long> {
    @Id(isIdentity = true)
    private long chadId;
    private final long accountIdFirst;
    private final long accountIdSecond;
    private final long version;

    public Chat(long chatId, long accountIdFirst, long accountIdSecond, long version) {
        this.chadId = chatId;
        this.accountIdFirst = accountIdFirst;
        this.accountIdSecond = accountIdSecond;
        this.version = version;
    }

    public Chat(){
        chadId = 0;
        accountIdFirst = 0;
        accountIdSecond = 0;
        version = 0;
    }

    public long getChadId() {
        return chadId;
    }

    public long getAccountIdFirst() {
        return accountIdFirst;
    }

    public long getAccountIdSecond() {
        return accountIdSecond;
    }

    @Override
    public Long getIdentityKey() {
        return chadId;
    }

    @Override
    public long getVersion() {
        return version;
    }
}
