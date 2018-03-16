package rapper;

import org.github.isel.rapper.DataMapper;
import org.junit.Assert;

public class QueryTests {
    @org.junit.Test
    public void shouldObtainQuerysForSimpleEntity(){
        DataMapper<Person, Integer> dataMapper = new DataMapper<>(Person.class, Integer.class);

        Assert.assertEquals("select nif, name, birthday from Person", dataMapper.getSelectQuery());
        Assert.assertEquals("delete from Person where nif = ?", dataMapper.getDeleteQuery());
        Assert.assertEquals("insert into Person ( nif, name, birthday ) values ( ?, ?, ? )", dataMapper.getInsertQuery());
        Assert.assertEquals("update Person set name = ? and birthday = ? where nif = ?", dataMapper.getUpdateQuery());
    }
}
