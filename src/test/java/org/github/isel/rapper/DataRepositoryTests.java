package org.github.isel.rapper;

import org.github.isel.rapper.domainModel.TopStudent;
import org.github.isel.rapper.utils.ConnectionManager;
import org.github.isel.rapper.utils.MapperRegistry;
import org.github.isel.rapper.utils.UnitOfWork;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.github.isel.rapper.AssertUtils.assertMultipleRows;
import static org.github.isel.rapper.AssertUtils.assertSingleRow;
import static org.github.isel.rapper.TestUtils.getTopStudentPSConsumer;
import static org.github.isel.rapper.TestUtils.topStudentSelectQuery;
import static org.github.isel.rapper.utils.DBsPath.TESTDB;
import static org.junit.Assert.assertEquals;

public class DataRepositoryTests {

    @Before
    public void before() throws SQLException {
        ConnectionManager manager = ConnectionManager.getConnectionManager(TESTDB);
        Connection con = manager.getConnection();
        con.prepareCall("{call deleteDB}").execute();
        con.prepareCall("{call populateDB}").execute();
        con.commit();
        con.close();
    }

    @Test
    public void findById() {
        DataMapper<TopStudent, Integer> dataMapper = new DataMapper<>(TopStudent.class);
        Mapperify<TopStudent, Integer> mapperify = new Mapperify<>(dataMapper);
        DataRepository<TopStudent, Integer> repository = new DataRepository<>(mapperify);

        Optional<TopStudent> first = repository.findById(454).join();
        assertEquals(1, mapperify.getIfindById().getCount());

        Optional<TopStudent> second = repository.findById(454).join();
        assertEquals(1, mapperify.getIfindById().getCount());

        assertEquals(first.get(), second.get());

        assertSingleRow(UnitOfWork.getCurrent(), second.get(), topStudentSelectQuery, getTopStudentPSConsumer(second.get().getNif()), AssertUtils::assertTopStudent);
    }

    @Test
    public void findAll() {
        DataMapper<TopStudent, Integer> dataMapper = new DataMapper<>(TopStudent.class);
        Mapperify<TopStudent, Integer> mapperify = new Mapperify<>(dataMapper);
        DataRepository<TopStudent, Integer> repository = new DataRepository<>(mapperify);

        List<TopStudent> first = repository.findAll().join();
        assertEquals(1, mapperify.getIfindAll().getCount());

        List<TopStudent> second = repository.findAll().join();
        assertEquals(2, mapperify.getIfindAll().getCount());

        assertMultipleRows(UnitOfWork.getCurrent(), second, topStudentSelectQuery.substring(0, topStudentSelectQuery.length()-16), AssertUtils::assertTopStudent, second.size());
    }

    @Test
    public void create() throws NoSuchFieldException, IllegalAccessException {
        DataMapper<TopStudent, Integer> dataMapper = new DataMapper<>(TopStudent.class);
        Mapperify<TopStudent, Integer> mapperify = new Mapperify<>(dataMapper);
        DataRepository<TopStudent, Integer> repository = new DataRepository<>(mapperify);

        Field repositoryMap = MapperRegistry.class.getDeclaredField("repositoryMap");
        repositoryMap.setAccessible(true);
        Map<Class, DataRepository> repositoryMap1 = (Map<Class, DataRepository>) repositoryMap.get(new MapperRegistry());
        repositoryMap1.put(TopStudent.class, repository);

        TopStudent topStudent = new TopStudent(456, "Manel", new Date(2020, 12, 1), 0, 1, 20, 2016, 0, 0);

        Boolean success = repository.create(topStudent).join();

        assertEquals(true, success);

        Optional<TopStudent> first = repository.findById(456).join();
        assertEquals(0, mapperify.getIfindById().getCount());

        assertEquals(first.get(), topStudent);
    }

    @Test
    public void createAll() {
    }

    @Test
    public void update() {
    }

    @Test
    public void updateAll() {
    }

    @Test
    public void deleteById() {
    }

    @Test
    public void delete() {
    }

    @Test
    public void deleteAll() {
    }
}