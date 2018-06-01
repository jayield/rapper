package com.github.jayield.rapper;

import com.github.jayield.rapper.utils.ConnectionManager;
import com.github.jayield.rapper.utils.DBsPath;
import com.github.jayield.rapper.utils.SqlSupplier;
import com.github.jayield.rapper.utils.UnitOfWork;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;

import static com.github.jayield.rapper.utils.ConnectionManager.getConnectionManager;

/**
 * Gives a way to do a transaction
 * When everything is ready to go, commit() must be called
 */
public class Transaction {

    private final CustomUnitOfWork customUnitOfWork;
    private Queue<Supplier<CompletableFuture>> actionsQueue = new ConcurrentLinkedQueue<>();

    public Transaction(int transactionLevel) {
        customUnitOfWork = new CustomUnitOfWork(transactionLevel);
    }

    /**
     * Adds a new action to the queue
     * @param action action to be queued
     * @return Transaction
     */
    public Transaction andDo(Supplier<CompletableFuture> action) {
        actionsQueue.add(() -> {
            UnitOfWork.setCurrent(customUnitOfWork);
            return action.get();
        });
        return this;
    }

    /**
     * It will try to commit, if anything goes wrong, rollback is executed
     * @return a CompletableFuture<Optional<Throwable>> representing the end of the commit. If an exception was caught, the optional will contain it
     */
    public CompletableFuture<Void> commit() {
        if (!actionsQueue.isEmpty()) {
            Supplier<CompletableFuture> action = actionsQueue.remove();
            return action.get().thenCompose(o -> commit());
        }
        return customUnitOfWork.commitTransaction();
    }

    private static class CustomUnitOfWork extends UnitOfWork {

        private static Supplier<Connection> getConnectionSupplier(int transactionLevel) {
            ConnectionManager connectionManager = getConnectionManager(DBsPath.DEFAULTDB);
            SqlSupplier<Connection> connectionSupplier = () -> {
                Connection connection = connectionManager.getConnection();
                connection.setTransactionIsolation(transactionLevel);
                return connection;
            };
            return connectionSupplier.wrap();
        }

        private CustomUnitOfWork(int transactionLevel) {
            super(getConnectionSupplier(transactionLevel));
        }

        @Override
        public CompletableFuture<Void> commit() {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> commitTransaction() {
            return super.commit();
        }
    }
}
