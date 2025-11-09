package com.nt.LiquibaseRollbackV1.service;

import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.ResourceAccessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;

@Service
public class LiquibaseServiceV1 {

    private final DataSource dataSource;

    // path to master changelog (same as spring.liquibase.change-log)
//    @Value("${spring.liquibase.change-log:classpath:db/changelog/changelog-master.xml}")
//    private String changelogFile;

    private static final String changelogFile = "db/changelog/changelog-master.xml";

    public LiquibaseServiceV1(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Create a tag at current DB state (optional; you can also use <tagDatabase/> in changelog)
     */
    public void tagDatabase(String tag) throws LiquibaseException {
        try (Connection conn = dataSource.getConnection()) {
            Database database = DatabaseFactory.getInstance()
                    .findCorrectDatabaseImplementation(new JdbcConnection(conn));
            ResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor();
            try (Liquibase liquibase = new Liquibase(changelogFile, resourceAccessor, database)) {
                liquibase.tag(tag);
            } finally {

                database.close();
            }
        } catch (Exception e) {
            throw new LiquibaseException("Failed to tag database: " + e.getMessage(), e);
        }
    }

    /**
     * Rollback database to the given tag.
     * This will execute rollback logic for all changesets applied after the tag.
     */
    public void rollbackToTag(String tag) throws LiquibaseException {
        try (Connection conn = dataSource.getConnection()) {
            Database database = DatabaseFactory.getInstance()
                    .findCorrectDatabaseImplementation(new JdbcConnection(conn));
            ResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor();
            try (Liquibase liquibase = new Liquibase(changelogFile, resourceAccessor, database)) {
                // two common variants:
                // liquibase.rollback(tag, (String) null);
                // or liquibase.rollback(tag, "");
                liquibase.rollback(tag, (String) null);

            } finally {
                database.close();
            }
        } catch (Exception e) {
            throw new LiquibaseException("Rollback to tag failed: " + e.getMessage(), e);
        }
    }
}
