package com.nt.LiquibaseRollbackV1.service;

import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.ResourceAccessor;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Service
public class LiquibaseService {
    private static final Logger logger = LoggerFactory.getLogger(LiquibaseService.class);

    private final DataSource dataSource;
//    private final String changelogFile; // e.g. "db/changelog/changelog-master.xml"
    private static final String changelogFile = "db/changelog/changelog-master.xml";
    public LiquibaseService(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void rollbackToTag(String tag) throws LiquibaseException {
        if (tag == null || tag.isBlank()) {
            throw new LiquibaseException("Tag must be provided and not blank");
        }

        Connection conn = null;
        boolean originalAutoCommit = true;
        try {
            conn = dataSource.getConnection();
            originalAutoCommit = conn.getAutoCommit();
            // ensure we control transaction commit/rollback
            if (originalAutoCommit) {
                try {
                    conn.setAutoCommit(false);
                } catch (SQLException se) {
                    logger.warn("Could not set autoCommit to false; continuing. SQLException: {}", se.getMessage());
                }
            }

            Database database = DatabaseFactory.getInstance()
                    .findCorrectDatabaseImplementation(new JdbcConnection(conn));
            ResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor();

            try (Liquibase liquibase = new Liquibase(changelogFile, resourceAccessor, database)) {
                // Check that the requested tag exists before attempting rollback
                List<String> availableTags = getAvailableTags(conn);
                if (availableTags == null || !availableTags.contains(tag)) {
                    throw new LiquibaseException("Tag '" + tag + "' not found. Available tags: " + availableTags);
                }

                // Perform the rollback (explicit contexts/labels)
                liquibase.rollback(tag, new Contexts(), new LabelExpression());

                // commit the underlying JDBC connection if we're managing transactions
                try {
                    if (!conn.getAutoCommit()) {
                        conn.commit();
                    }
                } catch (SQLException commitEx) {
                    logger.warn("Failed to commit connection after rollback: {}", commitEx.getMessage(), commitEx);
                    // rethrow to let caller know rollback operation didn't complete cleanly
                    throw new LiquibaseException("Failed to commit after rollback: " + commitEx.getMessage(), commitEx);
                }

            } finally {
                // close/cleanup the Liquibase-created Database resources
                try {
                    database.close();
                } catch (Exception closeEx) {
                    logger.debug("Error closing Liquibase Database object: {}", closeEx.getMessage(), closeEx);
                }
            }

        } catch (LiquibaseException le) {
            // rethrow liquibase exceptions unchanged
            throw le;
        } catch (Exception e) {
            // attempt JDBC rollback if possible
            if (conn != null) {
                try {
                    if (!conn.getAutoCommit()) {
                        conn.rollback();
                    }
                } catch (SQLException rbEx) {
                    logger.warn("Failed to rollback JDBC connection after error: {}", rbEx.getMessage(), rbEx);
                }
            }
            logger.error("Rollback to tag '{}' failed: {}", tag, e.getMessage(), e);
            throw new LiquibaseException("Rollback to tag failed: " + e.getMessage(), e);
        } finally {
            // restore original autoCommit if possible
            if (conn != null) {
                try {
                    if (conn.isClosed()) {
                        logger.debug("Connection already closed; cannot restore autoCommit");
                    } else {
                        try {
                            conn.setAutoCommit(originalAutoCommit);
                        } catch (SQLException acEx) {
                            logger.warn("Failed to restore autoCommit to {}: {}", originalAutoCommit, acEx.getMessage(), acEx);
                        }
                    }
                } catch (SQLException isClosedEx) {
                    logger.debug("Could not determine connection closed state: {}", isClosedEx.getMessage());
                }

                try {
                    conn.close();
                } catch (SQLException closeConnEx) {
                    logger.debug("Error closing connection: {}", closeConnEx.getMessage(), closeConnEx);
                }
            }
        }
    }

    private List<String> getAvailableTags(Connection conn) throws SQLException {
        String sql = "SELECT TAG FROM DATABASECHANGELOG WHERE TAG IS NOT NULL";
        List<String> tags = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                tags.add(rs.getString("TAG"));
            }
        }
        return tags;
    }
}
