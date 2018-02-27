package liquibase.dbtest.sybase;

import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.dbtest.AbstractIntegrationTest;
import liquibase.exception.DatabaseException;
import liquibase.exception.ValidationFailedException;
import liquibase.snapshot.SnapshotGeneratorFactory;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;

public class MySybIntegrationTest extends AbstractIntegrationTest {

    private final String changeSpecifyDbmsChangeLog;


    public MySybIntegrationTest() throws Exception {
//        super("sybase", "jdbc:jtds:sybase://ionc-dev.aimspecialtyhealth.com:8310;databaseName=srx");
        super("sybase", "jdbc:sybase:Tds:ionc-dev.aimspecialtyhealth.com:8310/srx?SQLINITSTRING=set quoted_identifier off");
        this.changeSpecifyDbmsChangeLog = "changelogs/sybase/complete/cl.xml";
    }

    @Test
    public void test1() throws Exception{
        if (getDatabase() == null) {
            return;
        }

        Liquibase liquibase = createLiquibase(changeSpecifyDbmsChangeLog);
        clearDb(liquibase, getDatabase());

        //run again to test changelog testing logic
        liquibase = createLiquibase(changeSpecifyDbmsChangeLog);
        try {
            liquibase.update(this.contexts);
        } catch (ValidationFailedException e) {
            e.printDescriptiveError(System.out);
            throw e;
        }
       clearDb(liquibase, getDatabase());
    }

    private void clearDb(Liquibase liquibase, Database database) throws DatabaseException {
        try {
            Statement statement = null;
            try {
                statement = ((JdbcConnection) database.getConnection()).getUnderlyingConnection().createStatement();
                statement.execute("drop table " + database.escapeTableName(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), database.getDatabaseChangeLogTableName()));
                database.commit();
            } catch (Exception e) {
                System.out.println("Probably expected error dropping databasechangelog table");
                e.printStackTrace();
                database.rollback();
            } finally {
                if (statement != null) {
                    statement.close();
                }
            }
            try {
                statement = ((JdbcConnection) database.getConnection()).getUnderlyingConnection().createStatement();
                statement.execute("drop table " + database.escapeTableName(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), database.getDatabaseChangeLogLockTableName()));
                database.commit();
            } catch (Exception e) {
                System.out.println("Probably expected error dropping databasechangeloglock table");
                e.printStackTrace();
                database.rollback();
            } finally {
                if (statement != null) {
                    statement.close();
                }
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        SnapshotGeneratorFactory.resetAll();
        DatabaseFactory.reset();
    }

    @Override
    public void tearDown() {

    }

    @Override
    public void setUp() throws Exception {
        super.setUp();


//        try {
//            // Create schemas for tests testRerunDiffChangeLogAltSchema
//            ((JdbcConnection) getDatabase().getConnection()).getUnderlyingConnection().createStatement().executeUpdate(
//                    "CREATE SCHEMA LBCAT2"
//            );
//        } catch (SQLException e) {
//            if (e.getSQLState().equals(H2_SQLSTATE_OBJECT_ALREADY_EXISTS)) {
//                // do nothing
//            } else {
//                throw e;
//            }
//        }
//
//        getDatabase().commit();

    }
}
