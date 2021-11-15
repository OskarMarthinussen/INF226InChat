package inf226.inchat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.UUID;

import inf226.storage.*;

/**
 * The SessionStorage stores Session objects in a SQL database.
 */
public final class SessionStorage
    implements Storage<Session,SQLException> {
    
    final Connection connection;
    final Storage<Account,SQLException> accountStorage;
    
    public SessionStorage(Connection connection,
                          Storage<Account,SQLException> accountStorage)
      throws SQLException {
        this.connection = connection;
        this.accountStorage = accountStorage;
        connection.createStatement()
                .executeUpdate("CREATE TABLE IF NOT EXISTS Session (id TEXT PRIMARY KEY, version TEXT, account TEXT, expiry TEXT, FOREIGN KEY(account) REFERENCES Account(id) ON DELETE CASCADE)");
    }
    
    @Override
    public Stored<Session> save(Session session)
      throws SQLException {
        System.err.println("Prepared Statement - SessionStorage: save");
        final Stored<Session> stored = new Stored<Session>(session);
        final String insertSessionQuery = "INSERT INTO Session VALUES(?, ?, ?, ?)";
        PreparedStatement insertSession = connection.prepareStatement(insertSessionQuery);
        insertSession.setObject(1, stored.identity);
        insertSession.setObject(2, stored.version);
        insertSession.setObject(3, session.account.identity);
        insertSession.setString(4, session.expiry.toString());
        insertSession.executeUpdate();
        return stored;
    }
    
    @Override
    public synchronized Stored<Session> update(Stored<Session> session,
                                            Session new_session)
        throws UpdatedException,
            DeletedException,
            SQLException {
                System.err.println("Prepared Statement - SessionStorage: update");
    final Stored<Session> current = get(session.identity);
    final Stored<Session> updated = current.newVersion(new_session);
    if(current.version.equals(session.version)) {
        final String updateSessionQuery = "UPDATE Session SET (version,account,expiry) =(?, ?, ?) WHERE id=?"; 
        PreparedStatement updateSession = connection.prepareStatement(updateSessionQuery);
        updateSession.setObject(1, updated.version);
        updateSession.setObject(2, new_session.account.identity);
        updateSession.setString(3, new_session.expiry.toString());
        updateSession.setObject(4, updated.identity);
        updateSession.executeUpdate();
    } else {
        throw new UpdatedException(current);
    }
    return updated;
    }
   
    @Override
    public synchronized void delete(Stored<Session> session)
       throws UpdatedException,
              DeletedException,
              SQLException {
                System.err.println("Prepared Statement - SessionStorage: delete");
        final Stored<Session> current = get(session.identity);
        if(current.version.equals(session.version)) {
            final String deleteSessionQuery = "DELETE FROM Session WHERE id =?";
            PreparedStatement deleteSession = connection.prepareStatement(deleteSessionQuery);
            deleteSession.setObject(1, session.identity);
            deleteSession.executeUpdate();
        } else {
        throw new UpdatedException(current);
        }
    }
    @Override
    public Stored<Session> get(UUID id)
      throws DeletedException,
             SQLException {
                System.err.println("Prepared Statement - SessionStorage: get");
        final String selectSessionQuery = "SELECT version,account,expiry FROM Session WHERE id =?";
        PreparedStatement selectSession = connection.prepareStatement(selectSessionQuery);
        selectSession.setString(1, id.toString());
        ResultSet selectSessionResult = selectSession.executeQuery();

        if(selectSessionResult.next()) {
            final UUID version = UUID.fromString(selectSessionResult.getString("version"));
            final Stored<Account> account
               = accountStorage.get(
                    UUID.fromString(selectSessionResult.getString("account")));
            final Instant expiry = Instant.parse(selectSessionResult.getString("expiry"));
            return (new Stored<Session>
                        (new Session(account,expiry),id,version));
        } else {
            throw new DeletedException();
        }
    }
    
    
} 
