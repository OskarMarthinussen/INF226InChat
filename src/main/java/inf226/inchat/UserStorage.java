package inf226.inchat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.UUID;

import inf226.storage.*;
import inf226.util.*;



/**
 * The UserStore stores User objects in a SQL database.
 */
public final class UserStorage
    implements Storage<User,SQLException> {
    
    final Connection connection;
    
    public UserStorage(Connection connection) 
      throws SQLException {
        this.connection = connection;
        connection.createStatement()
                .executeUpdate("CREATE TABLE IF NOT EXISTS User (id TEXT PRIMARY KEY, version TEXT, name TEXT, joined TEXT)");
    }
    
    @Override
    public Stored<User> save(User user)
      throws SQLException {
        final Stored<User> stored = new Stored<User>(user);
        final String insertUserQuery = "INSERT INTO User VALUES(?, ?, ?, ?)";
        PreparedStatement insertUser = connection.prepareStatement(insertUserQuery);
        insertUser.setObject(1, stored.identity);
        insertUser.setObject(2, stored.version);
        insertUser.setString(3, user.username.get());
        insertUser.setString(4, user.joined.toString());
        insertUser.executeUpdate();
        return stored;
    }
    
    @Override
    public synchronized Stored<User> update(Stored<User> user,
                                            User new_user)
        throws UpdatedException,
            DeletedException,
            SQLException {
        final Stored<User> current = get(user.identity);
        final Stored<User> updated = current.newVersion(new_user);
        if(current.version.equals(user.version)) {
            final String updateUserQuery = "UPDATE User SET (version,name,joined) =(?, ?, ?) WHERE id=?";
            PreparedStatement updateUser = connection.prepareStatement(updateUserQuery);
            updateUser.setObject(1, updated.version);
            updateUser.setString(2, new_user.username.get());
            updateUser.setObject(3, new_user.joined.toString());
            updateUser.setObject(4, updated.identity);
            updateUser.executeUpdate();
        } else {
            throw new UpdatedException(current);
        }
        return updated;
    }
   
    @Override
    public synchronized void delete(Stored<User> user)
       throws UpdatedException,
              DeletedException,
              SQLException {
        final Stored<User> current = get(user.identity);
        if(current.version.equals(user.version)) {
            final String deleteUserQuery = "DELETE FROM User WHERE id =?";
            PreparedStatement deleteUser = connection.prepareStatement(deleteUserQuery);
            deleteUser.setObject(1, user.identity);
            deleteUser.executeUpdate();
        } else {
        throw new UpdatedException(current);
        }
    }
    @Override
    public Stored<User> get(UUID id)
      throws DeletedException,
             SQLException {
            final String selectUserQuery = "SELECT version,name,joined FROM User WHERE id =?";
            PreparedStatement selectUser = connection.prepareStatement(selectUserQuery);
            selectUser.setString(1, id.toString());
            ResultSet selectUserResult = selectUser.executeQuery();
        if(selectUserResult.next()) {
            final UUID version = 
                UUID.fromString(selectUserResult.getString("version"));
            final UserName name = new UserName(selectUserResult.getString("name"));
            final Instant joined = Instant.parse(selectUserResult.getString("joined"));
            return (new Stored<User>
                        (new User(name,joined),id,version));
        } else {
            throw new DeletedException();
        }
    }
    
    /**
     * Look up a user by their username;
     **/
    public Maybe<Stored<User>> lookup(UserName name) {
        try {
            final String selectUserQuery = "SELECT id FROM User WHERE name = ?";
            PreparedStatement selectUser = connection.prepareStatement(selectUserQuery);
            selectUser.setString(1, name.get());
            ResultSet selectUserResult = selectUser.executeQuery();
            if(selectUserResult.next())
                return Maybe.just(get(UUID.fromString(selectUserResult.getString("id"))));
        } catch (Exception e1) {
            System.err.println(e1);
        }
        return Maybe.nothing();
    }
}


