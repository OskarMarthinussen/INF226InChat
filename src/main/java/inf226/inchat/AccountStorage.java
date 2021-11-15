package inf226.inchat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

import inf226.storage.*;

import inf226.util.immutable.List;
import inf226.util.*;

/**
 * This class stores accounts in the database.
 */
public final class AccountStorage
    implements Storage<Account,SQLException> {
    
    final Connection connection;
    final Storage<User,SQLException> userStore;
    final Storage<Channel,SQLException> channelStore;
   
    /**
     * Create a new account storage.
     *
     * @param  connection   The connection to the SQL database.
     * @param  userStore    The storage for User data.
     * @param  channelStore The storage for channels.
     */
    public AccountStorage(Connection connection,
                          Storage<User,SQLException> userStore,
                          Storage<Channel,SQLException> channelStore) 
      throws SQLException {
        this.connection = connection;
        this.userStore = userStore;
        this.channelStore = channelStore;
        
        connection.createStatement()
                .executeUpdate("CREATE TABLE IF NOT EXISTS Account (id TEXT PRIMARY KEY, version TEXT, user TEXT, password TEXT, FOREIGN KEY(user) REFERENCES User(id) ON DELETE CASCADE)");
        connection.createStatement()
                .executeUpdate("CREATE TABLE IF NOT EXISTS AccountChannel (account TEXT, channel TEXT, alias TEXT, ordinal INTEGER, PRIMARY KEY(account,channel), FOREIGN KEY(account) REFERENCES Account(id) ON DELETE CASCADE, FOREIGN KEY(channel) REFERENCES Channel(id) ON DELETE CASCADE)");
    }
    
    @Override
    public Stored<Account> save(Account account)
      throws SQLException {
        final Stored<Account> stored = new Stored<Account>(account);
  
        final String insertAccountQuery = "INSERT INTO Account VALUES(?, ?, ?, ?)";
        PreparedStatement insertAccount = connection.prepareStatement(insertAccountQuery);
        insertAccount.setObject(1, stored.identity);
        insertAccount.setObject(2, stored.version);
        insertAccount.setObject(3, account.user.identity);
        insertAccount.setString(4, account.password.get());
        insertAccount.executeUpdate();


        // Write the list of channels
        final Maybe.Builder<SQLException> exception = Maybe.builder();
        final Mutable<Integer> ordinal = new Mutable<Integer>(0);
        account.channels.forEach(element -> {
            try {
                String alias = element.first;
                Stored<Channel> channel = element.second;

                final String insertAccountChannelQuery = "INSERT INTO AccountChannel VALUES(?, ?, ?, ?)";
                PreparedStatement insertAccountChannel = connection.prepareStatement(insertAccountChannelQuery);
                insertAccountChannel.setObject(1, stored.identity);
                insertAccountChannel.setObject(2, channel.identity);
                insertAccountChannel.setString(3, alias);
                insertAccountChannel.setString(4, ordinal.get().toString());
                insertAccountChannel.executeUpdate();    
            } catch (SQLException e1) {
                System.err.println(e1);
            }            

            ordinal.accept(ordinal.get() + 1);
        });

        Util.throwMaybe(exception.getMaybe());
        return stored;
    }
    
    @Override
    public synchronized Stored<Account> update(Stored<Account> account,
                                            Account new_account)
        throws UpdatedException,
            DeletedException,
            SQLException {
    final Stored<Account> current = get(account.identity);
    final Stored<Account> updated = current.newVersion(new_account);
    if(current.version.equals(account.version)) {

        final String updateAccountQuery = "UPDATE Account SET (version, user) =(?, ?) WHERE id= ?";
        PreparedStatement updateAccount = connection.prepareStatement(updateAccountQuery);
        updateAccount.setObject(1, updated.version);
        updateAccount.setObject(2, new_account.user.identity);
        updateAccount.setObject(3, updated.identity);
        updateAccount.executeUpdate();

        
        // Rewrite the list of channels
        final String deleteAccountChannelQuery = "DELETE FROM AccountChannel WHERE account=?";
        PreparedStatement deleteAccount = connection.prepareStatement(deleteAccountChannelQuery);
        deleteAccount.setObject(1, account.identity);
        deleteAccount.executeUpdate();
        

        final Maybe.Builder<SQLException> exception = Maybe.builder();
        final Mutable<Integer> ordinal = new Mutable<Integer>(0);
        new_account.channels.forEach(element -> {
            try {
                String alias = element.first;
                Stored<Channel> channel = element.second;
                final String insertAccountChannelQuery = "INSERT INTO AccountChannel VALUES(?, ?, ?, ?)";
                PreparedStatement insertAccountChannel = connection.prepareStatement(insertAccountChannelQuery);
                insertAccountChannel.setObject(1, account.identity);
                insertAccountChannel.setObject(2, channel.identity);
                insertAccountChannel.setString(3, alias);
                insertAccountChannel.setString(4, ordinal.get().toString());
                insertAccountChannel.executeUpdate();    
            } catch (SQLException e1) {
                System.err.println(e1);
            }

            ordinal.accept(ordinal.get() + 1);
        });

        Util.throwMaybe(exception.getMaybe());
    } else {
        throw new UpdatedException(current);
    }
    return updated;
    }
   
    @Override
    public synchronized void delete(Stored<Account> account)
       throws UpdatedException,
              DeletedException,
              SQLException {
        final Stored<Account> current = get(account.identity);
        if(current.version.equals(account.version)) {
            final String deleteAccountQuery = "DELETE FROM Account WHERE id =?";
            PreparedStatement deleteAccount = connection.prepareStatement(deleteAccountQuery);
            deleteAccount.setObject(1, account.identity);
            deleteAccount.executeUpdate();
            //String sql =  "DELETE FROM Account WHERE id ='" + account.identity + "'";
            //connection.createStatement().executeUpdate(sql);
        } else {
        throw new UpdatedException(current);
        }
    }
    @Override
    public Stored<Account> get(UUID id)
      throws DeletedException,
             SQLException {

                 final String selectChannelQuery = "SELECT channel,alias,ordinal FROM AccountChannel WHERE account = ? ORDER BY ordinal DESC";
        final String selectAccountQuery = "SELECT version,user,password FROM Account WHERE id =?";
        
        PreparedStatement selectChannel = connection.prepareStatement(selectChannelQuery);
        PreparedStatement selectAccount = connection.prepareStatement(selectAccountQuery);
        
        selectChannel.setString(1, id.toString());
        selectAccount.setString(1, id.toString());
        
        ResultSet selectChannelResult = selectChannel.executeQuery();
        ResultSet selectAccountResult = selectAccount.executeQuery();

        if(selectAccountResult.next()) {
            final UUID version = UUID.fromString(selectAccountResult.getString("version"));
            final UUID userid =
            UUID.fromString(selectAccountResult.getString("user"));
            final Password password = new Password(selectAccountResult.getString("password"), false);
            
            final Stored<User> user = userStore.get(userid);
            // Get all the channels associated with this account
            final List.Builder<Pair<String,Stored<Channel>>> channels = List.builder();
            while(selectChannelResult.next()) {
                final UUID channelId = 
                    UUID.fromString(selectChannelResult.getString("channel"));
                final String alias = selectChannelResult.getString("alias");
                channels.accept(
                    new Pair<String,Stored<Channel>>(
                        alias,channelStore.get(channelId)));
            }
            return (new Stored<Account>(new Account(user,channels.getList(),password),id,version));
        } else {
            throw new DeletedException();
        }
    }
    
    /**
     * Look up an account based on their username.
     */
    public Stored<Account> lookup(UserName username)
      throws DeletedException,
             SQLException {
        final String selectAccountQuery = "SELECT Account.id from Account INNER JOIN User ON user=User.id where User.name=?";
        PreparedStatement selectAccount = connection.prepareStatement(selectAccountQuery);
        selectAccount.setString(1, username.get());
        ResultSet selectAccountResult = selectAccount.executeQuery();
        System.err.println(selectAccountQuery);

        if(selectAccountResult.next()) {
            final UUID identity = 
                    UUID.fromString(selectAccountResult.getString("id"));
            return get(identity);
        }
        throw new DeletedException();
    }
    
} 
 
