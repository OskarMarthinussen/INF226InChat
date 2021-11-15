package inf226.inchat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import java.util.TreeMap;
import java.util.Map;
import java.util.function.Consumer;

import inf226.storage.*;

import inf226.util.immutable.List;
import inf226.util.*;

/**
 * This class stores Channels in a SQL database.
 */
public final class ChannelStorage
    implements Storage<Channel,SQLException> {
    
    final Connection connection;
    /* The waiters object represent the callbacks to
     * make when the channel is updated.
     */
    private Map<UUID,List<Consumer<Stored<Channel>>>> waiters
        = new TreeMap<UUID,List<Consumer<Stored<Channel>>>>();
    public final EventStorage eventStore;
    
    public ChannelStorage(Connection connection) 
      throws SQLException {
        this.connection = connection;
        this.eventStore = new EventStorage(connection);
        
        connection.createStatement()
                .executeUpdate("CREATE TABLE IF NOT EXISTS Channel (id TEXT PRIMARY KEY, version TEXT, name TEXT)");
        connection.createStatement()
                .executeUpdate("CREATE TABLE IF NOT EXISTS ChannelMembers (channelID TEXT, account TEXT, permission TEXT, PRIMARY KEY(channelID, account, permission), FOREIGN KEY(channelID) REFERENCES Channel(id))");
    }
    
    @Override
    public Stored<Channel> save(Channel channel)
      throws SQLException {
        final Stored<Channel> stored = new Stored<Channel>(channel);
        System.err.println("ChannelStorage - save");
        final String insertChannelQuery = "INSERT INTO Channel VALUES(?, ?, ?)";
        PreparedStatement insertChannel = connection.prepareStatement(insertChannelQuery);
        insertChannel.setObject(1, stored.identity);
        insertChannel.setObject(2, stored.version);
        insertChannel.setString(3, channel.name);
        insertChannel.executeUpdate();
        System.err.println("ChannelStorage - save finsihed");

        return stored;
    }

    public void setPermission(Stored<Channel> channel, Stored<Account> account, String permission)
      throws SQLException {
        final String insertChannelMemberQuery = "INSERT INTO ChannelMembers VALUES(?, ?, ?)";
        PreparedStatement insertChannelMember = connection.prepareStatement(insertChannelMemberQuery);
        insertChannelMember.setObject(1, channel.identity);
        insertChannelMember.setObject(2, account.identity);
        insertChannelMember.setString(3, permission);
        insertChannelMember.executeUpdate();
    }

    public void changePermission(Stored<Channel> channel, Stored<Account> account, String permission)
    throws SQLException{
        final String updateChannelMembersQuery = "UPDATE ChannelMembers SET permission = ? WHERE channelID = ? AND account = ?";
        PreparedStatement updateChannelMembers = connection.prepareStatement(updateChannelMembersQuery);
        updateChannelMembers.setObject(1, permission);
        updateChannelMembers.setObject(2, channel.identity);
        updateChannelMembers.setObject(3, account.identity);
        updateChannelMembers.executeUpdate();
    }

    public String getPermission(Stored<Channel> channel, Stored<Account> account) throws SQLException, DeletedException {
        final String selectChannelMemeberQuery = "SELECT permission FROM ChannelMembers WHERE channelID = ? AND account = ?";
        PreparedStatement selectChannelMember = connection.prepareStatement(selectChannelMemeberQuery);
        selectChannelMember.setObject(1, channel.identity);
        selectChannelMember.setObject(1, account.identity);
        ResultSet selectChannelMemberResult = selectChannelMember.executeQuery();

        if(selectChannelMemberResult.next()) {
            final String permission = selectChannelMemberResult.getString("permission");
            return permission;
        } else {
            throw new DeletedException();
        }
    }

    @Override
    public synchronized Stored<Channel> update(Stored<Channel> channel,
                                            Channel new_channel)
        throws UpdatedException,
            DeletedException,
            SQLException {
        final Stored<Channel> current = get(channel.identity);
        final Stored<Channel> updated = current.newVersion(new_channel);
        if(current.version.equals(channel.version)) {

            final String updateChannelQuery = "UPDATE Channel SET (version,name) =(?, ?) WHERE id= ?";
            PreparedStatement updateChannel = connection.prepareStatement(updateChannelQuery);
            updateChannel.setObject(1, updated.version);
            updateChannel.setString(2, new_channel.name);
            updateChannel.setObject(3, updated.identity);
            updateChannel.executeUpdate();
        } else {
            throw new UpdatedException(current);
        }
        giveNextVersion(updated);
        return updated;
    }
   
    @Override
    public synchronized void delete(Stored<Channel> channel)
       throws UpdatedException,
              DeletedException,
              SQLException {
        final Stored<Channel> current = get(channel.identity);
        if(current.version.equals(channel.version)) {
        final String deleteChannelQuery =  "DELETE FROM Channel WHERE = ?";
        PreparedStatement deleteChannel = connection.prepareStatement(deleteChannelQuery);
        deleteChannel.setObject(1, channel.identity);
        deleteChannel.executeUpdate();
        
        } else {
        throw new UpdatedException(current);
        }
    }
    @Override
    public Stored<Channel> get(UUID id)
      throws DeletedException,
             SQLException {
        final String selectChannelQuery = "SELECT version,name FROM Channel WHERE id = ?";
        final String selectEventQuery = "SELECT id,rowid FROM Event WHERE channel = ? ORDER BY rowid ASC";

        PreparedStatement selectChannel = connection.prepareStatement(selectChannelQuery);
        PreparedStatement selectEvent = connection.prepareStatement(selectEventQuery);

        selectChannel.setString(1, id.toString());
        selectEvent.setString(1, id.toString());

        final ResultSet selectChannelResult = selectChannel.executeQuery();
        final ResultSet selectEventResult = selectEvent.executeQuery();

        if(selectChannelResult.next()) {
            final UUID version = 
                UUID.fromString(selectChannelResult.getString("version"));
            final String name =
            selectChannelResult.getString("name");
            // Get all the events associated with this channel
            final List.Builder<Stored<Channel.Event>> events = List.builder();
            while(selectEventResult.next()) {
                final UUID eventId = UUID.fromString(selectEventResult.getString("id"));
                events.accept(eventStore.get(eventId));
            }
            return (new Stored<Channel>(new Channel(name,events.getList()),id,version));
        } else {
            throw new DeletedException();
        }
    }
    
    /**
     * This function creates a "dummy" update.
     * This function should be called when events are changed or
     * deleted from the channel.
     */
    public Stored<Channel> noChangeUpdate(UUID channelId)
        throws SQLException, DeletedException {
        final String updateChannelQuery = "UPDATE Channel SET (version) =(?) WHERE id=?";
        PreparedStatement updateChannel = connection.prepareStatement(updateChannelQuery);
        updateChannel.setObject(1, UUID.randomUUID());
        updateChannel.setObject(2, channelId);
        updateChannel.executeUpdate();

  
        Stored<Channel> channel = get(channelId);
        giveNextVersion(channel);
        return channel;
    }
    
    /**
     * Get the current version UUID for the specified channel.
     * @param id UUID for the channel.
     */
    public UUID getCurrentVersion(UUID id)
      throws DeletedException,
             SQLException {
        final String selectChannelQuery = "SELECT version FROM Channel WHERE id = ?";
        PreparedStatement selectChannel = connection.prepareStatement(selectChannelQuery);
        selectChannel.setString(1, id.toString());
        final ResultSet selectChannelResult = selectChannel.executeQuery();

        if(selectChannelResult.next()) {
            return UUID.fromString(
                selectChannelResult.getString("version"));
        }
        throw new DeletedException();
    }
    
    /**
     * Wait for a new version of a channel.
     * This is a blocking call to get the next version of a channel.
     * @param identity The identity of the channel.
     * @param version  The previous version accessed.
     * @return The newest version after the specified one.
     */
    public Stored<Channel> waitNextVersion(UUID identity, UUID version)
      throws DeletedException,
             SQLException {
        var result
            = Maybe.<Stored<Channel>>builder();
        // Insert our result consumer
        synchronized(waiters) {
            var channelWaiters 
                = Maybe.just(waiters.get(identity));
            waiters.put(identity
                       ,List.cons(result
                                 ,channelWaiters.defaultValue(List.empty())));
        }
        // Test if there already is a new version avaiable
        if(!getCurrentVersion(identity).equals(version)) {
            return get(identity);
        }
        // Wait
        synchronized(result) {
            while(true) {
                try {
                    result.wait();
                    return result.getMaybe().get();
                } catch (InterruptedException e) {
                    System.err.println("Thread interrupted.");
                } catch (Maybe.NothingException e) {
                    // Still no result, looping
                }
            }
        }
    }
    
    /**
     * Notify all waiters of a new version
     */
    private void giveNextVersion(Stored<Channel> channel) {
        synchronized(waiters) {
            Maybe<List<Consumer<Stored<Channel>>>> channelWaiters 
                = Maybe.just(waiters.get(channel.identity));
            try {
                channelWaiters.get().forEach(w -> {
                    w.accept(channel);
                    synchronized(w) {
                        w.notifyAll();
                    }
                });
            } catch (Maybe.NothingException e) {
                // No were waiting for us :'(
            }
            waiters.put(channel.identity,List.empty());
        }
    }
    
    /**
     * Get the channel belonging to a specific event.
     */
    public Stored<Channel> lookupChannelForEvent(Stored<Channel.Event> e)
      throws SQLException, DeletedException {
        final String selectChannelQuery = "SELECT channel FROM ChannelEvent WHERE event=?";
        PreparedStatement selectChannel = connection.prepareStatement(selectChannelQuery);
        selectChannel.setObject(1, e.identity);
        final ResultSet selectChannelResult = selectChannel.executeQuery();

        if(selectChannelResult.next()) {
            
            final UUID channelId = UUID.fromString(selectChannelResult.getString("channel"));
            return get(channelId);
        }
        throw new DeletedException();
    }
} 
 
 
