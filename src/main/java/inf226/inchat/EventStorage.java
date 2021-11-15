package inf226.inchat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.UUID;

import inf226.storage.*;




public final class EventStorage
    implements Storage<Channel.Event,SQLException> {
    
    private final Connection connection;
    
    public EventStorage(Connection connection) 
      throws SQLException {
        this.connection = connection;
        connection.createStatement()
                .executeUpdate("CREATE TABLE IF NOT EXISTS Event (id TEXT PRIMARY KEY, version TEXT, channel TEXT, type INTEGER, time TEXT, FOREIGN KEY(channel) REFERENCES Channel(id) ON DELETE CASCADE)");
        connection.createStatement()
                .executeUpdate("CREATE TABLE IF NOT EXISTS Message (id TEXT PRIMARY KEY, sender TEXT, content Text, FOREIGN KEY(id) REFERENCES Event(id) ON DELETE CASCADE)");
        connection.createStatement()
                .executeUpdate("CREATE TABLE IF NOT EXISTS Joined (id TEXT PRIMARY KEY, sender TEXT, FOREIGN KEY(id) REFERENCES Event(id) ON DELETE CASCADE)");
    }
    
    @Override
    public Stored<Channel.Event> save(Channel.Event event)
      throws SQLException {        
        final Stored<Channel.Event> stored = new Stored<Channel.Event>(event);
        
        final String insertEventQuery = "INSERT INTO Event VALUES(?, ?, ?, ?, ?)";
        PreparedStatement insertEvent = connection.prepareStatement(insertEventQuery);
        insertEvent.setObject(1, stored.identity);
        insertEvent.setObject(2, stored.version);
        insertEvent.setObject(3, event.channel);
        insertEvent.setObject(4, event.type.code);
        insertEvent.setObject(5, event.time);
        insertEvent.executeUpdate();
        
        /* String sql =  "INSERT INTO Event VALUES('" + stored.identity + "','"
                                                  + stored.version  + "','"
                                                  + event.channel + "','"
                                                  + event.type.code + "','"
                                                  + event.time  + "')";
        connection.createStatement().executeUpdate(sql); 
        */

        switch (event.type) {
            case message:
                final String insertMessageQuery = "INSERT INTO Message VALUES(?, ?, ?)";
                PreparedStatement insertMessage = connection.prepareStatement(insertMessageQuery);
                insertMessage.setObject(1, stored.identity);
                insertMessage.setString(2, event.sender);
                insertMessage.setString(3, event.message);
                insertMessage.executeUpdate();
                break;
            case join:
                final String insertJoinedQuery = "INSERT INTO Joined VALUES(?, ?)";  
                PreparedStatement insertJoined = connection.prepareStatement(insertJoinedQuery);
                insertJoined.setObject(1, stored.identity);
                insertJoined.setString(2, event.sender);
                insertJoined.executeUpdate();
                break;
        }
        return stored;
    }
    
    @Override
    public synchronized Stored<Channel.Event> update(Stored<Channel.Event> event,
                                            Channel.Event new_event)
        throws UpdatedException,
            DeletedException,
            SQLException {
    final Stored<Channel.Event> current = get(event.identity);
    final Stored<Channel.Event> updated = current.newVersion(new_event);
    if(current.version.equals(event.version)) {

        final String updateEventQuery = "UPDATE Event SET (version,channel,time,type) =( ?, ?, ?, ?) WHERE id=?";
        PreparedStatement updateEvent = connection.prepareStatement(updateEventQuery);
        updateEvent.setObject(1, updated.version);
        updateEvent.setObject(2, new_event.channel);
        updateEvent.setObject(3, new_event.time);
        updateEvent.setObject(4, new_event.type.code);
        updateEvent.setObject(5, updated.identity);
        updateEvent.executeUpdate();
        
        /* 
        String sql = "UPDATE Event SET" +
            " (version,channel,time,type) =('" 
                            + updated.version  + "','"
                            + new_event.channel  + "','"
                            + new_event.time  + "','"
                            + new_event.type.code
                            + "') WHERE id='"+ updated.identity + "'";
        connection.createStatement().executeUpdate(sql); 
        */
        switch (new_event.type) {
            case message:
                final String updateMessageQuery = "UPDATE Message SET (sender,content)=( ?, ?) WHERE id=?";
                PreparedStatement updateMessage = connection.prepareStatement(updateMessageQuery);
                updateMessage.setString(1, new_event.sender);
                updateMessage.setString(2, new_event.message);
                updateMessage.setObject(3, updated.identity);
                updateMessage.executeUpdate();
                /* 
                sql = "UPDATE Message SET (sender,content)=('" + new_event.sender + "','"
                                                     + new_event.message +"') WHERE id='"+ updated.identity + "'"; */
                break;
            case join:
                final String updateJoinedQuery = "UPDATE Joined SET (sender)=(?) WHERE id=?";
                PreparedStatement updateJoined = connection.prepareStatement(updateJoinedQuery);
                updateJoined.setString(1, new_event.sender);
                updateJoined.setObject(2, updated.identity);
                updateJoined.executeUpdate();
                //sql = "UPDATE Joined SET (sender)=('" + new_event.sender +"') WHERE id='"+ updated.identity + "'";
                break;
        }
        //connection.createStatement().executeUpdate(sql);
    } else {
        throw new UpdatedException(current);
    }
        return updated;
    }
   
    @Override
    public synchronized void delete(Stored<Channel.Event> event)
       throws UpdatedException,
              DeletedException,
              SQLException {
        final Stored<Channel.Event> current = get(event.identity);
        if(current.version.equals(event.version)) {
            final String deleteEventQuery = "DELETE FROM Event WHERE id =?";
            PreparedStatement deleteEvent = connection.prepareStatement(deleteEventQuery);
            deleteEvent.setObject(1, event.identity);
            deleteEvent.executeUpdate();

            //String sql =  "DELETE FROM Event WHERE id ='" + event.identity + "'";
            //connection.createStatement().executeUpdate(sql);
        } else {
        throw new UpdatedException(current);
        }
    }
    @Override
    public Stored<Channel.Event> get(UUID id)
      throws DeletedException,
             SQLException {
        final String selectEventQuery = "SELECT version,channel,time,type FROM Event WHERE id = ?";
        PreparedStatement selectEvent = connection.prepareStatement(selectEventQuery);
        selectEvent.setString(1, id.toString());
        ResultSet selectEventResult = selectEvent.executeQuery();


        //final String sql = "SELECT version,channel,time,type FROM Event WHERE id = '" + id.toString() + "'";
        //final Statement statement = connection.createStatement();
        //final ResultSet rs = statement.executeQuery(sql);

        if(selectEventResult.next()) {
            final UUID version = UUID.fromString(selectEventResult.getString("version"));
            final UUID channel = 
                UUID.fromString(selectEventResult.getString("channel"));
            final Channel.Event.Type type = 
                Channel.Event.Type.fromInteger(selectEventResult.getInt("type"));
            final Instant time = 
                Instant.parse(selectEventResult.getString("time"));
            
            final Statement mstatement = connection.createStatement();
            switch(type) {
                case message:
                    final String selectMessageQuery = "SELECT sender,content FROM Message WHERE id = ?";
                    PreparedStatement selectMessage = connection.prepareStatement(selectMessageQuery);
                    selectMessage.setString(1, id.toString());
                    ResultSet selectMessageResult = selectMessage.executeQuery();

                    //final String msql = "SELECT sender,content FROM Message WHERE id = '" + id.toString() + "'";
                    //final ResultSet mrs = mstatement.executeQuery(msql);
                    selectMessageResult.next();
                    return new Stored<Channel.Event>(
                            Channel.Event.createMessageEvent(channel,time,selectMessageResult.getString("sender"),selectMessageResult.getString("content")),
                            id,
                            version);
                case join:
                    final String selectJoinedQuery = "SELECT sender FROM Joined WHERE id = ?";
                    PreparedStatement selectJoined = connection.prepareStatement(selectJoinedQuery);
                    selectJoined.setString(1, id.toString());
                    ResultSet selectJoinedResult = selectJoined.executeQuery();


                    //final String asql = "SELECT sender FROM Joined WHERE id = '" + id.toString() + "'";
                    //final ResultSet ars = mstatement.executeQuery(asql);
                    selectJoinedResult.next();
                    return new Stored<Channel.Event>(
                            Channel.Event.createJoinEvent(channel,time,selectJoinedResult.getString("sender")),
                            id,
                            version);
            }
        }
        throw new DeletedException();
    }
    
}


 
