package inf226.inchat;
import inf226.util.immutable.List;
import inf226.util.Pair;

import inf226.storage.*;

/**
 * The Account class holds all information private to
 * a specific user.
 **/
public final class Account {
    /*
     * A channel consists of a User object of public account info,
     * and a list of channels which the user can post to.
     */
    public final Stored<User> user;
    public final List<Pair<String,Stored<Channel>>> channels;
    public final Password password;

    public Account(final Stored<User> user,
                   final List<Pair<String,Stored<Channel>>> channels,
                   final Password password) {
        this.user = user;
        this.channels = channels;
        this.password = password;    
    }

    
    /**
     * Create a new Account.
     *
     * @param user The public User profile for this user.
     * @param password The login password for this account.
     **/
    public static Account create(final Stored<User> user, final Password password) {
        return new Account(user,List.empty(), password);
    }
    
    /**
     * Join a channel with this account.
     *
     * @return A new account object with the channel added.
     */
    public Account joinChannel(final String alias, final Stored<Channel> channel) {
        Pair<String,Stored<Channel>> entry
            = new Pair<String,Stored<Channel>>(alias,channel);
        return new Account(user, List.cons(entry,channels), password);
    }


    /**
     * Check weather if a string is a correct password for
     * this account.
     *
     * @return true if password matches.
     */
    public boolean checkPassword(String password) { 
        return this.password.checkPassword(password); 
    }
    
    
}
