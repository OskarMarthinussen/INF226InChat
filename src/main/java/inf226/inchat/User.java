package inf226.inchat;
import java.time.Instant;

/**
 * The User class holds the public information
 * about a user.
 **/
public final class User {
    public final UserName username;
    public final Instant joined;

    public User(UserName name, Instant joined) {
        this.username = name;
        this.joined = joined;
    }
    
    /**
     * Create a new user.
     */
    public static User create(UserName name) {
        return new User(name, Instant.now());
    }
}

