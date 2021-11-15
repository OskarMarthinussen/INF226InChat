package inf226.inchat;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import com.lambdaworks.crypto.SCryptUtil;

final class Password {
    private final String passwordHash;
    private Boolean NISTapproved;

    /**
     * Create a new password. 
     * @param password String
     * @param hashed true if password should be hashed, false otherwise
     */
    public Password(String password, boolean hash) {
        controlPassword(password);
        if (hash) this.passwordHash = SCryptUtil.scrypt(password ,16384, 8, 1);
        else this.passwordHash = password; 
    }

    /**
     * Return password hash.
     */
    public String get() {
        return this.passwordHash;
    }

    /**
     * Check userinput with stored hash.
     */
    public boolean checkPassword(String password) {
        return SCryptUtil.check(password, passwordHash);
    }

    /**
     * Check if password follow NIST requirements.
     * Sets variable NISTapproved to true if it does, false otherwise.
     */
    public final void controlPassword(String password) {
        if(password.length() < 8) this.NISTapproved = false;
        else if (scanPassword(password)) this.NISTapproved = false;
        else this.NISTapproved = true;
    }

    /**
     * Scan if password can be fond in password dictonary.
     * @param password Input from user
     * @return True if password is in dictonary, false otherwise.
     */
    public boolean scanPassword(String password) {
        try {
            Scanner scan = new Scanner(new File("assets/rockyou.txt"));
            while (scan.hasNext()) {
                final String passwordFromFile = scan.nextLine().trim();
                if (password.equals(passwordFromFile)) return true;
            }
        } catch (FileNotFoundException e) {
            System.err.println(e);
        }
        return false;
    }

    /**
     * Return result form control NIST analysis.
     */
    public boolean checkNIST(){
        return this.NISTapproved;
    }
}
