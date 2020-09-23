//import blockchainAPI.Storage;
import org.web3j.crypto.Credentials;
import org.web3j.crypto.ECKeyPair;
import org.web3j.crypto.Keys;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.methods.response.Web3ClientVersion;
import org.web3j.protocol.http.HttpService;

import java.io.IOException;
import java.math.BigInteger;
import java.security.InvalidAlgorithmParameterException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.UUID;

public class Web3jTest {
    public static BigInteger GAS_PRICE = new BigInteger(String.valueOf(4000000));
    public static BigInteger GAS_LIMIT = new BigInteger(String.valueOf(3000000));

    public static String address = "0x78e25714b870001bd79a768bb28e50fc41533733";

    public static void main(String[] args) throws Exception {
        String seed = UUID.randomUUID().toString();
        ECKeyPair exKey = Keys.createEcKeyPair();
        Web3j web = Web3j.build(new HttpService("https://ropsten.infura.io/v3/65313644b3c24f47b6e8b15abe8d5175"));
        Web3ClientVersion version = null;
        /*
        try {
            version = web.web3ClientVersion().send();
        }
        catch(IOException e){
            e.printStackTrace();
        }
        System.out.println("ok");
        String vstring = version.getWeb3ClientVersion();

        Credentials key = Credentials.create("5fc77030995574256452a4678f43dd6a85e9c69ecc72ffb7d6c75936c857abb5");
        //Storage storage = new Storage(address,web,key,GAS_PRICE,GAS_LIMIT);
        Storage Dir = Storage.load(address,web,key,GAS_PRICE,GAS_LIMIT);
        //Dir.store("12","312").send();
        System.out.println(Dir.retrieveHash("12").send());
        //Dir.store("Pas","212").send();
        //System.out.println(Dir.retrieveHash("Pas").encodeFunctionCall());
        //System.out.println("End");
        //Storage contract =  Storage.load();

         */
    }

}

