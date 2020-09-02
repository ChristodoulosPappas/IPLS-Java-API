//package io.ipfs.api;

import io.ipfs.api.cbor.*;
import io.ipfs.cid.*;
import io.ipfs.multihash.Multihash;
import io.ipfs.multiaddr.MultiAddress;

/*
class FileH{
    public static void FileH(){
        System.out.println("ok");
    }
    public  void  test() throws IOException {
        char[] data = new char[2];
        File fd = new  File("File.txt");
        FileWriter writter =  new FileWriter(fd,true);
        FileReader reader = new FileReader(fd);
        if(fd.createNewFile()){
            System.out.println("Created Succesfully");
        }
        writter.write("Hello world");
        writter.flush();
        reader.skip(2);
        reader.read(data,1,1);
        System.out.println(data);
        reader.read(data,1,1);
        System.out.println(data);


    }
}

class dataset implements java.io.Serializable {
    int num1;
    int num2;
    public void dataset(int num1,int num2){
        this.num1 = num1;
        this.num2 = num2;
    }
    public int add(){
        return  num1 + num2;
    }
}
*/
/*
class FileH{
    public static void main(String[] args) throws IOException {
        String s = "1d";
        int a = 1;
        int b = 2;
        ByteBuffer buff = ByteBuffer.allocate(10);
        buff.putInt(a);
        buff.putInt(b);
        buff.put(s.getBytes());
        System.out.println(buff.toString());
        FileOutputStream out = new FileOutputStream("Test");
        FileInputStream in = new FileInputStream("Test");
        byte [] writeData = buff.array();
        out.write(writeData);
        out.close();
        byte[] instream = new byte[10];
        in.read(instream);
        in.close();
        System.out.println(instream);
        ByteBuffer nbuff = ByteBuffer.wrap(instream);

        int na,nb;
        byte[] nstr = new byte[2];
        na = nbuff.getInt();
        nb = nbuff.getInt();
        nbuff.get(nstr);
        System.out.println(na);
        System.out.println(nb);
        System.out.println(new String(nstr));

    }

}
*/