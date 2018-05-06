/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author kaixuangao
 */
import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.rmi.AlreadyBoundException;
import java.util.*;



public class TcpServerThread extends Thread{
    private Socket clientSocket;
    private static BufferedReader sin;
    private static PrintWriter sout;

    public volatile static String[] flag = {"true"};

    public TcpServerThread(Socket s) throws InterruptedException,IOException, AlreadyBoundException {
        clientSocket = s;

        sin = new BufferedReader(new InputStreamReader(clientSocket
                .getInputStream()));
        sout = new PrintWriter(new BufferedWriter(new OutputStreamWriter(
                clientSocket.getOutputStream())), true);
        start();

    }
    

    public static void leaderCommit(String[] strings) {
        if (strings.length > 3 && strings[3].equals("put")) {
            sout.println(Main.kvstore.put(strings[4], strings[5]));
            //TODO test
            System.out.println("leader stored: " + Main.kvstore.get(strings[4]));
        }
        else if (strings.length > 3 && strings[3].equals("del"))
            sout.println(Main.kvstore.del(strings[4]));
        synchronized (flag) {
//            flag[0] = "false";
            flag.notify();
        }
    }

    public static void followerCommit(String[] strings) {
        if (strings.length > 3 && strings[3].equals("put"))
            Main.kvstore.put(strings[4], strings[5]);
        else if (strings.length > 3 && strings[3].equals("del"))
            Main.kvstore.del(strings[4]);
    }

    public void run() {
        try {
            // Use while loop to listen to incoming events
            while (true) {
                String str1 = sin.readLine();
                //TODO test
                System.out.println("Incoming logs: " + str1);
                if (str1 != null && !str1.isEmpty()) {
                    String[] strings = str1.split(" ");
//                System.out.println(strings.length);
//                System.out.println(Arrays.toString(strings));

                    if (strings.length == 0) {
                        sout.println("Please re-input your command!");
                        break;
                    }
                    else if (strings.length > 3 && (strings[3].equals("put") || strings[3].equals("del"))) {
                        //Redirect to leader
                        if (Main.serverRole.equals("leader")) {
                            //TODO: test
                            System.out.println("leader: "+ Leader.leaderId);
                            synchronized (flag) {
                                while (!flag[0].equals("false")) {
                                    Main.logs.add(new LogNode(Main.currentTerm, strings));
                                    int prevMatch = Leader.matchIndex.get(Leader.leaderId);
                                    int prevNext = Leader.nextIndex.get(Leader.leaderId);
                                    Leader.matchIndex.put(Leader.leaderId, prevMatch + 1);
                                    Leader.nextIndex.put(Leader.leaderId, prevNext + 1);
                                    //TODO: test
                                    System.out.println("Log added: "+Main.logs.size());
                                    try { flag.wait(); } catch (InterruptedException e) {e.printStackTrace();}
                                    break;
                                }
                            }
                            break;
                        }
                        else {
                            sout.println("leaderId " + Leader.leaderId);
                            break;
                        }
                    }
                    else if (strings.length > 3 && strings[3].equals("get")) {
                        sout.println(Main.kvstore.get(strings[4]));
                        break;
                    }
                    else if (strings.length > 3 && strings[3].equals("store")) {

                        for (String key: Main.kvstore.store().keySet()) {
                            sout.println(Main.kvstore.get(key));
                        }
                        break;
                    }
                    else if(strings.length > 3 && strings[3].equals("exit")) {
                        System.exit(0);
                        break;
                    }
                    else {
                        sout.println("Please re-input your command!");
                        break;
                    }
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

class TCP_Hashmap {

    Map<String, String> myHash = new HashMap<>();

    public boolean isLocked = false;

    synchronized String put (String key, String value) {
        myHash.put(key, value);
        return ("put key " + key);
    }

    String get (String key) {
        if (myHash.get(key) == null || myHash.get(key).isEmpty())
            return ("No such key value pair!");
        return ("get key = " + key + "; val = " + myHash.get(key));
    }

    synchronized String del (String key) {
        if (myHash.get(key) == null || myHash.get(key).isEmpty())
            return ("No such key value pair!");
        myHash.remove(key);
        return ("delete key " + key);
    }

    Map<String, String> store() {
        return myHash;
    }
}


class ServerClient{
    private BufferedReader in;
    private PrintWriter out;

    public ServerClient(String[] args) {
        try {
            InetAddress address = InetAddress.getByName(args[2]);
            Socket socket = new Socket(address, Integer.parseInt(args[3]));
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            this.out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void put(String serverId) {
        out.println(serverId);
    }

    public String[] store() throws IOException {
        out.println("store");
        return in.readLine().split(" ");
    }
}