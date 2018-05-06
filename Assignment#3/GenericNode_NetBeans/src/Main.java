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
import java.net.*;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.util.*;

class ServerNode {
    public String ip;

    public ServerNode(String ip) {
        this.ip = ip;
    }
}

class LogNode implements Serializable {
    public int term;
    public String[] log;

    public LogNode(int term, String[] log) {
        this.term = term;
        this.log = log;
    }
}

public class Main {
    public volatile static int currentTerm = 0;
    public volatile static String voteFor = "";
    public volatile static ArrayList<LogNode> logs = new ArrayList<>();
    public volatile static ArrayList<ServerNode> servers = new ArrayList<>();
    public volatile static TCP_Hashmap kvstore = new TCP_Hashmap();

    public volatile static int commitIndex = 0;
    public volatile static int lastApplied = 0;
    public volatile static String serverRole = "follower";
    private static Follower follower;
    
    public static final int heartbeatInterval = 300;
    public static final int maxELectionTimeout = 10000;
    public static final int minELectionTimeout = 3000;

    private static int serverNum = 0;

    public static void main(String[] args) {
        
        if (args.length == 5 && args[0].equals("ts"))
            serverNum = Integer.parseInt(args[4]);

        if (args[0].equals("ts")) {
            ts(args);
        } else if (args[0].equals("tc")) {
            tc(args);
        }
    }


    static void ts(String[] args) {
        int portNo = Integer.parseInt(args[1]);

        if (args.length == 2) {

            // Start Centralized membership Server
            try {
                new CentralizedServer(portNo);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {

            // Start TCP kv-store Server
            try {
                ServerSocket serverSocket = new ServerSocket(portNo);
                System.out.println("The replicated kv-store server is started: " + serverSocket);

                // TCP centralized membership server periodic refresh
                // A kv-store server publishes its IP and port to centralized server per second
                String serverId = InetAddress.getLocalHost().toString().split("/")[1];

                // SC is the client side of the kv-store server to publish its ip tp node directory
                new Timer().schedule(new TimerTask() {
                    @Override
                    public void run() {
                        new ServerClient(args).put(serverId);
                    }
                },0,1000);

                //Get all servers' IP addresses and store in servers arrayList.
                String[] serverIds =  new ServerClient(args).store();

                // Block until all servers come online
                while (serverIds.length != serverNum) {
                    int max = 10000;
                    int min = 1000;
                    int randomTimeout = new Random().nextInt(max) % (max - min + 1) + min;
                    //TODO test
                    System.out.println(randomTimeout);
                    try { Thread.sleep(randomTimeout); } catch (InterruptedException e) {e.printStackTrace();}
                    serverIds = new ServerClient(args).store();
                }
                for (String ip: serverIds) {
                    servers.add(new ServerNode(ip));
                }
                //TODO test
                System.out.println(new Date());

                
                int max = 15000;
                int min = 1000;
                int randomTimeout = new Random().nextInt(max) % (max - min + 1) + min;
                System.out.println("Sleep for " + randomTimeout + "millis");
                try { Thread.sleep(randomTimeout); } catch (InterruptedException e) {e.printStackTrace();}

                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        // last log index = logs.size() - 1
                        logs.add(new LogNode(0,null));
                        // Listen to heartbeat
                        try {
                            //TODO test
                            System.out.println(new Date());
                            follower = new Follower();
                        } catch (RemoteException e) {e.printStackTrace();}
                          catch (AlreadyBoundException e) {e.printStackTrace();}
                          catch (MalformedURLException e) {e.printStackTrace();}
                        try {
                            waitForElection();
                        } catch (InterruptedException e) {e.printStackTrace();}
                          catch (UnknownHostException e) {e.printStackTrace();}
                    }
                }
                ).start();

                try {
                    while (true) {
                        Socket socket = serverSocket.accept();
                        try {
                            new TcpServerThread(socket);
                        } catch (InterruptedException e) {e.printStackTrace();}
                          catch (AlreadyBoundException e) {e.printStackTrace();}
                    }
                }
                finally {
                    serverSocket.close();
                    System.exit(0);
                }
            } catch (IOException e) { e.printStackTrace(); }
        }
    }

    public static void waitForElection() throws UnknownHostException, InterruptedException {
        follower.hasHeartbeat = true;
        follower.setTimer();

        // Wait till start election
        while(follower.hasHeartbeat) {
            System.out.println("Keep being follower");
            Thread.sleep(1000);
        }
        // Start election, transfer to candidate.
        //TODO test
        System.out.println("new candidate: "+new Date());
        Candidate candidate = new Candidate();
        if (candidate.leaderGranted) {
            new Leader();

            // Block till become follower
            while (serverRole.equals("leader")) {}
            if (serverRole.equals("follower")) {
                waitForElection();
            }
        }
        else if (candidate.electionCanceled)
            waitForElection();
    }

    static void tc(String[] args) {
        try {
            InetAddress address = InetAddress.getByName(args[1]);
            Socket socket = new Socket(address, Integer.parseInt(args[2]));
            String str = "";

            for(int i = 0; i < args.length; ++i) {
                str = str + args[i] + " ";
            }

            try {
                System.out.println("socket = " + socket);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
                out.println(str);

                while (true) {
                    String string = in.readLine();
                    if (string != null && !string.isEmpty()) {
                        System.out.println("Server response: " + string);
                        String[] strings = string.split(" ");
                        if (strings[0].equals("leaderId")) {
                            System.out.println("Redirect to leader: " + string);
                            redirectToLeader(strings[1], args);
                        }
                    }else break;
                }
            } finally {
                socket.close();
            }
        } catch (IOException var11) {
            var11.printStackTrace();
        }

    }

    static void redirectToLeader(String addr, String[] args) throws IOException {
        InetAddress address = InetAddress.getByName(addr);
        Socket socket = new Socket(address, Integer.parseInt(args[2]));
        String str = "";

        for(int i = 0; i < args.length; ++i) {
            str = str + args[i] + " ";
        }
        try {
            System.out.println("socket = " + socket);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
            out.println(str);

            while (true) {
                String string = in.readLine();
                if (string != null && !string.isEmpty()) {
                    System.out.println("Server response: " + string);
                    String[] strings = string.split(" ");
                    if (strings[0].equals("leaderId")) {
                        redirectToLeader(strings[1], args);
                    }
                }else break;
            }
        } finally {
            socket.close();
        }
    }
}
