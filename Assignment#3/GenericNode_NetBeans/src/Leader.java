/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author kaixuangao
 */
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Leader {
    public static volatile HashMap<String, Integer> nextIndex = new HashMap<>();
    public static volatile HashMap<String, Integer> matchIndex = new HashMap<>();
    public static String leaderId;

    public Leader() throws UnknownHostException, InterruptedException{
        Main.serverRole = "leader";
        int serverNum = Main.servers.size();
        ExecutorService executor = Executors.newFixedThreadPool(serverNum);

        for (int i = 0; i < serverNum; i++) {
            String ip = Main.servers.get(i).ip;
            nextIndex.put(ip, Main.logs.size());
            matchIndex.put(ip, 0);
            if (!ip.equals(leaderId)) {
                Runnable worker = new AppendEntriesThread(ip);
                executor.execute(worker);
            }
        }
        while (Main.serverRole.equals("leader")) {
            Thread.sleep(200);
            for (int i = Main.lastApplied; i < Main.logs.size(); i++) {
                int matchCount = 0;
                for (String ip: matchIndex.keySet()) {
                    if (matchIndex.get(ip) >= i)
                        ++matchCount;
                }
                if (i > Main.commitIndex && matchCount > serverNum / 2
                        && Main.logs.get(i).term == Main.currentTerm)
                    Main.commitIndex = i;
            }
            while (Main.commitIndex > Main.lastApplied) {
                ++Main.lastApplied;
                System.out.println("Last Applied: " + Main.lastApplied);
                //TODO: apply log[lastApplied]
                String[] log = Main.logs.get(Main.lastApplied).log;
                TcpServerThread.leaderCommit(log);
            }
        }

        Main.waitForElection();
    }
}

class AppendEntriesThread implements Runnable {
    private String addr;
    private String ip;

    public AppendEntriesThread(String ip) {
        this.addr = "rmi://" + ip + ":1099/AppendEntries";
        this.ip = ip;
    }

    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName()+" Send heartbeat to: "+addr);
        Timer heartbeatTimer = new Timer();
        TimerTask sendHeartbeatTask = new TimerTask() {
            @Override
            public void run() {
                //Design entries
                ArrayList<LogNode> entries = new ArrayList<>();

                if (Main.logs.size() - 1 != 0
                        && Main.logs.size() - 1 >= Leader.nextIndex.get(ip)) {
                    for (int i = Leader.nextIndex.get(ip); i < Main.logs.size(); i++) {
                        entries.add(Main.logs.get(i));
                    }
                }

                int prevLogIndex = Leader.nextIndex.get(ip) - 1;
                int prevLogTerm = Main.logs.get(prevLogIndex).term;
                try {
                    AeRemoteInterface follower = (AeRemoteInterface) Naming.lookup(addr);
                    //TODO: test
//                    System.out.println("entries length: " + entries.size());
//                    System.out.println("term1 " + Main.currentTerm);
//                    System.out.println("pre log index " + prevLogIndex);
//                    System.out.println("pre log term " + prevLogTerm);

                    String[] response = follower.appendEntries(Main.currentTerm, Leader.leaderId,
                            prevLogIndex, prevLogTerm, entries, Main.commitIndex).split(" ");
                    //TODO: test
//                    System.out.println("term2 " + Main.currentTerm);
//                    System.out.println(Arrays.toString(response));
                        //check response
                    if (response.length == 2) {
                        if ("true".equals(response[0])) {
                            int prevMatch = Leader.matchIndex.get(ip);
                            int prevNext = Leader.nextIndex.get(ip);
                            Leader.matchIndex.put(ip, prevMatch + entries.size());
                            Leader.nextIndex.put(ip, prevNext + entries.size());
                        }
                        else if ("false".equals(response[0])) {
                            // Leader term is stale, transfers to follower.
                            if (Integer.parseInt(response[1]) > Main.currentTerm) {
                                Main.currentTerm = Integer.parseInt(response[1]);
                                Main.serverRole = "follower";
                            }
                            else if (Integer.parseInt(response[1]) <= Main.currentTerm) {
                                int prevNext = Leader.nextIndex.get(ip);
                                Leader.nextIndex.put(ip, --prevNext);
                                //TODO test
//                                System.out.println("next index for "+ ip + ": " + Leader.nextIndex.get(ip));
                            }
                        }
                    }
                } catch (RemoteException e) {e.printStackTrace();}
                catch (MalformedURLException e2) {e2.printStackTrace();}
                catch (NotBoundException e3) {e3.printStackTrace();}
            }
        };
        heartbeatTimer.schedule(sendHeartbeatTask, 0, Main.heartbeatInterval);

    }
}
