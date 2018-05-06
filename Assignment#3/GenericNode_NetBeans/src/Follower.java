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
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

public class Follower {
    private AppendEntriesImpl appendEntriesImpl;
    public boolean hasHeartbeat = true;

    Follower() throws RemoteException, AlreadyBoundException, MalformedURLException {
        Main.serverRole = "follower";
        this.appendEntriesImpl = new AppendEntriesImpl();
        LocateRegistry.createRegistry(1099);
        Naming.bind("rmi://localhost:1099/AppendEntries", appendEntriesImpl);
        System.out.println("Start listen to heartbeat!");

        RequestVoteImpl requestVoteImpl = new RequestVoteImpl();
        LocateRegistry.createRegistry(1100);
        Naming.bind("rmi://localhost:1100/RequestVote", requestVoteImpl);
        System.out.println("RequestVote RMI object bound successfully!");

//        setTimer();
    }

    public void setTimer() {
        Timer electionTimer = new Timer();
        TimerTask electionTask = new TimerTask() {
            int prevHb = appendEntriesImpl.heartbeatCount;
            @Override
            public void run() {
                if (appendEntriesImpl.heartbeatCount == prevHb) {
                    hasHeartbeat = false;
                    //TODO test
                    System.out.println(new Date());
                    System.out.println("Start election!");
                }
                else {
                    setTimer();
                    System.out.println("Refresh timer!");
                }
            }
        };
        //Random set timer
        int randomTimeout = new Random().nextInt(Main.maxELectionTimeout) %
                (Main.maxELectionTimeout - Main.minELectionTimeout + 1) + Main.minELectionTimeout;
        electionTimer.schedule(electionTask, randomTimeout);
    }
}

interface AeRemoteInterface extends Remote {
    String appendEntries(int term, String leaderId, int prevLogIndex, int prevLogTerm,
                       ArrayList<LogNode> entries, int leaderCommit) throws RemoteException;
}

class AppendEntriesImpl extends UnicastRemoteObject implements AeRemoteInterface {
    public int heartbeatCount = 0;

    AppendEntriesImpl() throws RemoteException {}

    public String appendEntries(int term, String leaderId, int prevLogIndex, int prevLogTerm,
                              ArrayList<LogNode> entries, int leaderCommit) throws RemoteException {
        heartbeatCount = ++heartbeatCount;
        if (leaderCommit > Main.commitIndex) {
            Main.commitIndex = Math.min(leaderCommit, Main.logs.size() - 1);
            while (Main.commitIndex > Main.lastApplied) {
                ++Main.lastApplied;
                //TODO: apply log[lastApplied]
                String[] log = Main.logs.get(Main.lastApplied).log;

                //TODO: test
//                System.out.println(Arrays.toString(log));
//                System.out.println("last log Index: " + (Main.logs.size() - 1));
                System.out.println("commitIndex: " + Main.commitIndex + "; lastApplied: " + Main.lastApplied);

                TcpServerThread.followerCommit(log);
            }
        }

        if (term < Main.currentTerm) {
            return "false " + Main.currentTerm;
        }
        else {
            Leader.leaderId = leaderId;
            if (term > Main.currentTerm) {
                // Cancel election if an election has been started.
//                Candidate.electionCanceled = true;
                Main.serverRole = "follower";
            }
            Main.currentTerm = term;
            if (Main.logs.size() - 1 < prevLogIndex) {
                System.out.println("Some logs missed!");
                return "false " + Main.currentTerm;
            }
            else {
                int localTerm = Main.logs.get(prevLogIndex).term;
                if (localTerm != prevLogTerm) {
                    for (int i = prevLogIndex; i < Main.logs.size(); i++){
                        Main.logs.remove(prevLogIndex);
                    }
                    System.out.println("Log term is inconsistent!");
                    return "false " + Main.currentTerm;
                }
                else {
                    for (int i = 0; i < entries.size(); i++) {
                        Main.logs.add(new LogNode(entries.get(i).term, entries.get(i).log));
                    }
                    return "true " + Main.currentTerm;
                }
            }
        }
    }
}

interface RvRemoteInterface extends Remote {
    String requestVote(int term, String candidateId, int lastLogIndex, int LastLogTerm) throws RemoteException;
}

class RequestVoteImpl extends UnicastRemoteObject implements RvRemoteInterface {

    public RequestVoteImpl() throws RemoteException {

    }

    public String requestVote(int term, String candidateId, int lastLogIndex, int LastLogTerm) throws RemoteException {
        int tmp = Main.logs.size() - 1;
        if (Main.currentTerm == term) {
            if (Main.voteFor.length() == 0 || candidateId.equals(Main.voteFor)) {
                if (lastLogIndex >= tmp && LastLogTerm >= Main.logs.get(tmp).term) {
                    Main.voteFor = candidateId;
                    return "TRUE " + Main.currentTerm;
                } else return "FALSE " + Main.currentTerm;
            }
            else {
                //TODO test
                System.out.println("Already voted for another candidate!");
                return "FALSE " + Main.currentTerm;
            }
        }
        else if (Main.currentTerm < term && lastLogIndex >= tmp && LastLogTerm >= Main.logs.get(tmp).term) {
            Main.currentTerm = term;
            Main.voteFor = candidateId;
            return "TRUE " + Main.currentTerm;
        } else return "FALSE " + Main.currentTerm;
    }
}
