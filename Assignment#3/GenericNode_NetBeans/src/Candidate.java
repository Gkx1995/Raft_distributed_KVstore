/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author kaixuangao
 */
import java.net.*;
import java.net.UnknownHostException;
import java.rmi.*;
import java.util.Date;
import java.util.concurrent.*;

public class Candidate {
    public volatile static int voteCount = 1;
    public volatile static boolean electionCanceled = false;
    public static boolean leaderGranted = false;
    private String candidateId;

    Candidate() throws UnknownHostException {
        Main.serverRole = "candidate";
        candidateId = InetAddress.getLocalHost().toString().split("/")[1];
        startVote();
    }

    private void startVote() {
        //TODO test
        System.out.println("start vote: "+ new Date());
        Main.currentTerm++;
        voteCount = 1;
        Main.voteFor = candidateId;
        int serverNum = Main.servers.size();

        ExecutorService executor = Executors.newFixedThreadPool(serverNum);

        for (int i = 0; i < serverNum; i++) {
            String ip = Main.servers.get(i).ip;
            if (!ip.equals(candidateId)) {
                Runnable worker = new WorkerThread(ip, candidateId);
                executor.execute(worker);
            }
        }
        executor.shutdown();
        while (!executor.isTerminated()) { } // Wait for all requestVote finishing.
        System.out.println("Finished all vote requests!");
        //TODO test
        System.out.println("election canceled: " + electionCanceled);

        if (voteCount > serverNum / 2 && !electionCanceled)
        {
            leaderGranted = true;
            Leader.leaderId = candidateId;
            System.out.println("Leader has been elected: " + candidateId);
        }
        else if (voteCount <= serverNum / 2 && !electionCanceled && Main.serverRole.equals("candidate"))
        {
            try { Thread.sleep(1000); } catch (InterruptedException e){e.printStackTrace();}
            System.out.println("Restart election!");
            startVote();
        }

    }
}

class WorkerThread implements Runnable {

    private String addr;
    private String candidateId;

    public WorkerThread(String ip, String candidateId){
        this.addr = "rmi://" + ip + ":1100/RequestVote";
        this.candidateId = candidateId;
    }

    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName()+" Request vote from: "+addr);
        try {
            int tmp = Main.logs.size() - 1;
            RvRemoteInterface follower = (RvRemoteInterface) Naming.lookup(addr);
            String[] strings = follower.requestVote(Main.currentTerm, candidateId, tmp, Main.logs.get(tmp).term).split(" ");

            if (strings.length == 2) {
                if ("TRUE".equals(strings[0]))
                    ++Candidate.voteCount;
                else if ("FALSE".equals(strings[0])) {

                    // Candidate's term is stale, election canceled, go back to be follower.
                    if (Integer.parseInt(strings[1]) > Main.currentTerm) {
                        Main.currentTerm = Integer.parseInt(strings[1]);
                        Candidate.electionCanceled = true;
                        Main.serverRole = "follower";
                    }
                }
            }
            else System.out.println("Vote error!");

        } catch (RemoteException e1) {e1.printStackTrace();}
          catch (MalformedURLException e2) {e2.printStackTrace();}
          catch (NotBoundException e3) {e3.printStackTrace();}
        System.out.println(Thread.currentThread().getName()+" End.");
    }
}
