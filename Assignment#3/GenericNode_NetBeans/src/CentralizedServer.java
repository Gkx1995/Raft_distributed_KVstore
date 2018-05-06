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
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;

public class CentralizedServer {

    private BufferedReader sin;
    private PrintWriter sout;
    private NodeDirectory members;
    private ServerSocket serverSocket;

    public CentralizedServer(int portNo) throws IOException {
        this.serverSocket = new ServerSocket(portNo);
        System.out.println("The node directory server is started: " + serverSocket);
        this.members = new NodeDirectory();

        try {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                this.sin = new BufferedReader(new InputStreamReader(clientSocket
                        .getInputStream()));
                this.sout = new PrintWriter(new BufferedWriter(new OutputStreamWriter(
                        clientSocket.getOutputStream())), true);
                try {
                    while (true) {
                        String str = sin.readLine();
                        if (str != null && !str.isEmpty()) {

                            if (str.equals("store")) {
                                sout.println(members.store());
                                break;
                            }
                            else {
                                members.put(str);
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
        } finally {
            serverSocket.close();
        }
    }
}

class NodeDirectory {
    private Set<String> members = new HashSet<>();

    synchronized void put(String key) {
        members.add(key);
        System.out.println("Node directory updated!");
    }

    String store() {
        String string = "";
        for (String str: members) {
            string += str + " ";
        }
        return string;
    }
}

