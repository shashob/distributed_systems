package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;


/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static int key = 0;
    static int providerKey =0;
    static String providerUri = "content://edu.buffalo.cse.cse486586.groupmessenger2.provider";
    ContentValues keyValueToInsert = new ContentValues();
    static PriorityQueue <String> message_queue = new PriorityQueue<>(10,new StringComparator());

    //    static final String REMOTE_PORT0 = "11108";
//    static final String REMOTE_PORT1 = "11112";
//    static final String REMOTE_PORT2 = "11116";
//    static final String REMOTE_PORT3 = "11120";
//    static final String REMOTE_PORT4 = "11124";
    static final String[] All__PORTS = {"11108","11112","11116","11120","11124"};

    static final int SERVER_PORT = 10000;
    String myPort;


    /*implement b-multicast here*/
    /*int priority = 0;
    PriorityQueue<Message> newMessage= new PriorityQueue<message>(50);*/

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);


//        Uri.Builder uriBuilder = new Uri.Builder();
//        uriBuilder.authority("edu.buffalo.cse.cse486586.groupmessenger2.provider");
//        uriBuilder.scheme("content");
//        providerUri = uriBuilder.build();

//        Uri newUri = getContentResolver().insert(
//                Uri.parse("content://edu.buffalo.cse.cse486586.groupmessenger2.provider"),  // assume we already created a Uri object with our provider URI
//                keyValueToInsert
//        );

        /*
         * Calculate the port number that this AVD listens on.
         * It is just a hack that I came up with to get around the networking limitations of AVDs.
         * The explanation is provided in the PA1 spec.
         */
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides. Please make sure
             * you know how it works by reading
             * http://developer.android.com/reference/android/os/AsyncTask.html
             */
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            /*
             * Log is a good way to debug your code. LogCat prints out all the messages that
             * Log class writes.
             *
             * Please read http://developer.android.com/tools/debugging/debugging-projects.html
             * and http://developer.android.com/tools/debugging/debugging-log.html
             * for more information on debugging.
             */
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        Button button = (Button) findViewById(R.id.button4);
        button.setOnClickListener(new View.OnClickListener() {
            public void onClick(View v) {
                // Do something in response to button click
                EditText ed = (EditText) findViewById(R.id.editText1);
                String msg = ed.getText().toString() + "\n";
                ed.setText(""); // This is one way to reset the input box.
//                TextView localTextView = (TextView) findViewById(R.id.textView1);
//                localTextView.append("\t" + msg); // This is one way to display a string.



                /*
                 * Note that the following AsyncTask uses AsyncTask.SERIAL_EXECUTOR, not
                 * AsyncTask.THREAD_POOL_EXECUTOR as the above ServerTask does. To understand
                 * the difference, please take a look at
                 * http://developer.android.com/reference/android/os/AsyncTask.html
                 */
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);

            }
        });
        
        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));
        
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    /*The below servertask and clientask is being implemented from the PA1 */
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        //added now


        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            /*https://www.journaldev.com/741/java-socket-programming-server-client*/
            /*the below loop checks continously for a connection request from the client using the accept() method and has try catch exception handlers if
            an input connection fails or class is not found */


            //HashMap<Integer,String> hm = new HashMap<Integer, String>();

            while (true) {
                try {

                    Socket socket = serverSocket.accept();
                    ObjectInputStream ob_in_str = new ObjectInputStream(socket.getInputStream());
                    String input_msg = (String) ob_in_str.readObject();

                    Log.d("Server side: ","message received in server from the client is---->"+ input_msg);

                    //Log.d("checking","contents of the queue are: "+message_queue.toString());



                    //send to the client the highest sequence number+1 heard so far
                    key = key+1;

                    String[] str = input_msg.split(":");

                    //string with only message and client id
                    if (str[5].equals("0")){
                        //added to the priority queue

                        //get the values of the message, status, priority etc.
                        String mess = str[0];
                        String id = str[1];


                        //sending proposed priority
                        ObjectOutputStream proposed_priority = new ObjectOutputStream(socket.getOutputStream());
                        proposed_priority.writeObject(mess+":"+id+":"+key+":"+myPort+":"+"Proposalsent"+":"+"0"); //now format is msg:priority:key

                        Log.d("Server side", "message being entered to queue and the queue is---->"+message_queue.toString());
                        message_queue.add(mess+":"+id+":"+key+":"+myPort+":"+"Proposalsent"+":"+"0");

                    }
                    //string with final proposal values
                    if (str[5].equals("1")){

                        //String final_val = message_queue.peek();
                        String[] str_final = input_msg.split(":");
                        String mess = str_final[0];

                        String obtained_proposal = str[2];
                        String obtained_port = str[3];
                        Log.d("Server side","The final proposal of the msg is--->"+obtained_proposal);


                        if(key<Integer.parseInt(obtained_proposal)){
                            key = Integer.parseInt(obtained_proposal);

                        }



                        Iterator value = message_queue.iterator();

                        while (value.hasNext()){
                            String m = (String) value.next();
                            String[] st = m.split(":");

                            String ms = st[0];
                            if(ms.equals(mess)){
                                Log.d("Server side","The message that will be deleted is--> "+m);
                                value.remove();
                                Log.d("Server side", "Size of the priority queue after deletion is: "+message_queue.size());
                            }

                        }
                        Log.d("Server side","The message that is being added to the pr queue is -->"+input_msg);
                        message_queue.add(input_msg);
                        Log.d("Server side","The top most element in the priority queue now is  -->"+message_queue.peek());




                    }
                    //Log.d("checking","top most element in pq is :"+message_queue.peek()+"and size is :"+message_queue.size());

                    String msg = message_queue.peek();
                    while(message_queue.size() >0 && msg.split(":")[5].equals("1")){
                        String top_msg = message_queue.poll();

                        Log.d("Server side","The polled message from the queue is: "+top_msg);

                        String[] topmsg = top_msg.split(":");
                        Log.d("Server side","delivery status of the message is: "+topmsg[5]+"   :size of the queue at this point: "+message_queue.size());

                        publishProgress(topmsg[0]);
                        Log.d("SERVER Published--->",topmsg[0]);
                        //socket.close();

                        msg = message_queue.peek();



                    }

                } catch (IOException e) {
                    System.out.println("Socket connection timed out");
                    break;
                } catch (ClassNotFoundException e) {
                    Log.e("Class", "ClassNotFoundException");
                    break;
                }
            }

            return null;
        }

        protected void onProgressUpdate(String... strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");

            /*
             * The following code creates a file in the AVD's internal storage and stores a file.
             *
             * For more information on file I/O on Android, please take a look at
             * http://developer.android.com/training/basics/data-storage/files.html
             */
            // Create a new map of values, where column names are the keys

            String filename = "GroupMessengerOutput";
            String string = strReceived + "\n";
            FileOutputStream outputStream;

/*
            ContentValues keyValueToInsert = new ContentValues();

            // inserting <”key-to-insert”, “value-to-insert”>
            keyValueToInsert.put("key", key);
            keyValueToInsert.put("value", strReceived);
            key = key + 1;
            Uri newUri = getContentResolver().insert(
                    Uri.parse("content://edu.buffalo.cse.cse486586.groupmessenger1.provider"),  // assume we already created a Uri object with our provider URI
                    keyValueToInsert
            );
            */

            Log.d("Checking","cuurently in publish progress"+strReceived);
            // inserting <”key-to-insert”, “value-to-insert”>
            keyValueToInsert.put("key", providerKey++);
            keyValueToInsert.put("value", strReceived);
            getContentResolver().insert(Uri.parse(providerUri),keyValueToInsert);
//            key++;

            return;
        }
    }
    private class ClientTask extends AsyncTask<String, Void, Void> {
        String final_msg = "";
        String final_server_priority = "";
        String final_sever_port= "";
        float prev_max_priority = -1;
        @Override
        protected Void doInBackground(String... msgs) {
            try {

                //Integer priority_num = 1;

                //Step-1: B-multicast to everyone
                //proposal part: first for loop
                key = key+1;
                for (String repo:All__PORTS) {
                    /*String remotePort = REMOTE_PORT0;
                    if (msgs[1].equals(REMOTE_PORT0))
                        remotePort = REMOTE_PORT1;*/

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(repo));

                    String msgToSend = msgs[0];

                    //sending to its own server and incrementing the key value

                    String priority_id = key+"->"+myPort;

                    String new_msg = msgToSend+":"+priority_id+":"+"0"+":"+"0"+":"+"0"+":"+"0"; //now message with pid in the format  msg:priority and some junk

                    Log.d("Client side: ","message to be sent from client for proposal: "+new_msg);


                    /*
                     * TODO: Fill in your client code that sends out a message.
                     */
                    /*https://www.journaldev.com/741/java-socket-programming-server-client*/
                    /*creating an output stream to send the message from the client to the server and close the socket after a message
                    is  sent */

                    //first b-multicast to the other avds
                    ObjectOutputStream ob_out_str = new ObjectOutputStream(socket.getOutputStream());
                    ob_out_str.writeObject(new_msg);


                    //reads the proposal priority
                    ObjectInputStream ob_in_str = new ObjectInputStream(socket.getInputStream());
                    String prop_priority = (String) ob_in_str.readObject();

                    Log.d("Client side","The obtained proposed priority from the server is---> "+prop_priority);


                    String[] str_final = prop_priority.split(":");

                    String msg = str_final[0];
                    String client_key = str_final[1];
                    String server_priority = str_final[2];
                    String server_port = str_final[3];



                    //this line will combine like 1.11108 and compare with next value
                    float current_max_priority = Float.parseFloat(server_priority+"."+server_port);

                    //calculates their maximum value
                    if(current_max_priority>prev_max_priority){
                        prev_max_priority = current_max_priority;

                        //reset the final_status value

                        final_msg = msg+":"+client_key+":"+server_priority+":"+server_port+":"+"finalporposal"+":"+"1";

                        Log.d("Client side","the current maximum priority is: "+current_max_priority);

                        final_server_priority = server_priority;



                    }

                    //store proposed priority
                    socket.close();

                }

                /*
                if(key<Integer.parseInt(final_server_priority)){
                    key = Integer.parseInt(final_server_priority);
                }


                 */


                //getting the maximum proposed priority list. This would have the maximum priority value
                //Integer max = Collections.max(proposed_pr_list);

                Log.d("Client side ","The final max priority is"+ final_server_priority);

                //Log.d("Checking","Final server priority is:"+ final_server_priority);

                for(String repo1:All__PORTS) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(repo1));

                    String msgToSend = final_msg;

                    Log.d("Client side","The message being sent after deciding final proposal is---->"+final_msg);

                    ObjectOutputStream ob_out_str = new ObjectOutputStream(socket.getOutputStream());
                    ob_out_str.writeObject(msgToSend);

                    socket.close();

                }




            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException "+e.toString());
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            return null;
        }
    }

    static class StringComparator implements Comparator<String>{
        //public StringComparator() {
        //}


        @Override
        public int compare(String lhs, String rhs) {
            Log.d("Comparator side","The message that is being compare are ---->"+ lhs+ "----------and---------"+ rhs);
            String[] str1  = lhs.split(":");

            String[] str2 = rhs.split(":");

            String server_pr1 = str1[2];
            String server_pr2 = str2[2];

            String server_po1 = str1[3];
            String server_po2 = str2[3];

            String s1 = server_pr1+"."+server_po1;
            String s2 = server_pr2+"."+server_po2;

            //this line will combine like 1.11108 and compare with next value
            float current_max = Float.parseFloat(s1);
            float current_max2 = Float.parseFloat(s2);

            if(current_max<current_max2){
                return -1;
            }
            else{
                return 1;
            }
        }
    }
}
