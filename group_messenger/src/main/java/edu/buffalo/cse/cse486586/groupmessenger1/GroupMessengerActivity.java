package edu.buffalo.cse.cse486586.groupmessenger1;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.widget.Button;
import android.widget.TextView;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static int key = 0;
//    static final String REMOTE_PORT0 = "11108";
//    static final String REMOTE_PORT1 = "11112";
//    static final String REMOTE_PORT2 = "11116";
//    static final String REMOTE_PORT3 = "11120";
//    static final String REMOTE_PORT4 = "11124";
    static final String[] All__PORTS = {"11108","11112","11116","11120","11124"};

    static final int SERVER_PORT = 10000;


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        setContentView(R.layout.activity_group_messenger);

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


        
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
        /*Button b = (Button)findViewById(R.id.button4);*/


        //The below code has been taken as part of PA1
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
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);

            }
        });
        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    /*The below servertask and clienntask is being implemented from the PA1 */
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

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

            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    ObjectInputStream ob_in_str = new ObjectInputStream(socket.getInputStream());
                    String input_msg = (String) ob_in_str.readObject();

                    // Create a new map of values, where column names are the keys

                    ContentValues keyValueToInsert = new ContentValues();

                    // inserting <”key-to-insert”, “value-to-insert”>
                    keyValueToInsert.put("key", key);
                    keyValueToInsert.put("value", input_msg);
                    key = key + 1;
                    Uri newUri = getContentResolver().insert(
                            Uri.parse("content://edu.buffalo.cse.cse486586.groupmessenger1.provider"),  // assume we already created a Uri object with our provider URI
                            keyValueToInsert
                    );


                    publishProgress(input_msg);
                    socket.close();
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

            String filename = "GroupMessengerOutput";
            String string = strReceived + "\n";
            FileOutputStream outputStream;

            try {
                outputStream = openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(string.getBytes());
                outputStream.close();
            } catch (Exception e) {
                Log.e(TAG, "File write failed");
            }

            return;
        }
    }
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {

                //loop applied over all the ports
                for (String repo:All__PORTS) {
                    /*String remotePort = REMOTE_PORT0;
                    if (msgs[1].equals(REMOTE_PORT0))
                        remotePort = REMOTE_PORT1;*/

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(repo));

                    String msgToSend = msgs[0];
                    /*
                     * TODO: Fill in your client code that sends out a message.
                     */
                    /*https://www.journaldev.com/741/java-socket-programming-server-client*/
                    /*creating an output stream to send the message from the client to the server and close the socket after a message
                    is  sent */

                    ObjectOutputStream ob_out_str = new ObjectOutputStream(socket.getOutputStream());
                    ob_out_str.writeObject(msgToSend);


                    socket.close();


                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException "+e.toString());
            }

            return null;
        }
    }
}



