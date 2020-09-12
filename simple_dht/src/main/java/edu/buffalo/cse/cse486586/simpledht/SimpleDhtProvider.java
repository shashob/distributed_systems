package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.io.Serializable;
import java.io.StreamCorruptedException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.provider.BaseColumns;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONObject;

public class SimpleDhtProvider extends ContentProvider {
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    public static int DATABASE_VERSION;

    static {
        DATABASE_VERSION = (int) (Math.random() * 1000);
    }

    FeedReaderDbHelper dbHelper;
    SQLiteDatabase db;
    private Uri myUri;
    private ServerSocket serverSocket;

    String myPort;
    String hash_of_key;
    String current_port_hash = null;
    String successor_hash;
    String ancestor_hash;
    String successor_node = "";
    String ancestor_node = "";
    String portStr;
    String hash_of_selection;

    //List<String> node_list = new ArrayList<String>();
    TreeMap<String, String> hash_table = new TreeMap<String, String>();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        // TODO Auto-generated method stub
        dbHelper = new FeedReaderDbHelper(getContext());
        db = dbHelper.getWritableDatabase();
        int delete_val;
        String key_hash = "";

        String[] searchQuery = {selection};

        try {
            key_hash = genHash(selection);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        try {
            successor_hash = genHash(successor_node);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        try {
            ancestor_hash = genHash(ancestor_node);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }



        if(successor_node.equals("") && ancestor_node.equals("")){

            if (selection.equals("*") || selection.equals("@")){
                delete_val = db.delete(FeedReaderContract.FeedEntry.TABLE_NAME,null,null);

            }
            else{

                delete_val = db.delete(FeedReaderContract.FeedEntry.TABLE_NAME, "key" + " = ?",searchQuery);

            }
            return delete_val;

        }
        else if(selection.equals("@")){
            delete_val = db.delete(FeedReaderContract.FeedEntry.TABLE_NAME,null,null);
            return delete_val;

        }
        else if (key_hash.compareTo(ancestor_hash) >= 0 && key_hash.compareTo(current_port_hash) < 0 || ((ancestor_hash.compareTo(current_port_hash) > 0 && successor_hash.compareTo(current_port_hash) > 0) && (key_hash.compareTo(ancestor_hash) >= 0 || key_hash.compareTo(current_port_hash) < 0))) {
            //Key is stored locally.. Delete it
            delete_val = db.delete( FeedReaderContract.FeedEntry.TABLE_NAME,"key" + " = ?", searchQuery);
            return delete_val;
        }
        else if(selection.equals("*")){

            Shashank del = new Shashank();

            del.setAction("delete");
            del.setPort(portStr);
            del.setSelection(selection);
            del.setSuccessor(successor_node);
            del.setOriginal_avd(portStr);

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,del);

            delete_val = db.delete(FeedReaderContract.FeedEntry.TABLE_NAME,null,null);

            return delete_val;

        }
        else if(selection.charAt(1)=='&' || selection.split("&").length==2){
            String[] vals = selection.split("&");
            String sel_param = vals[0];
            String avd = vals[1];

            if(!avd.equals(successor_node)){
                Shashank del = new Shashank();

                del.setAction("delete");
                del.setPort(portStr);
                del.setSelection(sel_param);
                del.setSuccessor(successor_node);

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,del);


            }

            delete_val = db.delete(FeedReaderContract.FeedEntry.TABLE_NAME,null,null);
            return delete_val;


        }
        else{
            Shashank del = new Shashank();

            del.setAction("delete");
            del.setPort(portStr);
            del.setSelection(selection);
            del.setSuccessor(successor_node);
            del.setOriginal_avd(portStr);

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,del);

        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        String Key = values.getAsString("key");
        String value = values.getAsString("value");

        Log.d("in insert method", "I am this port --->" + portStr + " the ancestor at this moment of the given node is---->" + ancestor_node + " the successor at this moment--->" + successor_node);

        try {
            hash_of_key= genHash(Key);
            Log.d(Key,hash_of_key);

            Log.d("anc", "ancestor is---->" + ancestor_node);
            Log.d("cur", "current node is---->" + portStr);
            Log.d("suc", "successor is---->" + successor_node);
            ancestor_hash = genHash(ancestor_node);
            successor_hash = genHash(successor_node);
            Log.d("insert method", " The ancestor hash is--->" + ancestor_hash + " the node: " + ancestor_node + " The successor hash is--->" + successor_hash + " the node:" + successor_node + " the current hash is--->" + current_port_hash);

            //multiple if conditions to compare the value of the hash
            //single avd
            if (successor_node.equals("") && ancestor_node.equals("")) {
                Log.d("In insert", "This is if condition 1......Inserting on single avd with key-->" + Key);
                db.insertWithOnConflict(FeedReaderContract.FeedEntry.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_IGNORE);
                return uri;
            }
            //base case(covers insertion up to last node)
            else if (hash_of_key.compareTo(ancestor_hash) >= 0 && hash_of_key.compareTo(current_port_hash) < 0) {

                db.insertWithOnConflict(FeedReaderContract.FeedEntry.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_IGNORE);
                Log.d("inserting:", " This is else if condition 2....base case where the key is being inserted is in between two nodes ----> " + values.toString());
                return uri;
            }
            //case for insertion between last node and first node
            //(current_port_hash.compareTo(successor_hash) < 0 && current_port_hash.compareTo(ancestor_hash) < 0)
            else if ((ancestor_hash.compareTo(current_port_hash) > 0 && successor_hash.compareTo(current_port_hash) > 0)) {
                if (hash_of_key.compareTo(ancestor_hash) >= 0 || hash_of_key.compareTo(current_port_hash) < 0) {
                    db.insertWithOnConflict(FeedReaderContract.FeedEntry.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_IGNORE);
                    Log.d("inserting:", " This is else if condition 3....values that is being inserted----> " + values.toString());
                    return uri;
                } else {
                    Shashank msg_insert = new Shashank();
                    msg_insert.setPort(String.valueOf(portStr));
                    msg_insert.setSuccessor(successor_node);
                    msg_insert.setAction("insert");
                    msg_insert.setKey(Key);
                    msg_insert.setValue(value);


                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_insert);
                    Log.d("in insert mode: ", "This is else inside else if forwarding ....Will be passing on from this node:" + msg_insert.getPort() + "   to this node to check if it can be inserted--->" + msg_insert.getSuccessor());

                }

            }
            //else forward to next node
            else {

                Shashank msg_insert = new Shashank();
                msg_insert.setPort(String.valueOf(portStr));
                msg_insert.setSuccessor(successor_node);
                msg_insert.setAction("insert");
                msg_insert.setKey(Key);
                msg_insert.setValue(value);

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_insert);

                Log.d("in insert mode: ", "This is final else condition.....Will be passing on from this node: " + msg_insert.getPort() + "   to this node to check if it can be inserted--->" + msg_insert.getSuccessor());
            }


        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }


        //generate hash of the obtained key
        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        dbHelper = new FeedReaderDbHelper(getContext());
        db = dbHelper.getWritableDatabase();

        Context context = getContext();
        TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        //Log.d("in oncreate method:","my port is this--->"+portStr+" my successor port is this---->"+successor_node+" my ancestor node is this---->"+ancestor_node);

        //Log.d("In oncreate method","Inside the oncreate method"+" and the value of myPort is-->"+portStr);

        try {
            Log.d("in oncreate method", "reached calling the server");
            //calling it's own server
            serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            e.printStackTrace();
        }


        //storing the current hash value of the incoming port in the variable current_port_hash
        try {
            current_port_hash = genHash(portStr);
            Log.d("in oncreate method", "the port is--->" + portStr);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        //for initial node join
        try {
            hash_table.put(genHash("5554"), "5554");

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        //check the port here
        if (!portStr.equals("5554")) {
            //send message
            //create the string
            Shashank mes_from_create = new Shashank();
            mes_from_create.setAction("join");
            mes_from_create.setPort(portStr);
            mes_from_create.setSuccessor("");
            mes_from_create.setAncestor("");

            //sends the type of task that needs to be done

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, mes_from_create);
            //Log.d("in oncreate method","I am this port --->"+portStr+" the ancestor at this moment of the given node is---->"+ancestor_node+" the successor at this moment--->"+successor_node);
            //Log.d("in oncreate method","stored the hash value"+" initial hash table values are---->"+hash_table.values());
            //Log.d("in oncreate method:","my port is this--->"+mes_from_create.getPort()+" my successor port is this---->"+mes_from_create.getSuccessor()+" my ancestor node is this---->"+mes_from_create.getAncestor());
        }

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        db = dbHelper.getReadableDatabase();

        String[] columns = {"key", "value"};

        String searchClause = "key" + " = ?";
        String[] searchQuery = {selection};


        Log.d("In query method", "The selection string is --->" + selection);

        try {
            hash_of_selection = genHash(selection);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        try {
            successor_hash = genHash(successor_node);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        try {
            ancestor_hash = genHash(ancestor_node);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        Log.d(TAG,hash_of_selection);
        Log.d(TAG,successor_hash);
        Log.d(TAG,ancestor_hash);

        //if there is no avd present and query has to be searched in the local partition
        if (ancestor_node.equals("") && successor_node.equals("")) {
            //if there is a single avd and the selection  parameters are either * and @, means fetch from the same table
            if (selection.equals("*") || selection.equals("@")) {

                Log.d("in query method", "if where there is no successor or ancestor" + " the selection parameter is--->" + selection);

                Cursor cursor = db.rawQuery("Select * from " + FeedReaderContract.FeedEntry.TABLE_NAME, null);
                MatrixCursor mc = new MatrixCursor(columns);

                cursor.moveToFirst();
                while (!cursor.isAfterLast()) {
                    Object[] values = {cursor.getString(0), cursor.getString(1)};
                    mc.addRow(values);
                    cursor.moveToNext();
                }
                cursor.close();


                return mc;

            } else {
                //if checking for a key specifically
                Log.d("in query method", " the selection parameter is --->" + selection);
                Cursor cursor = db.query(FeedReaderContract.FeedEntry.TABLE_NAME, columns, searchClause, searchQuery, null, null, null);
                //Log.v("query", selection);
                cursor.moveToFirst();
                Object[] values = {cursor.getString(0), cursor.getString(1)};
                MatrixCursor matrixCursor = new MatrixCursor(columns);
                matrixCursor.addRow(values);
                cursor.close();

                return matrixCursor;

            }
        }
        else if (selection.equals("@")) {

            Log.d("in query method", "Fetching all values from the same table" + " the selection parameter is--->" + selection);

            //fetch values in the given partition
            Cursor cursor = db.rawQuery("Select * from " + FeedReaderContract.FeedEntry.TABLE_NAME, null);
            MatrixCursor mc = new MatrixCursor(columns);

            cursor.moveToFirst();
            while (!cursor.isAfterLast()) {
                Object[] values = {cursor.getString(0), cursor.getString(1)};
                mc.addRow(values);
                cursor.moveToNext();
            }
            cursor.close();

            return mc;

        }
        else if(selection.equals("*")){

            Log.d("In query method", " Now fetching values from all the tables starting from this node---->"+portStr+" the selection parameter is---->"+selection);

            //First fetch all the values from own table

            MatrixCursor mc = new MatrixCursor(columns);

            Shashank query_all = new Shashank();

            query_all.setAction("query");
            query_all.setPort(portStr);
            query_all.setSelection(selection);
            query_all.setSuccessor(successor_node);
            query_all.setOriginal_avd(portStr);


            //now call continously all the remaining tables
            try {
                HashMap cursor_query = new QueryClient().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, query_all).get();

                Log.d("QUERY METHOD:"," THE SIZE OF THE HASHMAP RECEIVED FINALLY AFTER * IS----->"+cursor_query.size());

                Object key = "";
                Object val = "";

                Iterator key_val = cursor_query.entrySet().iterator();
                while(key_val.hasNext()){
                    Map.Entry pair = (Map.Entry)key_val.next();
                    key = pair.getKey();
                    val = pair.getValue();
                    Object[] values = {key,val};
                    mc.addRow(values);

                }




                Log.d("In query method:"," Fetched the cursor object from all the tables");

                Cursor cursor = db.rawQuery("Select * from " + FeedReaderContract.FeedEntry.TABLE_NAME, null);

                cursor.moveToFirst();
                while (!cursor.isAfterLast()) {
                    Object[] values_all = {cursor.getString(0), cursor.getString(1)};
                    mc.addRow(values_all);
                    cursor.moveToNext();
                }
                cursor.close();

                return mc;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

        }
        else if(selection.charAt(1)=='&' || selection.split("&").length==2){
            String[] vals = selection.split("&");
            String sel_param = vals[0];
            String avd = vals[1];
            MatrixCursor mc = new MatrixCursor(columns);

            Log.d("query method","Entered looping for broadcasting to all the avds");

            if(!avd.equals(successor_node)){
                Shashank query_all = new Shashank();

                query_all.setAction("query");
                query_all.setPort(portStr);
                query_all.setSelection(sel_param);
                query_all.setOriginal_avd(avd);
                query_all.setSuccessor(successor_node);

                Log.d("Query method"," Inside * sending all.... THE MESSAGE BEING SENT IS---->"+query_all.toString());

                try {

                    //this will return a matrix cursor
                    HashMap cursor_query = new QueryClient().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, query_all).get();

                    Log.d("QUERY METHOD"," THE SIZE OF THE HASHMAP IN THE PORT---->"+portStr+" IS--->"+cursor_query.size());
                    Object key = "";
                    Object val = "";

                    Iterator key_val = cursor_query.entrySet().iterator();
                    while(key_val.hasNext()){
                        Map.Entry pair = (Map.Entry)key_val.next();
                        key = pair.getKey();
                        val = pair.getValue();
                        Object[] values = {key,val};
                        mc.addRow(values);

                    }




                    Log.d("Query method"," The hashmap after getting the values from the queryclient is---->"+cursor_query);

                    Log.d("query method"," Returning the cursor to the query caller for a single key");



                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }

            }

            Cursor cursor = db.rawQuery("Select * from " + FeedReaderContract.FeedEntry.TABLE_NAME, null);

            cursor.moveToFirst();
            while (!cursor.isAfterLast()) {
                Object[] values_all = {cursor.getString(0), cursor.getString(1)};
                mc.addRow(values_all);
                cursor.moveToNext();
            }
            cursor.close();

            return mc;

        }

        else if (hash_of_selection.compareTo(ancestor_hash) >= 0 && hash_of_selection.compareTo(current_port_hash) < 0 || ((ancestor_hash.compareTo(current_port_hash) > 0 && successor_hash.compareTo(current_port_hash) > 0) && (hash_of_selection.compareTo(ancestor_hash) >= 0 || hash_of_selection.compareTo(current_port_hash) < 0))) {
            //Key is stored locally
            Log.d("query method"," Searching for single key in my own partition"+" key is---->"+selection);
            Cursor cursor = db.query(FeedReaderContract.FeedEntry.TABLE_NAME, columns, searchClause, searchQuery, null, null, null);
            //Log.v("query", selection);
            cursor.moveToFirst();
            Object[] values = {cursor.getString(0), cursor.getString(1)};
            MatrixCursor matrixCursor = new MatrixCursor(columns);
            matrixCursor.addRow(values);
            cursor.close();
            db.close();
            Log.d("query method"," Returning the cursor to the query caller for a single key");
            return matrixCursor;
        }

        else{
            //got some key to be searched from all the tables
            MatrixCursor mc = new MatrixCursor(columns);

            Shashank query_all = new Shashank();

            query_all.setAction("query");
            query_all.setPort(portStr);
            query_all.setSelection(selection);
            query_all.setSuccessor(successor_node);
            query_all.setOriginal_avd(portStr);

            Log.d("query method "," Calling the query client method to find if the key is present in one of the successors---->"+query_all.toString());

            //now call continously all the remaining tables
            try {

                //this will return a matrix cursor
                HashMap cursor_query = new QueryClient().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, query_all).get();
                Object key = "";
                Object val = "";

                Iterator key_val = cursor_query.entrySet().iterator();
                while(key_val.hasNext()){
                    Map.Entry pair = (Map.Entry)key_val.next();
                    key = pair.getKey();
                    val = pair.getValue();

                }

                Object[] values = {key,val};
                mc.addRow(values);


                Log.d("Query method"," The hashmap after getting the values from the queryclient is---->"+cursor_query);

                Log.d("query method"," Returning the cursor to the query caller for a single key");

                return mc;

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

        }

        return null;
    }



    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, Shashank, Void> {


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

            try {
                while (true) {
                    try {
                        Socket socket = serverSocket.accept();
                        ObjectInputStream ob_in_str = new ObjectInputStream(socket.getInputStream());
                        Shashank input_msg = (Shashank) ob_in_str.readObject();

                        ObjectOutputStream ob_out_str = new ObjectOutputStream(socket.getOutputStream());
                        Log.d("Server side:", "********ENTERED SERVER METHOD*******");
                        Log.d("Server side", "Entered the server from client" + " the ancestor node is--->" + input_msg.getAncestor() + " the successor node is--->" + input_msg.getSuccessor());

                        //Log.d("Server side:", " I am port----->" +"5554"+ " the join request is from the node--->"+input_msg.port);
                        //Log.d("Server side"," ....the ancestor at this moment is---->"+ancestor_node+" the successor at this moment--->"+successor_node);

                        //store the ports in a ordered list
                        if (input_msg.getAction().equals("join")) {
                            Log.d("server side", "the port requesting to join is-->" + input_msg.getPort());

                            try {

                                // appended the hash value to the hash table i.e. a treemap will sort the values for you
                                hash_table.put(genHash(input_msg.getPort()), input_msg.getPort());


                                //reorder the hashmap
                                Log.d("Server side", "The hash table is sorted" + ", and the values are-->" + hash_table.values());

                            } catch (NoSuchAlgorithmException e) {
                                e.printStackTrace();
                            }

                            //calling the comparator to get successor and ancestor for 5554
                            input_msg.setOriginal_avd("5554");
                            String suc_ans_avd0 = cmp(hash_table, input_msg.getOriginal_avd());
                            String[] suc_ans = suc_ans_avd0.split(":");

                            //SETTING THE SUCCESSOR AND ANCESTOR OF THE 5554 AVD HERE WHEN A NEW NODE JOINS
                            successor_node = suc_ans[1];
                            ancestor_node = suc_ans[2];
                            Log.d("Server side", " the successor of 5554 is---->" + successor_node + " the ancestor of the node 5554 is---->" + ancestor_node);

                            //send back message to the join requester that they joined
                            /*input_msg.status="joined";
                            ob_out_to_client.writeObject(input_msg);
                            Log.d("Server side"," The status of the port has been updated---->"+ input_msg.status);
*/
                            //after one node joins just call the client-task through the progress update method
                            Log.d("Server side", " THIS IS THE MESSAGE JUST BEFORE BROADCASTING----->" + input_msg.toString());
                            String[] active_node_list = hash_table.values().toArray(new String[hash_table.size()]);
                            for (int i = 0; i < active_node_list.length; i++) {

                                if (active_node_list[i].equals("5554")) {
                                    continue;

                                }
                                else {

                                    //calculate the values and update in server
                                    String msg_for_broadcast = cmp(hash_table, active_node_list[i]);

                                    Shashank msg_for_progress_update = new Shashank();
                                    Log.d("Server side"," THIS IS THE MESSAGE IN BROADCASTING---->"+msg_for_progress_update.toString());
                                    String[] msg_from_comparator = msg_for_broadcast.split(":");

                                    //set each node's successor and ancestor
                                    msg_for_progress_update.setPort(msg_from_comparator[0]);
                                    msg_for_progress_update.setSuccessor(msg_from_comparator[1]);
                                    msg_for_progress_update.setAncestor(msg_from_comparator[2]);
                                    msg_for_progress_update.setAction(msg_from_comparator[3]);

                                    publishProgress(msg_for_progress_update);
                                    //socket.close();

                                    Log.d("Server side,","inside for loop for broadcasting to each node it's successor and predecessor"+" the message being passsed is--->"+msg_for_progress_update.toString());


                                    //sending the successors and ancestors for each nodes back to its servers
                                    //ObjectOutputStream ob_out_str_in_broad_client = new ObjectOutputStream(socket.getOutputStream());
                                    //ob_out_str_in_broad_client.writeObject(msg_for_progress_update);*//*

                                }


                            }


                        } else if (input_msg.getAction().equals("broadcast")) {

                            successor_node = input_msg.getSuccessor();
                            ancestor_node = input_msg.getAncestor();

                            Log.d("Server side", "In final update position" + ", and the values are-->" + hash_table.values());
                            Log.d("Server side", " ****MY NODE IS****" + input_msg.getPort() + "*****MY SUCCESSOR IS*****" + successor_node + "****MY ANCESTOR NODE IS*****" + ancestor_node);
                            //socket.close();

                        }
                        else if(input_msg.getAction().equals("insert")){
                            String key = input_msg.getKey();
                            String values = input_msg.getValue();
                            Log.d("Server side","called from the insert method"+" the node where the insertion will be tried is--->"+input_msg.getSuccessor());
                            Uri.Builder uriBuilder = new Uri.Builder();
                            uriBuilder.authority("edu.buffalo.cse.cse486586.simpledht.provider");
                            uriBuilder.scheme("content");
                            ContentValues cv = new ContentValues();
                            cv.put("key", key);
                            cv.put("value", values);
                            myUri = uriBuilder.build();
                            Log.d("Server side","the key-value pair inserted is--->"+cv);
                            insert(myUri,cv);
                        }

                        else if(input_msg.getAction().equals("query")){

                            Log.d("Server side:","The incoming request message in query check is---->"+input_msg.toString());
                            //call this server's query method to get the values
                            String selection = input_msg.getSelection();

                            input_msg.setSuccessor(successor_node); //This node is very important for future in binding chain


                            if(selection.equals("*")){

                                selection = "*&";

                                //call the query method to fetch until the last node in the ring
                                Cursor cursor = query(myUri, null, selection + input_msg.getOriginal_avd(), null, null);

                                if(cursor.moveToFirst()){
                                    do{
                                        input_msg.query_table.put(cursor.getString(cursor.getColumnIndex("key")),cursor.getString(cursor.getColumnIndex("value")));
                                    }while (cursor.moveToNext());
                                }

                                cursor.close();


                                Log.d("Server query check"," The input query table has the following values---->"+input_msg.query_table);
                                Log.d("Server query check"," THE SIZE OF THE HASHMAP RECIEVED FROM THE AVD IS---->"+input_msg.query_table.size());
                                //returning the matrix cursor object back to the calling client
                                ob_out_str.writeObject(input_msg);

                                Log.d("Server side"," Sent back the cursor back to the client");


                            }
                            //if it's just a key
                            else{
                                //Log.d("server query check"," The input message here is---->"+input_msg.toString());


                                Cursor cursor = query(myUri, null, selection, null, null);
                                if(cursor.moveToFirst()){
                                    do{
                                        input_msg.query_table.put(cursor.getString(cursor.getColumnIndex("key")),cursor.getString(cursor.getColumnIndex("value")));
                                    }while (cursor.moveToNext());
                                }

                                cursor.close();

                                Log.d("Server query check"," The query table has the values as follows---->"+input_msg.query_table);//everything working fine till here
                                //send back the matrix cursor to the caller and close the cursor
                                ob_out_str.writeObject(input_msg);

                            }

                        }

                        else if(input_msg.getAction().equals("delete")){
                            input_msg.setSuccessor(successor_node);
                            String selection = input_msg.getSelection();

                            if(selection.equals("*")){
                                selection = "*&";

                                delete(myUri, selection+input_msg.getOriginal_avd(), null);

                            }
                            else{
                                delete(myUri,selection,null);
                            }
                        }

                    } catch (IOException e) {
                        System.out.println("Socket connection timed out");
                        Log.d("server side", " caught exception");
                        e.printStackTrace();


                    } catch (ClassNotFoundException e) {
                        Log.e("Class", "ClassNotFoundException");

                    }

                }
            } catch (Exception e){
                    e.printStackTrace();
                    Log.d("Server side","caught in an exception");
            }
            return null;

        }

        protected void onProgressUpdate(Shashank... strings) {
            //String strReceived = strings[0].trim();
            Shashank msg_to_client_from_progress = strings[0];
            //String[] s = strReceived.split(":");
            Log.d("PM","********** ENTERED PROGRESS UPDATE********");

            if (msg_to_client_from_progress.getAction().equals("broadcast")) {
                Log.d("In progress method"," The client task is being called and the message is---->"+msg_to_client_from_progress.toString());
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_to_client_from_progress);
            }

        }


    }

    private class ClientTask extends AsyncTask<Shashank, Void, Void> {

        @Override
        protected Void doInBackground(Shashank... msgs) {



                Socket socket = null;
                try {
                    Log.d("Client side:", " **********ENTERED CLIENT METHOD********");
                    //creating new instance of that class(to get values that are updated
                    Shashank in_client = msgs[0];
                    //Shashank in_client = new Shashank();

                    Log.d("Client side", "the incoming task is---->" + in_client.getAction() + " and the port from which request has arised is--->" + in_client.getPort());

                    if (in_client.getAction().equals("join")) {
                        Log.d("client side", "Now in the join operation of the client");
                        //binding to the 5554 server for joining the incoming node
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(String.valueOf(5554 * 2)));


                        Log.d("Connect", Boolean.toString(socket.isConnected()));
                        Log.d("Client side", "the message being sent to the server is from join is---->" + in_client.toString());
                        ObjectOutputStream ob_out_str = new ObjectOutputStream(socket.getOutputStream());
                        ob_out_str.writeObject(in_client);
                        ob_out_str.flush();

//                        socket.close();

                    } else if (in_client.getAction().equals("broadcast")) {
                        Log.d("client side", "Now in the broadcast operation of the client" + " the port that will be binded is----->" + in_client.getPort());

                        int port = Integer.parseInt(in_client.getPort());
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(String.valueOf(port * 2)));

                        //sending the successors and ancestors for each nodes back to its servers
                        ObjectOutputStream ob_out_str_in_broad_client = new ObjectOutputStream(socket.getOutputStream());
                        ob_out_str_in_broad_client.writeObject(in_client);
                        ob_out_str_in_broad_client.flush();

                        Log.d("Client side:", "sending message for each node in the list to update its successor and predecessor" + " FINAL MESSAGE FOR UPDATION IS--->" + in_client.toString());

//                        socket.close();

                    } else if (in_client.getAction().equals("insert")) {
                        Log.d("client side", "Now in the insert operation of the client" + " the port that will be binded is----->" + in_client.getSuccessor());
                        int port = Integer.parseInt(in_client.getSuccessor());
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(String.valueOf(port * 2)));

                        //sending the successors and ancestors for each nodes back to its servers
                        ObjectOutputStream ob_out_str_in_insert_client = new ObjectOutputStream(socket.getOutputStream());
                        ob_out_str_in_insert_client.writeObject(in_client);
                        ob_out_str_in_insert_client.flush();

//                        socket.close();

                    }
                    else if(in_client.getAction().equals("delete")){
                        Log.d("Client side","Now in the delete operation of the client"+" the port that will be binded is --->"+ in_client.getSuccessor());

                        int port = Integer.parseInt(in_client.getSuccessor());
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(String.valueOf(port * 2)));

                        //sending the successors and ancestors for each nodes back to its servers
                        ObjectOutputStream ob_out_str_in_insert_client = new ObjectOutputStream(socket.getOutputStream());
                        ob_out_str_in_insert_client.writeObject(in_client);
                        ob_out_str_in_insert_client.flush();

                    }

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException " + e.toString());
                } catch (NullPointerException e) {
                    Log.e(TAG, "Null pointer exception caught");
                    System.out.print("NullPointerException Caught");
                }

//                try {
//                   socket.close();
//                } catch (IOException e) {
//                   e.printStackTrace();
//                }catch (NullPointerException e){
//                   e.printStackTrace();
//                }

                Log.d("Client side:"," reached the end of the client method");
                return null;

        }


    }

   private class QueryClient extends AsyncTask<Shashank,Void,HashMap> {


        @Override
        protected HashMap doInBackground(Shashank... shashanks) {
            String[] columns = {"key", "value"};
            MatrixCursor mc = new MatrixCursor(columns);

            HashMap<String, String> hash_query = new HashMap<String, String>();
            Socket socket;
            try {
                Shashank in_query = shashanks[0];

                String cur_node = in_query.getOriginal_avd();// store the originating calling avd
                String successor = in_query.getSuccessor();//set the first successor


                if (in_query.getAction().equals("query")) {


                    //loop until the current node doesn't equal the successor
                    if(in_query.getSelection().equals("*")){


                        Log.d("query Client side", "Now in the query method of the client" + " the incoming request message is---->" + in_query.toString());
                        int port = Integer.parseInt(successor);
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(String.valueOf(port * 2)));

                        //sending the successors and ancestors for each nodes back to its servers
                        ObjectOutputStream ob_out_str_in_query_client = new ObjectOutputStream(socket.getOutputStream());
                        ob_out_str_in_query_client.writeObject(in_query);
                        ob_out_str_in_query_client.flush();

                        //get the cursor first
                        ObjectInputStream ob_in_str_for_cursor = new ObjectInputStream(socket.getInputStream());
                        Shashank input_cursor = (Shashank) ob_in_str_for_cursor.readObject();

                        Log.d("Query client method:"," The message received from the server after query check is---->"+input_cursor.toString());
                        Log.d("query client method"," The values in the hash table of the object received is---->"+input_cursor.query_table);

                        hash_query = input_cursor.query_table;

                        //returning the cursor back to the caller
                        Log.d("query client method"," Returning the hashmap of size---->"+hash_query.size());
                        return hash_query;


                        //socket.close();

                    }
                    else{
                        Log.d("query client method"," Entered else condition.... checking for a specific key");


                        Log.d("query Client side", "Now in the query method of the client" + " the incoming request message is---->" + in_query.toString());
                        int port = Integer.parseInt(successor);
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(String.valueOf(port * 2)));

                        //sending the successors and ancestors for each nodes back to its servers
                        ObjectOutputStream ob_out_str_in_query_client = new ObjectOutputStream(socket.getOutputStream());
                        ob_out_str_in_query_client.writeObject(in_query);
                        ob_out_str_in_query_client.flush();

                        //get the cursor first
                        ObjectInputStream ob_in_str_for_cursor = new ObjectInputStream(socket.getInputStream());
                        Shashank input_cursor = (Shashank) ob_in_str_for_cursor.readObject();

                        Log.d("query client method"," The input cursor received from--->"+port+" is--->"+input_cursor.toString());

                        hash_query = input_cursor.query_table;

                        //returning the cursor back to the caller
                        Log.d("query client method"," Returning the hashmap---->"+hash_query);
                        return hash_query;


                    }

                }


            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException " + e.toString());
            } catch (NullPointerException e) {
                Log.e(TAG, "Null pointer exception caught");
                System.out.print("NullPointerException Caught");
            }catch(ClassNotFoundException e){
                Log.e(TAG,"Class not found exception caught");
            }

            return null;

        }
    }

    public String cmp(TreeMap<String, String > hash_table, String port){
        String successor="_";
        String ancestor="_";
        String msg_for_broadcast="_";
        //calculate the suc and ans of 5554 and update it here
        try{
            if(hash_table.higherKey(genHash(port))!=null){
                Log.d("Server side:"," entered hashkey check");
                Map.Entry<String, String> suc = hash_table.higherEntry(genHash(port));
                successor= suc.getValue();

            }else{
                Map.Entry<String, String> suc = hash_table.firstEntry();
                successor= suc.getValue();

            }

            //checking for the first node
            if(hash_table.lowerKey(genHash(port))!=null){
                Log.d("Server side:"," entered hashkey check");
                Map.Entry<String,String> ans = hash_table.lowerEntry(genHash(port));
                ancestor=ans.getValue();
            }
            else{
                Map.Entry<String,String > ans = hash_table.lastEntry();
                ancestor=ans.getValue();

            }
            msg_for_broadcast = port+":"+successor+":"+ancestor+":"+"broadcast";

        }
        catch (NoSuchAlgorithmException e) {
            Log.d("In comparator","Entered the exception");
            e.printStackTrace();
        }
        return msg_for_broadcast;
    }


    public final class FeedReaderContract {
        // To prevent someone from accidentally instantiating the contract class,
        // make the constructor private.
//        private FeedReaderContract() {}

        /* Inner class that defines the table contents */
        public class FeedEntry implements BaseColumns {
            public static final String TABLE_NAME = "entry";
            public static final String COLUMN_NAME_TITLE = "key";
            public static final String COLUMN_NAME_TITLE2 = "value";


        }


    }

    static final String SQL_CREATE_ENTRIES =
            "CREATE TABLE " + FeedReaderContract.FeedEntry.TABLE_NAME + " (" +
                    FeedReaderContract.FeedEntry.COLUMN_NAME_TITLE + " TEXT PRIMARY KEY," +
                    FeedReaderContract.FeedEntry.COLUMN_NAME_TITLE2 + " TEXT)";

    static final String SQL_DELETE_ENTRIES =
            "DROP TABLE IF EXISTS " + FeedReaderContract.FeedEntry.TABLE_NAME;

    class FeedReaderDbHelper extends SQLiteOpenHelper {
        // If you change the database schema, you must increment the database version.

        public static final String DATABASE_NAME = "FeedReader.db";


        public FeedReaderDbHelper(Context context) {
            super(context, DATABASE_NAME, null,DATABASE_VERSION);// (int)(Math.random()*1000));
        }
        public void onCreate(SQLiteDatabase db) {
            db.execSQL(SQL_CREATE_ENTRIES);
        }
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            // This database is only a cache for online data, so its upgrade policy is
            // to simply to discard the data and start over
            db.execSQL(SQL_DELETE_ENTRIES);
            onCreate(db);
        }
        public void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            onUpgrade(db, oldVersion, newVersion);
        }
    }
}

class Shashank implements Serializable {
    String action;
    String port;
    String successor;
    String ancestor;
    String original_avd;
    String status;
    String key;
    String value;
    String selection;
    Cursor cursor;

    public HashMap<String, String> getQuery_table() {
        return query_table;
    }

    public void setQuery_table(HashMap<String, String> query_table) {
        this.query_table = query_table;
    }

    HashMap<String, String> query_table = new HashMap<String, String>();

    @Override
    public String toString() {
        return "Shashank{" +
                "action='" + action + '\'' +
                ", port='" + port + '\'' +
                ", successor='" + successor + '\'' +
                ", ancestor='" + ancestor + '\'' +
                ", original_avd='" + original_avd + '\'' +
                ", status='" + status + '\'' +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", selection='" + selection + '\'' +
                '}';
    }

    public String getSelection() {
        return selection;
    }

    public void setSelection(String selection) {
        this.selection = selection;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getSuccessor() {
        return successor;
    }

    public void setSuccessor(String successor) {
        this.successor = successor;
    }

    public String getAncestor() {
        return ancestor;
    }

    public void setAncestor(String ancestor) {
        this.ancestor = ancestor;
    }

    public String getOriginal_avd() {
        return original_avd;
    }

    public void setOriginal_avd(String original_avd) {
        this.original_avd = original_avd;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
