package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

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

public class SimpleDynamoProvider extends ContentProvider {
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
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

	String[] final_ports = {"11124","11112","11108","11116","11120"};
	//List<String> node_list = new ArrayList<String>();
	TreeMap<String, String> hash_table = new TreeMap<String, String>();
	private ArrayList<String> hash_of_ports;

	String[] final_ports_small = {"5562","5556","5554","5558","5560"};


	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		FeedReaderDbHelper dbHelper = new FeedReaderDbHelper(getContext());
		SQLiteDatabase db = dbHelper.getWritableDatabase();

		/*dbHelper = new FeedReaderDbHelper(getContext());
		db = dbHelper.getWritableDatabase();*/

		db.execSQL("DELETE FROM "+ FeedReaderContract.FeedEntry.TABLE_NAME);


		int delete_val;
		String key_hash = "";

		try {
			key_hash = genHash(selection);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		String port = getLocation(key_hash);

		if(port.equals(portStr)){
			//own port
			if (selection.equals("*") || selection.equals("@")){
				delete_val = db.delete(FeedReaderContract.FeedEntry.TABLE_NAME,null,null);
				return  delete_val;

			}
			else{
				delete_val = db.delete(FeedReaderContract.FeedEntry.TABLE_NAME, null,null);
				return delete_val;
			}
		}
		else{
			Shashank del = new Shashank();

			String all_replicas = successors(port);

			String[] rep = all_replicas.split(":");
			String replica1 = rep[0];
			String replica2 = rep[1];

			del.setAction("delete");
			del.setPort(port);
			del.setSelection(selection);
			del.setOriginal_avd(portStr);
			del.active_ports.add(replica1);
			del.active_ports.add(replica2);
			delete_val = db.delete(FeedReaderContract.FeedEntry.TABLE_NAME, null,null);

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,del);

			return delete_val;

		}




	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		Log.d("insert method","*********NOW INSIDE INSERT METHOD*********");

		FeedReaderDbHelper dbHelper = new FeedReaderDbHelper(getContext());
		SQLiteDatabase db = dbHelper.getWritableDatabase();
		//int version = 1;*/

		String Key = values.getAsString("key");
		String value = values.getAsString("value");


		Log.d("insert method","Key is---->"+Key);
		Log.d("insert method","Value is---->"+value);

		try{
			hash_of_key = genHash(Key);

			//will contain hash values in arranged order
			String coordinator = getNodes(hash_of_key);
			String[] n = coordinator.split(":");
			String actual_node = n[0];
			String next_node = n[1];
			String next_to_next_node = n[2];

			Log.d(TAG,actual_node);
			Log.d(TAG,next_node);
			Log.d(TAG,next_to_next_node);

			Log.d(TAG," hash table is--->"+hash_table);
			Log.d(TAG," hash of the key--->"+hash_of_key);
			//String port = final_ports_small[p];
			Log.v("Port is", hash_table.get(actual_node));
			//Log.d("insert method"," The value of the port where the key values will be inserted is---->"+port);

			Shashank send2 = new Shashank();
			send2.setAction("insert");
			send2.setKey(Key);
			send2.setValue(value);
			//new
			send2.active_ports.add(hash_table.get(actual_node));
			send2.active_ports.add(hash_table.get(next_node));
			send2.active_ports.add(hash_table.get(next_to_next_node));


			if(actual_node.equals(current_port_hash) || next_node.equals(current_port_hash)|| next_to_next_node.equals(current_port_hash)){
				Log.d("insert method"," Insertion done in self");
				db.insertWithOnConflict(FeedReaderContract.FeedEntry.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_IGNORE);

				send2.active_ports.remove(send2.active_ports.indexOf(hash_table.get(current_port_hash)));
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, send2);
			}
			else {
				//if i am not one of the actual node or next node or a next to next node of the key
				Log.d("insert method", " The string being sent for insertion is---->" + send2.toString());
				//the ports will have all the values of the active list
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, send2);

			}

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		//dbHelper = new FeedReaderDbHelper(getContext());
		//db = dbHelper.getWritableDatabase();
		Log.d("create method"," *********NOW INSIDE ONCREATE METHOD************");

		FeedReaderDbHelper dbHelper = new FeedReaderDbHelper(getContext());
		SQLiteDatabase db = dbHelper.getReadableDatabase();

		/*dbHelper = new FeedReaderDbHelper(getContext());
		db = dbHelper.getReadableDatabase();*/

		Context context = getContext();
		TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));


		//storing the current hash value of the incoming port in the variable current_port_hash
		try {
			current_port_hash = genHash(portStr);
			Log.d("in oncreate method", "the port is--->" + portStr);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		//as already the final_small_ports list is sorted the hash of ports will have the hashes in order
		for (String repo : final_ports_small) {
			try {
				hash_table.put(genHash(repo), repo);

			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}
		Log.d("on create method"," hash table values--->"+hash_table);
		//write recovery task below
		//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, send_successors);

		try {
			Log.d("in oncreate method", "reached calling the server");
			//calling it's own server
			serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			e.printStackTrace();
			//if server connection fails
		}

		//when an avd is initialized get its successor and predecessor

		//String suc_ans = cmp(hash_table,portStr);
		String suc_ans=suc_ans(current_port_hash);
		String[] suc_and_ans = suc_ans.split(":");
		String successor = suc_and_ans[0];
		String ancestor = suc_and_ans[1];

		Log.d("create method"," The successor is--->"+successor+" and ancestor is---->"+ancestor);

		HashMap suc_ans_details = new HashMap();

		//change the code below

		Shashank recover = new Shashank();

		recover.setAction("recover");
		recover.setSelection("@");
		recover.active_ports.add(successor);
		recover.active_ports.add(ancestor);
		try {
			HashMap maps_from_recover = new Recover().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, recover).get();

			//this should have combined data from the
			suc_ans_details.putAll(maps_from_recover);

		} catch (InterruptedException el) {
			el.printStackTrace();
		} catch (ExecutionException el) {
			el.printStackTrace();
		}



		//change the code below
		Log.d("on create method"," The size: "+suc_ans_details.size()+" The contents of the hashmap is---->"+suc_ans_details);

		for (Object j : suc_ans_details.keySet()) {
			try {
				Log.d("on create method"," The key for which replicas are being obtained is--->"+j.toString());
				String hashKey = genHash(j.toString());
				String correctNode, replica1, replica2;
				String nodes = getNodes(hashKey);
				String[] reps = nodes.split(":");

				correctNode = reps[0];
				replica1 = reps[1];
				replica2 = reps[2];

				//if there are some values that had to be inserted and had been missed insert in my own avd
				if (correctNode.equals(current_port_hash) || replica1.equals(current_port_hash) || replica2.equals(current_port_hash)) {
					Log.d("on create method","The value of the current port hash is---->"+current_port_hash);
					Log.d("on create method"," The hash is equal to one of the checkings");
					ContentValues values = new ContentValues();
					values.put("key", j.toString());
					values.put("value", suc_ans_details.get(j).toString());
					Log.d("on create method"," The contents inserted are----->"+values);
					db.insertWithOnConflict(FeedReaderContract.FeedEntry.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
				}
			} catch (NoSuchAlgorithmException elm) {
				elm.printStackTrace();
			}
		}
		return false;

	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub

		Log.d("query method"," *********NOW INSIDE QUERY METHOD**********");
		FeedReaderDbHelper dbHelper = new FeedReaderDbHelper(getContext());
		SQLiteDatabase db = dbHelper.getReadableDatabase();

		/*db = dbHelper.getReadableDatabase();*/

		try {
			hash_of_selection = genHash(selection);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		String[] columns = {"key", "value"};
		//Cursor cursor = db.query(FeedReaderContract.FeedEntry.TABLE_NAME, columns, null, null, null, null, null);

		Log.d("In query method", "The selection string is --->" + selection);

		if(selection.equals("@")){
			Log.d("query method"," queried here");
			Cursor cursor = db.query(FeedReaderContract.FeedEntry.TABLE_NAME, columns, null, null, null, null, null);
			return cursor;

		}
		else if(selection.equals("*")){
			Log.d("query method"," The query being fetched everywhere");
			Shashank in_query = new Shashank();
			in_query.setAction("query");
			in_query.setSelection(selection);
			in_query.setOriginal_avd(portStr);
			MergeCursor mergeCursor;
			try {

				Cursor hash = new QueryClient().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, in_query).get();
				//Log.d("query method"," The hash value obtained from the query client is--->"+query.toString());
				Cursor cursor = db.query(FeedReaderContract.FeedEntry.TABLE_NAME, columns, null, null, null, null, null);
				mergeCursor = new MergeCursor(new Cursor[]{cursor, hash});
				return mergeCursor;

			}
			catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}

		}
		else {
			//for a specific key
			try{
			Log.d("query method", " Querying for a particular key");

			//String port = String.valueOf(return_index(hash_of_selection));
			//String port = getLocation(hash_of_selection);

			String query_coordinator = getNodes(hash_of_selection);
			String[] query_node = query_coordinator.split(":");
			String cordinator = query_node[0];
			String next_node = query_node[1];
			String next_to_next_node = query_node[2];

			String[] columnsn = {"key", "value"};
			String searchClause = "key" + " = ?";
			String[] searchQuery = {selection};


			Shashank in_query = new Shashank();
			in_query.setAction("query");
			in_query.setPort(hash_table.get(cordinator));
			in_query.setSelection(selection);
			in_query.active_ports.add(hash_table.get(cordinator));
			in_query.active_ports.add(hash_table.get(next_node));
			in_query.active_ports.add(hash_table.get(next_to_next_node));


			if (cordinator.equals(current_port_hash) || next_node.equals(current_port_hash) || next_to_next_node.equals(current_port_hash)) {
				Cursor cursor = db.query(FeedReaderContract.FeedEntry.TABLE_NAME, columnsn, searchClause, searchQuery, null, null, null);
				//Log.v("query", selection);
				if(cursor.getCount() != 0){
					return cursor;
				}else{
					in_query.active_ports.remove(in_query.active_ports.indexOf(hash_table.get(current_port_hash)));
					Cursor curso = new QueryClient().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, in_query).get();
					return curso;

				}
			}
			else{
				Log.v("Query", in_query.selection);
				Cursor cursor = new QueryClient().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, in_query).get();
				return cursor;

			}

			} catch (InterruptedException e) {
					e.printStackTrace();
			} catch (ExecutionException e) {
					e.printStackTrace();
			}

		}

		return null;
	}



	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
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

			try {
				while (true) {
					Socket socket = serverSocket.accept();
					try {


						ObjectInputStream ob_in_str = new ObjectInputStream(socket.getInputStream());
						Shashank input_msg = (Shashank) ob_in_str.readObject();
						Log.d("server method"," The input message is---->"+input_msg.toString());

						ObjectOutputStream ob_out_str = new ObjectOutputStream(socket.getOutputStream());


						Log.d("Server side:", "********ENTERED SERVER METHOD*******");


						if (input_msg.getAction().equals("insert")) {
							//Log.d("Server side", "called from the insert method" + " the node where the is being done is--->" + input_msg.getPort());
							FeedReaderDbHelper dbHelper = new FeedReaderDbHelper(getContext());
							SQLiteDatabase db = dbHelper.getWritableDatabase();

							String key = input_msg.getKey();
							String values = input_msg.getValue();
							Log.d(TAG, key);
							Log.d(TAG, values);

							Uri.Builder uriBuilder = new Uri.Builder();
							uriBuilder.authority("edu.buffalo.cse.cse486586.simpledynamo");
							uriBuilder.scheme("content");
							ContentValues cv = new ContentValues();
							cv.put("key", key);
							cv.put("value", values);


							myUri = uriBuilder.build();
							Log.d("server side", " The values being inserted is---->" + cv);
							db.insertWithOnConflict(FeedReaderContract.FeedEntry.TABLE_NAME, null, cv, SQLiteDatabase.CONFLICT_IGNORE);

							//db.insertWithOnConflict(FeedReaderContract.FeedEntry.TABLE_NAME, null, cv, SQLiteDatabase.CONFLICT_REPLACE);

						} else if (input_msg.getAction().equals("query")) {
							String selection = input_msg.getSelection();
							FeedReaderDbHelper dbHelper = new FeedReaderDbHelper(getContext());
							SQLiteDatabase db = dbHelper.getReadableDatabase();
							/*dbHelper = new FeedReaderDbHelper(getContext());
							db = dbHelper.getReadableDatabase();*/

							String[] columns = {"key", "value"};
							String searchClause = "key" + " = ?";
							String[] searchQuery = {selection};


							if (input_msg.getSelection().equals("*")) {
								Cursor cursor = db.rawQuery("Select * from " + FeedReaderContract.FeedEntry.TABLE_NAME, null);

								HashMap cursorMap = new HashMap();
								if (cursor.moveToFirst()) {
									do {
										String cursorKey = cursor.getString(cursor.getColumnIndex("key"));
										String cursorValue = cursor.getString(cursor.getColumnIndex("value"));
										cursorMap.put(cursorKey, cursorValue);
									} while (cursor.moveToNext());
								}
								cursor.close();
								ob_out_str.writeObject(cursorMap);
								ob_out_str.flush();

							} else {

								Cursor cursor = db.query(FeedReaderContract.FeedEntry.TABLE_NAME, columns, searchClause, searchQuery, null, null, null);
								//Log.v("query", selection);
								HashMap cursorMap = new HashMap();
								if (cursor.moveToFirst()) {
									do {
										String cursorKey = cursor.getString(cursor.getColumnIndex("key"));
										String cursorValue = cursor.getString(cursor.getColumnIndex("value"));
										cursorMap.put(cursorKey, cursorValue);
									} while (cursor.moveToNext());
								}
								cursor.close();
								ob_out_str.writeObject(cursorMap);
								ob_out_str.flush();

							}


						} else if (input_msg.getAction().equals("delete")) {
							Log.d("server side:", " Inside the delete method");
							FeedReaderDbHelper dbHelper = new FeedReaderDbHelper(getContext());
							SQLiteDatabase db = dbHelper.getWritableDatabase();
							/*dbHelper = new FeedReaderDbHelper(getContext());
							db = dbHelper.getWritableDatabase();*/
							db.execSQL("DELETE FROM " + FeedReaderContract.FeedEntry.TABLE_NAME);
							//db.delete(FeedReaderContract.FeedEntry.TABLE_NAME, null,null);

						} else if (input_msg.getAction().equals("recover")) {
							Log.d("Server side", " Inside the recover method");

							FeedReaderDbHelper dbHelper = new FeedReaderDbHelper(getContext());
							SQLiteDatabase db = dbHelper.getReadableDatabase();
							/*dbHelper = new FeedReaderDbHelper(getContext());
							db = dbHelper.getReadableDatabase();*/
							Cursor cursor = db.query(FeedReaderContract.FeedEntry.TABLE_NAME, null, null, null, null, null, null);
							HashMap cursorMap = new HashMap();
							if(cursor.moveToFirst()){
								do{
									String cursorKey = cursor.getString(cursor.getColumnIndex("key"));
									String cursorValue = cursor.getString(cursor.getColumnIndex("value"));
									cursorMap.put(cursorKey,cursorValue);
								}while (cursor.moveToNext());
							}
							cursor.close();

							//will be returning a hashmap to the recover task
							Log.d("Server recovery","Sending query back from recover"+input_msg.getQuery_table());
							ob_out_str.writeObject(cursorMap);
							ob_out_str.flush();
						}

					}
					catch (IOException e) {
						//System.out.println("Socket connection timed out");
						Log.d("server side", " caught exception");
						e.printStackTrace();
					} catch (ClassNotFoundException e) {
						Log.e("Class", "ClassNotFoundException");

					}

				}
			}
			catch (Exception e) {
				e.printStackTrace();
				Log.d("Server side", "caught in an exception");
			}

			return null;
		}

	}

	private class ClientTask extends AsyncTask<Shashank, Void, Void> {

		@Override
		protected Void doInBackground(Shashank... msgs) {


			try {
				Log.d("Client side:", " **********ENTERED CLIENT METHOD********");
				//creating new instance of that class(to get values that are updated
				Shashank in_client = msgs[0];
				Socket socket;

				if(in_client.getAction().equals("insert")) {
					Log.d("client task", " entered insertion");
					if (in_client.active_ports.size() == 3) {
						//if the request came from the else condition of the insert method
						Log.d("client side"," entered where array list is 3");

						for (String repo : in_client.active_ports) {
							try{
								int port = Integer.parseInt(repo) * 2;
								socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
										Integer.parseInt(String.valueOf(port)));
								Log.d("client side:", "port being sent " + repo + " key before sending--->" + in_client.getKey() + " value--->" + in_client.getValue());

								//sending the successors and ancestors for each nodes back to its servers
								ObjectOutputStream ob_out_str = new ObjectOutputStream(socket.getOutputStream());
								ob_out_str.writeObject(in_client);
								ob_out_str.flush();
							}catch (Exception e){
								Log.d("Inside insert1 client", ""+e.getMessage());
								e.printStackTrace();
							}

						}
					} else if (in_client.active_ports.size() == 2) {
						Log.d("client side"," entered where array list is 2");
						//if the request came from the if condition of the insert method
						for (String repo :in_client.active_ports) {

							try{
								int port = Integer.parseInt(repo) * 2;
								socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
										Integer.parseInt(String.valueOf(port)));
								Log.d("client side:", "port being sent " + repo + " key before sending--->" + in_client.getKey() + " value--->" + in_client.getValue());

								//sending the successors and ancestors for each nodes back to its servers
								ObjectOutputStream ob_out_str = new ObjectOutputStream(socket.getOutputStream());
								ob_out_str.writeObject(in_client);
								ob_out_str.flush();

							}catch (Exception e){
								Log.d("Inside insert2 clien", ""+e.getMessage());
								e.printStackTrace();
							}

						}
					}
				}
				else if(in_client.getAction().equals("delete")){
					Log.d("client method"," entered deletion");

					for(String repo:in_client.active_ports){
						try{
							if(repo.equals(portStr)){
								continue;
							}
							else{
								int port = Integer.parseInt(repo)*2;
								socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
										Integer.parseInt(String.valueOf(port)));

								//sending the successors and ancestors for each nodes back to its servers
								ObjectOutputStream ob_out_str= new ObjectOutputStream(socket.getOutputStream());
								ob_out_str.writeObject(in_client);
								ob_out_str.flush();

							}
						}catch (Exception e){
							Log.d("Inside delete clientask", ""+e.getMessage());
							e.printStackTrace();
						}

					}
				}

			} catch (NullPointerException e) {
				Log.e(TAG, "Null pointer exception caught");
				System.out.print("NullPointerException Caught");
			}

			Log.d("Client side:", " reached the end of the client method");
			return null;

		}

	}

	private class Recover extends AsyncTask<Shashank, Void, HashMap> {

		@Override
		protected HashMap doInBackground(Shashank... shashanks) {

			Shashank in_recover = shashanks[0];
			Log.d("recover method"," The contents received here is--->"+in_recover.toString());
			HashMap hashq = new HashMap();

			Socket socket;
			try{
				HashMap[] maps = new HashMap[2];
				int i=0;
				for(String s:in_recover.active_ports){
					try {
						int port = Integer.parseInt(s) * 2;
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(String.valueOf(port)));
						Log.d("recover method", " The port that will be binded is--->" + port);
						//sending the successors and ancestors for each nodes back to its servers
						ObjectOutputStream ob_out_str = new ObjectOutputStream(socket.getOutputStream());
						ob_out_str.writeObject(in_recover);
						ob_out_str.flush();

						ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

						hashq = (HashMap) in.readObject();
						maps[i] = hashq;
						i++;

						Log.d("recover method:", "The size is--->" + hashq.size() + " The hash table received is--->" +hashq);
					}
					catch (Exception ex){
						Log.d("Inside for of recovery", ""+ex.getMessage());
						ex.printStackTrace();
					}
				}
				HashMap h = new HashMap(maps[0]);
				h.putAll(maps[1]);
				Log.d("on recover client"," The size: "+h.size()+" The contents of the hashmap is---->"+h);
				return h;

			}catch (NullPointerException n){
				Log.e(TAG, "Null pointer exception caught");
				System.out.print("NullPointerException Caught");
			}


			Log.d("recover method","THE FINAL SIZE OF RECOVERED HASHMAP IS---->"+hashq.size()+" THE CONTENTS AFTER RECOVERING ARE---->"+hashq);

			return hashq;

		}
	}



	private class QueryClient extends AsyncTask<Shashank, Void, Cursor> {


		@Override
		protected Cursor doInBackground(Shashank... shashanks) {

			HashMap<String, String> hash_query;
			Socket socket;

			Shashank in_query = shashanks[0];
			MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
			try {
				if (in_query.getAction().equals("query")) {
					if(in_query.selection.equals("*")){
						Log.d("query Client side", "Now in the query method of the client" + " the incoming request message is---->" + in_query.toString());
						for (String repo : final_ports_small) {

							try{
								if (repo.equals(portStr)) {
									continue;
								} else {
									int rep = Integer.parseInt(repo) * 2;
									socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
											Integer.parseInt(String.valueOf(rep)));

									//sending the successors and ancestors for each nodes back to its servers
									ObjectOutputStream ob_out_str_in_query_client = new ObjectOutputStream(socket.getOutputStream());
									ob_out_str_in_query_client.writeObject(in_query);
									ob_out_str_in_query_client.flush();

									//get the cursor first
									ObjectInputStream ob_in_str_for_cursor = new ObjectInputStream(socket.getInputStream());
									hash_query = (HashMap) ob_in_str_for_cursor.readObject();

									for(Object j: hash_query.keySet()){
										Log.v("Query returned",j.toString());
										matrixCursor.addRow(new Object[] {j, hash_query.get(j)});
									}

								}

							}catch (Exception exm){
								Log.d("Inside for query client", ""+exm.getMessage());
								exm.printStackTrace();
							}

						}

					}
					else{
						for(String repo:in_query.active_ports){
							try{

								int rep = Integer.parseInt(repo) * 2;
								socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
										Integer.parseInt(String.valueOf(rep)));

								//sending the successors and ancestors for each nodes back to its servers
								ObjectOutputStream ob_out_str_in_query_client = new ObjectOutputStream(socket.getOutputStream());
								ob_out_str_in_query_client.writeObject(in_query);
								ob_out_str_in_query_client.flush();

								//get the cursor first
								ObjectInputStream ob_in_str_for_cursor = new ObjectInputStream(socket.getInputStream());
								hash_query = (HashMap) ob_in_str_for_cursor.readObject();

								Log.d("query client "," The hash map received for a specific key is");

								for(Object j: hash_query.keySet()){
									Log.v("Query returned",j.toString());
									matrixCursor.addRow(new Object[] {j, hash_query.get(j)});
								}

								if(!hash_query.isEmpty()){
									Log.d("query client method"," The hash table is not empty");
								}

							}catch (Exception e){
								Log.d("Inside for query client", ""+e.getMessage());
								e.printStackTrace();

							}

						}
					}

				}

			}catch (Exception e){
				Log.d("Inside for query client", ""+e.getMessage());
				e.printStackTrace();
			}
			matrixCursor.moveToFirst();
			return matrixCursor;
		}
	}


	//returns the coordinator, replica1 and replica2 of the model
	public String getNodes(String hashKey){
		String correctNode = "";
		String replica1 = "";
		String replica2 = "";

		if (hash_table.ceilingKey(hashKey) == null) {
			correctNode = hash_table.firstKey();
		} else {
			correctNode = hash_table.ceilingKey(hashKey);
		}
		if (hash_table.higherKey(correctNode) == null) {
			replica1 = hash_table.firstKey();
		} else {
			replica1 = hash_table.higherKey(correctNode);
		}
		if (hash_table.higherKey(replica1) == null) {
			replica2 = hash_table.firstKey();
		} else {
			replica2 = hash_table.higherKey(replica1);
		}

		String msg_to_be_sent = correctNode+":"+replica1+":"+replica2;
		Log.d("inside get nodes method"," The correct node is---->"+correctNode+" the replica 1 is---->"+replica1+" the replica 2 is--->"+replica2);
		return msg_to_be_sent;

	}

	//returns the index of the partition in the hash table
	public String suc_ans(String myhash){
		String succ, pred;
		if(hash_table.higherKey(myhash) == null){
			succ = hash_table.firstKey();
		}
		else{
			succ = hash_table.higherKey(myhash);
		}

		if(hash_table.lowerKey(myhash) == null){
			pred = hash_table.lastKey();
		}else{
			pred = hash_table.lowerKey(myhash);
		}

		//String send_back = succ+":"+pred;
		String successor = hash_table.get(succ);
		String ancestor = hash_table.get(pred);
		Log.d("successor method"," The value of the successo is--->"+successor+" the ancestor is--->"+ancestor);
		String msg_back = successor+":"+ancestor;
		return msg_back;

	}

	//return the first and second successor
	public String successors(String port){
		Log.d("SUCCESSOR CHECKER","***********NOW CHECKING FOR THE PORT**********"+port);
		String successors = "";
		String first_successor = "";
		String second_successor = "";


		if(port.equals("5562")){
			first_successor = "5556";
			second_successor = "5554";
		}
		else if(port.equals("5556")){
			first_successor = "5554";
			second_successor = "5558";

		}
		else if(port.equals("5554")){
			first_successor = "5558";
			second_successor = "5560";

		}
		else if(port.equals("5558")){
			first_successor = "5560";
			second_successor = "5562";
		}
		else if(port.equals("5560")){
			first_successor = "5562";
			second_successor = "5556";
		}

		successors = first_successor+":"+second_successor;
		return successors;
	}

	public String getLocation(String key){
		String[] nodes = hash_table.keySet().toArray(new String[5]);
		int port=0;
		for(int i=0;i<nodes.length-1;i++){
			if(key.compareTo(nodes[i])>0 && key.compareTo(nodes[i+1])<=0){
				port = i+1;
				break;
			}
		}
		if(port==nodes.length){
			port=0;

		}
		return hash_table.get(nodes[port]);
	}

	public String cmp(TreeMap<String, String> hash_table, String port) {
		String successor = "_";
		String ancestor = "_";
		String msg_for_broadcast = "_";
		//calculate the suc and ans of 5554 and update it here
		try {
			if (hash_table.higherKey(genHash(port)) != null) {
				Log.d("Server side:", " entered hashkey check");
				Map.Entry<String, String> suc = hash_table.higherEntry(genHash(port));
				successor = suc.getValue();

			} else {
				Map.Entry<String, String> suc = hash_table.firstEntry();
				successor = suc.getValue();

			}

			//checking for the first node
			if (hash_table.lowerKey(genHash(port)) != null) {
				Log.d("Server side:", " entered hashkey check");
				Map.Entry<String, String> ans = hash_table.lowerEntry(genHash(port));
				ancestor = ans.getValue();
			} else {
				Map.Entry<String, String> ans = hash_table.lastEntry();
				ancestor = ans.getValue();

			}
			msg_for_broadcast = port + ":" + successor + ":" + ancestor + ":" + "broadcast";

		} catch (NoSuchAlgorithmException e) {
			Log.d("In comparator", "Entered the exception");
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
			public static final String COLUMN_VERSION = "version";

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
		public static final String COLUMN_VERSION = "version";





		public FeedReaderDbHelper(Context context) {
			super(context, DATABASE_NAME, null, DATABASE_VERSION);// (int)(Math.random()*1000));
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
	private static final long serialVersionUID = -299482035708790407L;
	String action;
	String port;
	String successor;
	String ancestor;
	String original_avd;
	String status;
	String key;
	String value;
	String selection;
	String first_successor;
	String second_successor;
	String first_ancestor;
	String second_ancestor;


	Cursor cursor;

	ArrayList<String> active_ports = new ArrayList<String>();
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
				", first_successor='" + first_successor + '\'' +
				", second_successor='" + second_successor + '\'' +
				", first_ancestor='" + first_ancestor + '\'' +
				", second_ancestor='" + second_ancestor + '\'' +
				", cursor=" + cursor +
				", active_ports=" + active_ports +
				", query_table=" + query_table +
				'}';
	}

	public String getFirst_ancestor() {
		return first_ancestor;
	}

	public void setFirst_ancestor(String first_ancestor) {
		this.first_ancestor = first_ancestor;
	}

	public String getSecond_ancestor() {
		return second_ancestor;
	}

	public void setSecond_ancestor(String second_ancestor) {
		this.second_ancestor = second_ancestor;
	}


	public String getFirst_successor() {
		return first_successor;
	}

	public void setFirst_successor(String first_successor) {
		this.first_successor = first_successor;
	}

	public String getSecond_successor() {
		return second_successor;
	}

	public void setSecond_successor(String second_successor) {
		this.second_successor = second_successor;
	}



	public HashMap<String, String> getQuery_table() {
		return query_table;
	}

	public void setQuery_table(HashMap<String, String> query_table) {
		this.query_table = query_table;
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



