package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.Context.MODE_PRIVATE;

public class SimpleDynamoProvider extends ContentProvider {

	static final int SERVER_PORT = 10000;
	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";
	static final String REMOTE_PORT0_EMULATOR = "5554";
	static final String REMOTE_PORT1_EMULATOR = "5556";
	static final String REMOTE_PORT2_EMULATOR = "5558";
	static final String REMOTE_PORT3_EMULATOR = "5560";
	static final String REMOTE_PORT4_EMULATOR = "5562";
	static final String PROVIDER_URI = "edu.buffalo.cse.cse486586.simpledynamo.provider";
	public String predecessor = null;
	public String predecessor_id = null;
	public String successor_id = null;
	public String successor = null;
	public String id = null;
	String hash1 = null;
	public String self_port = null;
	HashMap<String,Node> hash_Map = new HashMap<String,Node>();
	public Lock lock = new ReentrantLock();
	public Lock lock1 = new ReentrantLock();



	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		String key = null;
		try {
			key = genHash(selection);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}


		if(
				(key.compareTo(id)<=0&&key.compareTo(predecessor_id)>0)||
				(key.compareTo(id)<=0&&id.compareTo(predecessor_id)<0)||
				(key.compareTo(id)>0&&id.compareTo(predecessor_id)<0&&key.compareTo(predecessor_id)>0)
		) {
//			File file = new File(getContext().getFilesDir(),selection);
//			file.delete();
			deleteFromKey(selection,"local");
			try {
				new ClientTask1().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "delete",selection,hash1,"2").get();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}

		}
		return 0;
	}

	public synchronized void deleteFromKey(String selection,String port){
		Log.v(" debug delete"," key is "+selection+ " port is "+ port);
		File path = new File(getContext().getFilesDir(), port);
		File file = new File(path,selection);
		file.delete();
	}


	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		String key = (String) values.get("key");
		String key1 =key;
		String val = values.get("value") + "\n";
		Log.v(" inserting key is  ", key1);

		try {
			key = genHash(key);
			Log.v(" insert hash ", key);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		for(Map.Entry mapElement:hash_Map.entrySet()){
			Node node = (Node)mapElement.getValue();
			if((key.compareTo(node.id)<=0&&key.compareTo(node.predeccessor_id)>0)||
					(key.compareTo(node.id)<=0&&node.id.compareTo(node.predeccessor_id)<0)||
					(key.compareTo(node.id)>0&&node.id.compareTo(node.predeccessor_id)<0&&
							key.compareTo(node.predeccessor_id)>0)){
					Log.v(" insert log"," I am here for value  "+key1+ " for node "+ node.my_port+
							" with hash "+key);
					sendToNode(key1,val,node);
			}

		}
		return null;
	}
	public void sendToNode(String key1, String values, Node node){
		try {
			String key = genHash(key1);
			Log.v("sendToNode"," id is "+id+ " node is "+node.id);
			new ClientTask1().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert",
					key1,values,node.my_port,"2").get();
		}
		catch (Exception ex){

		}
	}

	public MatrixCursor sendToNodeForQuery(String key1, Node node){
		try {
			String key = genHash(key1);
//			if(id.equals(node.id)){
//				MatrixCursor resultCursor = new MatrixCursor(new String[]{"key", "value"});
//				Log.v("query debug"," key is "+ key1);
//				resultCursor = getResults(key1,resultCursor,key1);
//				return resultCursor;
//			}
			List<QueryObject> queryObjects = new ClientTask1().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query",key1,node.my_port).get();
			QueryObject queryObject = queryObjects.get(0);
			MatrixCursor resultCursor = new MatrixCursor(new String[]{"key", "value"});
			resultCursor.newRow().add("key", queryObject.getKey()).add("value", queryObject.getValue().trim());
			return resultCursor;
		}
		catch (Exception ex){
			return null;
		}
	}

	public class Node{
		public String successor;
		public String successor_port;
		public String predeccessor;
		public String predeccessor_port;
		public String my_port;
		public String successor_id;
		public String predeccessor_id;
		public String id;
		public Node(String my_port,String successor, String predeccessor,
					String successor_port, String predeccessor_port){
			try {
				this.id = genHash(my_port);
				this.successor_id = genHash(successor);
				this.predeccessor_id = genHash(predeccessor);
				this.successor = successor;
				this.predeccessor = predeccessor;
				this.my_port = my_port;
				this.successor_port = successor_port;
				this.predeccessor_port = predeccessor_port;
			}
			catch (Exception ex){

			}
		}
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		hash1 = String.valueOf((Integer.parseInt(portStr)));
		Log.v(" port is "," id is string "+hash1);

		hash_Map.put(REMOTE_PORT0_EMULATOR,
				new Node(REMOTE_PORT0_EMULATOR,REMOTE_PORT2_EMULATOR,REMOTE_PORT1_EMULATOR,
						REMOTE_PORT2,REMOTE_PORT1));
		hash_Map.put(REMOTE_PORT1_EMULATOR,
				new Node(REMOTE_PORT1_EMULATOR,REMOTE_PORT0_EMULATOR,REMOTE_PORT4_EMULATOR,
				REMOTE_PORT0,REMOTE_PORT4));
		hash_Map.put(REMOTE_PORT2_EMULATOR,
				new Node(REMOTE_PORT2_EMULATOR,REMOTE_PORT3_EMULATOR,REMOTE_PORT0_EMULATOR,
						REMOTE_PORT3,REMOTE_PORT0));
		hash_Map.put(REMOTE_PORT3_EMULATOR,
				new Node(REMOTE_PORT3_EMULATOR,REMOTE_PORT4_EMULATOR,REMOTE_PORT2_EMULATOR,
						REMOTE_PORT4,REMOTE_PORT2));
		hash_Map.put(REMOTE_PORT4_EMULATOR,
				new Node(REMOTE_PORT4_EMULATOR,REMOTE_PORT1_EMULATOR,REMOTE_PORT3_EMULATOR,
						REMOTE_PORT1,REMOTE_PORT3));
		try {
			id = genHash(hash1);
			predecessor = hash_Map.get(hash1).predeccessor;
			successor = hash_Map.get(hash1).successor;
			predecessor_id = hash_Map.get(hash1).predeccessor_id;
			successor_id = hash_Map.get(hash1).successor_id;
			Log.v("hash is", " my emulator is "+hash1+ " with id ie hash "+id);
			self_port = myPort;
			Log.v("test", " my port is "+ self_port+ " my hash id is "+ id);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		File path = new File(getContext().getFilesDir(), "local");
		if (!path.exists()) {
			path.mkdirs();
		}
		File path1 = new File(getContext().getFilesDir(), predecessor);
		if (!path1.exists()) {
			path1.mkdirs();
//					}
		}
		Node node1 = hash_Map.get(predecessor);
		File path2 = new File(getContext().getFilesDir(), node1.predeccessor);
		if (!path2.exists()) {
			path2.mkdirs();
//					}
		}





		File[] files = path.listFiles();


//			File[] files = getContext().getFilesDir().listFiles();

		for (File file : files) {
			Log.v(" chala"," kuch toh chal "+file+ " with key as "+ file.getName());
			deleteFromKey(file.getName(),"local");
//                Log.v("query check is", " checking result cursor log "+resultCursor.getCount());
		}

		Node node = hash_Map.get(hash1);

		File file2 = new File(getContext().getFilesDir(),node.predeccessor);
		Log.v(" debug node yahan",node.successor);
		if(file2!=null) {
			File[] files2 = file2.listFiles();
			for (File file : files2) {
				Log.v("final", node.successor);
				deleteFromKey(file.getName(),node.predeccessor);
			}
		}

		Node node3 = hash_Map.get(node.predeccessor);

		File file3 = new File(getContext().getFilesDir(),node3.predeccessor);
		if(file3!=null) {
			File[] files3 = file3.listFiles();
			Log.v(" bich ka", "working");
			for (File file : files3) {
				deleteFromKey(file.getName(), node3.predeccessor);
			}
			Log.v(" debug node yahan 1", node.my_port);

		}





//		lock.lock();
		try {
			Log.v(" working test"," inside the replica part");
			new ClientTask1().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
					"replica",hash1);

		} catch (Exception e) {
			Log.v(" phatega"," yuss");
			e.printStackTrace();
		}
		Log.v("yahan sai hai","rocking");
		try {
			 new ClientTask1().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
					"preplica",hash1);
		} catch (Exception ex){

		}
//		lock.unlock();

		for(Map.Entry mapElement:hash_Map.entrySet()){
			Node node5 = (Node)mapElement.getValue();
			Log.v(" debug Join"," node key is "+ node5.id+" node is "+node5.my_port+" node successor is "+ node5.successor_port+
					" node predecessor is "+ node5.predeccessor_port);
		}
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e("test", "Can't create a ServerSocket");
			Log.v("error", "testing "+e.getMessage());
			return false;
		}
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
//		lock1.lock();
		String key = null;
		try {
			key = genHash(selection);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		Log.v("query"," key is "+selection);
		Log.v("query"," hash of key is "+key);

		QueryObject queryObject = null;
		MatrixCursor resultCursor = new MatrixCursor(new String[]{"key", "value"});
		if(selection.equals("@")) {
//			File directory = new File(getContext().getFilesDir()+"/"+"local");
			File path = new File(getContext().getFilesDir(), "local");
			File[] files = path.listFiles();


//			File[] files = getContext().getFilesDir().listFiles();

			for (File file : files) {
				Log.v(" chala"," kuch toh chal "+file+ " with key as "+ file.getName());
				resultCursor = getResults(file.getName(), resultCursor, "local");
//                Log.v("query check is", " checking result cursor log "+resultCursor.getCount());
			}

			Node node = hash_Map.get(hash1);

			File file2 = new File(getContext().getFilesDir(),node.predeccessor);
			Log.v(" debug node yahan",node.successor);
			if(file2!=null) {
				File[] files2 = file2.listFiles();

//		File[] files = getContext().getFilesDir().listFiles();
				for (File file : files2) {
//                Log.v("query check is", " checking result cursor log "+resultCursor.getCount());
					Log.v("final", node.successor);
					resultCursor = getResults(file.getName(), resultCursor, node.predeccessor);

				}
			}

			Node node1 = hash_Map.get(node.predeccessor);

			File file3 = new File(getContext().getFilesDir(),node1.predeccessor);
			if(file3!=null) {
				File[] files3 = file3.listFiles();
//		File[] files = getContext().getFilesDir().listFiles();
				Log.v(" bich ka", "working");
				for (File file : files3) {
//                Log.v("query check is", " checking result cursor log "+resultCursor.getCount());
					resultCursor = getResults(file.getName(), resultCursor, node1.predeccessor);

				}
				Log.v(" debug node yahan 1", node.my_port);
			}
			return resultCursor;
		}
		if(selection.equals("*")){
			try {
				List<QueryObject> queryObjects = new ClientTask1().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query*",selection).get();

				for(QueryObject queryObject1: queryObjects){
					resultCursor.newRow().add("key", queryObject1.getKey()).add("value",
					queryObject1.getValue().trim());
				}
				return resultCursor;

			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}

		}
		for(Map.Entry mapElement:hash_Map.entrySet()){
			Node node = (Node)mapElement.getValue();
			if((key.compareTo(node.id)<=0&&key.compareTo(node.predeccessor_id)>0)||
					(key.compareTo(node.id)<=0&&node.id.compareTo(node.predeccessor_id)<0)||
					(key.compareTo(node.id)>0&&node.id.compareTo(node.predeccessor_id)<0&&
							key.compareTo(node.predeccessor_id)>0)){
				resultCursor = sendToNodeForQuery(selection,node);
				return resultCursor;
			}

		}

		return null;
	}

	public MatrixCursor getResults(String key,MatrixCursor resultCursor,String port)  {
		Log.v("getResults","key is "+key);
		String msg = "";
		msg = queryHelper(key,msg,port);
		Log.v("query12345", "yahan toh chal " + key+ " value is "+msg);
		resultCursor.newRow().add("key", key).add("value", msg.trim());
		return resultCursor;
	}

	public List<QueryObject> getAllFiles(){
		List<QueryObject> queryObjects = new ArrayList<QueryObject>();
		File file1 = new File(getContext().getFilesDir(),"local");
		File[] files = file1.listFiles();
//		File[] files = getContext().getFilesDir().listFiles();
		for (File file : files) {
//                Log.v("query check is", " checking result cursor log "+resultCursor.getCount());
			String value = queryHelper(file.getName(),"","local");
			QueryObject queryObject = new QueryObject(file.getName(),value);
			queryObjects.add(queryObject);
		}

		Node node = hash_Map.get(hash1);

		File file2 = new File(getContext().getFilesDir(),node.predeccessor);
		File[] files2 = file2.listFiles();
//		File[] files = getContext().getFilesDir().listFiles();
		for (File file : files2) {
//                Log.v("query check is", " checking result cursor log "+resultCursor.getCount());
			String value = queryHelper(file.getName(),"",node.predeccessor);
			QueryObject queryObject = new QueryObject(file.getName(),value);
			queryObjects.add(queryObject);
		}

		Node node1 = hash_Map.get(node.predeccessor);

		File file3 = new File(getContext().getFilesDir(),node1.predeccessor);
		File[] files3 = file3.listFiles();
//		File[] files = getContext().getFilesDir().listFiles();
		for (File file : files3) {
//                Log.v("query check is", " checking result cursor log "+resultCursor.getCount());
			String value = queryHelper(file.getName(),"",node1.predeccessor);
			QueryObject queryObject = new QueryObject(file.getName(),value);
			queryObjects.add(queryObject);
		}

		return queryObjects;
	}

	public List<QueryObject> getFilesForFolder(String port){
		List<QueryObject> queryObjects = new ArrayList<QueryObject>();
		File file1 = new File(getContext().getFilesDir(),port);
		File[] files = file1.listFiles();
		for (File file : files) {
			String value = queryHelper(file.getName(),"",port);
			QueryObject queryObject = new QueryObject(file.getName(),value);
			queryObjects.add(queryObject);
		}
		return queryObjects;
	}

	public String queryHelper(String key,String msg,String port)  {
		try{

			File file = new File(getContext().getFilesDir(),port);
			file = new File(file,key);
			StringBuilder s = new StringBuilder();
			try{
				BufferedReader b = new BufferedReader(new FileReader(file));
				String l = null;
				while ((l = b.readLine())!=null){
					s.append(l);
					s.append("\n");
				}
				b.close();
			}catch (IOException ex){
				return null;
			}
			Log.v(" result aya"," result is "+s.toString());
			return s.toString();
//			InputStream inputStream = getContext().openFileInput(key);
//			InputStreamReader InputRead = new InputStreamReader(inputStream);
//
//			char[] buffer = new char[100];
//
//			int c;
//
//			while ((c = InputRead.read(buffer)) > 0) {
//				// char to string conversion
//				String readstring = String.copyValueOf(buffer, 0, c);
//				msg += readstring;
//			}
//			Log.v("query", "message is key " + key + " value is " + msg);
//			InputRead.close();
//			return msg;
		} catch (Exception ex) {
			Log.v("query", ex.getMessage());
			Log.v("phata", "yahan phata 1 for key "+ key);
			return null;
		}
	}

	public QueryObject queryChecker(String selection,String port)  {
		try {
			String key = genHash(selection);
			String value = "";
			QueryObject queryObject = null;
			value = queryHelper(selection,value,port);
			queryObject = new QueryObject(selection,value);
			Log.v(" query object", queryObject.getKey()+"  "+ queryObject.getValue());
			return queryObject;
		}
		catch (NoSuchAlgorithmException ex){
			ex.getStackTrace();
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

    public synchronized void insertValues(String key,String value,String choice, String port){
		String filename = key;
		String val = value + "\n";
		FileOutputStream outputStream;

		try {
//			outputStream = getContext().openFileOutput(filename, getContext().MODE_PRIVATE);
//			outputStream.write(val.getBytes());
//			outputStream.close();
			File path = null;
			if("save".equals(choice)) {
				 path = new File(getContext().getFilesDir(), "local");
			}
			else {
				path = new File(getContext().getFilesDir(), port);

			}
			if (!path.exists()) {
				path.mkdirs();
//					}
			}

//			String path = getContext().
//					getApplicationInfo().dataDir+"/files/";



//					File path = getContext().getFilesDir();
//			String path_local = null;
//			if("save".equals(choice)) {
//
//				path_local = path.toString() + "local" ;
//			}else {
//				path_local = path.toString() + "/"+port;
//			}
			File files = new File(path,key);
			if(files.exists()){
				files.delete();
			}
//			if (!Files.exists()) {
//				Files.mkdirs();
////					}
//			}
//			Files.delete();
			try {
				BufferedWriter buf = new BufferedWriter(new FileWriter(files, true));
				buf.append(value);
				buf.close();
			}
			catch (IOException ex){

			}
			catch (Exception ex){

			}







//			File path = getContext().getFilesDir();
//			String path_local = null;
//			if("save".equals(choice)) {
//				path_local = path.toString() + "/local";
//			}else {
//				path_local = path.toString() + "/"+port;
//			}
//			File dir = new File(path_local);
//			if (!dir.exists()) {
//				dir.mkdirs();
//			}
//			OutputStreamWriter out;
//			File  f = new File(path_local,key);
//			out = new OutputStreamWriter(getContext().openFileOutput(f.getPath(), MODE_PRIVATE));
//			out.write(value);
//			out.close();
		} catch (Exception e) {
			Log.v("insert", "File write failed");
		}
	}


	private class ClientTask1 extends AsyncTask<String, Void, List<QueryObject>>{
		@Override
		protected List<QueryObject> doInBackground(String... msgs){
//            if(msgs[3].equals("insert")){
			try {
				if(msgs[0].equals("insert")) {
					Node node = hash_Map.get(msgs[3]);
					Socket socket = null;
					try {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(node.my_port) * 2);
						DataOutputStream message1 = new DataOutputStream(socket.getOutputStream());
						String messageToSend = "insert" + "|" + msgs[1] + "|" + msgs[2];
						messageToSend = messageToSend + "|" + "save" + "|" + node.my_port;
						Log.v(" message send", " message to send is "+messageToSend+" to port "
								+node.my_port);
						message1.writeUTF(messageToSend);
					}
					catch (SocketTimeoutException sx){
					} catch (SocketException s) {
						Log.e(" failure exception", "ClientTask UnknownHostException in port 3");
					} catch (UnknownHostException e) {
//                    e.printStackTrace();
						Log.e("issue"," I hope the node failure woprks");
					} catch (IOException e) {

						Log.e("issue"," I hope the node failure works");
						Log.v(" phata 3", "io phata");
					}
					Node node1 = hash_Map.get(node.successor);
					try {
						Socket socket1 = null;
						socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(node.successor_port));
						DataOutputStream message2 = new DataOutputStream(socket1.getOutputStream());
						String messageToSend2 = "insert" + "|" + msgs[1] + "|" + msgs[2];
						messageToSend2 = messageToSend2 + "|" + "other" + "|" + node.my_port;
						Log.v(" message send", " message to send is "+messageToSend2+" to port "
								+node.successor);
						message2.writeUTF(messageToSend2);
					}
					catch (SocketTimeoutException sx){
						Log.v("phata"," socket exception");
					} catch (SocketException s) {
						Log.e(" failure exception", "ClientTask UnknownHostException in port 3");
					} catch (UnknownHostException e) {
//                    e.printStackTrace();
						Log.e("issue"," I hope the node failure woprks");
					} catch (IOException e) {

						Log.e("issue"," I hope the node failure works");
						Log.v(" phata 3", "io phata");
					}
					try {
						Node node2 = hash_Map.get(node1.successor);
						Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(node1.successor_port));
						DataOutputStream message3 = new DataOutputStream(socket2.getOutputStream());
						String messageToSend3 = "insert" + "|" + msgs[1] + "|" + msgs[2] + "|" + "other" + "|" + node.my_port;
						Log.v(" message send", " message to send is "+messageToSend3+" to port "
								+node1.successor);
						message3.writeUTF(messageToSend3);
					}
					catch (SocketTimeoutException sx){
					} catch (SocketException s) {
						Log.e(" failure exception", "ClientTask UnknownHostException in port 3");
					} catch (UnknownHostException e) {
//                    e.printStackTrace();
						Log.e("issue"," I hope the node failure woprks");
					} catch (IOException e) {

						Log.e("issue"," I hope the node failure works");
						Log.v(" phata 3", "io phata");
					}
					return null;
				}
				else if(msgs[0].equals("query")){
					Node node = hash_Map.get(msgs[2]);
					List<QueryObject> queryObjects = new ArrayList<QueryObject>();
					Socket socket = null;
					HashMap<String,Integer> h = new HashMap<String, Integer>();
					try {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(node.my_port) * 2);
						DataOutputStream message1 = new DataOutputStream(socket.getOutputStream());
						String messageToSend = "query" + "|" + msgs[1]+"|"+"local";
						Log.v("query", " message to send is " + messageToSend);
						message1.writeUTF(messageToSend);
						ObjectInputStream messageReceived = new ObjectInputStream(socket.getInputStream());
						QueryObject queryObject = (QueryObject) messageReceived.readObject();
						Log.v(" client query debug"," query value is for key "+queryObject.getKey()+" val is "+
								queryObject.getValue());
						if(queryObject.getValue()!=null) {
							h.put(queryObject.getValue(), 1);
						}
					}catch (SocketTimeoutException sx){
					} catch (SocketException s) {
						Log.e(" failure exception", "ClientTask UnknownHostException in port 3");
					} catch (UnknownHostException e) {
//                    e.printStackTrace();
						Log.e("issue"," I hope the node failure woprks");
					} catch (IOException e) {

						Log.e("issue"," I hope the node failure works");
						Log.v(" phata 3", "io phata");
					}

					Node node1 = hash_Map.get(node.successor);
					try{
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(node1.my_port) * 2);
						DataOutputStream message1 = new DataOutputStream(socket.getOutputStream());
						String messageToSend = "query" + "|" + msgs[1]+"|"+node.my_port;
						Log.v("query", " message to send is " + messageToSend);
						message1.writeUTF(messageToSend);
						ObjectInputStream messageReceived = new ObjectInputStream(socket.getInputStream());
						QueryObject queryObject = (QueryObject) messageReceived.readObject();
						Log.v(" client query debug 2"," query value is for key "+queryObject.getKey()+" val is "+
								queryObject.getValue());
						if(queryObject.getValue()!=null&&h.get(queryObject.getValue())!=null){
							h.put(queryObject.getValue(),2);
						}else if(queryObject.getValue()!=null) {
							h.put(queryObject.getValue(),1);
						}
					}
					catch (SocketTimeoutException sx){
					} catch (SocketException s) {
						Log.e(" failure exception", "ClientTask UnknownHostException in port 3");
					} catch (UnknownHostException e) {
//                    e.printStackTrace();
						Log.e("issue"," I hope the node failure woprks");
					} catch (IOException e) {

						Log.e("issue"," I hope the node failure works");
						Log.v(" phata 3", "io phata");
					}
					Node node2 = hash_Map.get(node1.successor);
					try{
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(node2.my_port) * 2);
						DataOutputStream message1 = new DataOutputStream(socket.getOutputStream());
						String messageToSend = "query" + "|" + msgs[1]+"|"+node.my_port;
						Log.v("query", " message to send is " + messageToSend);
						message1.writeUTF(messageToSend);
						ObjectInputStream messageReceived = new ObjectInputStream(socket.getInputStream());
						QueryObject queryObject = (QueryObject) messageReceived.readObject();
						Log.v(" client query debug 3"," query value is for key "+queryObject.getKey()+" val is "+
								queryObject.getValue());
						if(queryObject.getValue()!=null&&h.get(queryObject.getValue())!=null){
							h.put(queryObject.getValue(),2);
						}else if(queryObject.getValue()!=null) {
							h.put(queryObject.getValue(),1);
						}
					}
					catch (SocketTimeoutException sx){
					} catch (SocketException s) {
						Log.e(" failure exception", "ClientTask UnknownHostException in port 3");
					} catch (UnknownHostException e) {
//                    e.printStackTrace();
						Log.e("issue"," I hope the node failure woprks");
					} catch (IOException e) {

						Log.e("issue"," I hope the node failure works");
						Log.v(" phata 3", "io phata");
					}
					String value = null;
					int count =0;
					for(Map.Entry mapElement:h.entrySet()) {
						Log.v(" for query "," query key is "+mapElement.getKey()+" val is "+mapElement.getValue());
						if(count<(Integer)mapElement.getValue()){
							count = (Integer)mapElement.getValue();
							value = (String) mapElement.getKey();
						}
					}
					QueryObject queryObject = new QueryObject(msgs[1],value);
					Log.v(" query Log is "," key is "+msgs[1]+ " value is "+ value);
					queryObjects.add(queryObject);
					return queryObjects;
				}
				else if(msgs[0].equals("query*")){
					List<QueryObject> queryObjects = new ArrayList<QueryObject>();
					for(Map.Entry mapElement:hash_Map.entrySet()){
						try {
							Node node = (Node) mapElement.getValue();
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(node.my_port) * 2);
							DataOutputStream message1 = new DataOutputStream(socket.getOutputStream());
							String messageToSend = "query*" + "|" + self_port;
							message1.writeUTF(messageToSend);
							ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
							List<QueryObject> queryObjects1 = (List<QueryObject>) objectInputStream.readObject();
							queryObjects.addAll(queryObjects1);
						}catch (SocketTimeoutException sx){
						} catch (SocketException s) {
							Log.e(" failure exception", "ClientTask UnknownHostException in port 3");
						} catch (UnknownHostException e) {
//                    e.printStackTrace();
							Log.e("issue"," I hope the node failure woprks");
						} catch (IOException e) {

							Log.e("issue"," I hope the node failure works");
							Log.v(" phata 3", "io phata");
						}
					}
					return queryObjects;
				}
				else if(msgs[0].equals("delete*")){
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(successor));
					DataOutputStream message1 = new DataOutputStream(socket.getOutputStream());
					String messageToSend = "delete*" + "|" + self_port;
					message1.writeUTF(messageToSend);
				}
				else if(msgs[0].equals("delete")){
					Node node = hash_Map.get(msgs[2]);
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(node.successor_port));
						DataOutputStream message1 = new DataOutputStream(socket.getOutputStream());
						String messageToSend = "delete" + "|" + msgs[1] + "|" + hash1;
						message1.writeUTF(messageToSend);
					}
					catch (SocketTimeoutException sx){
					} catch (SocketException s) {
					} catch (UnknownHostException e) {
					} catch (IOException e) {
					} catch (Exception ex){
					}
					try {
						String messageToSend = "delete" + "|" + msgs[1] + "|" + hash1;
						Log.v("delete", " message to send is " + messageToSend);
						Node node1 = hash_Map.get(node.successor);
						Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(node1.successor_port));
						DataOutputStream message2 = new DataOutputStream(socket1.getOutputStream());
						message2.writeUTF(messageToSend);
					}catch (SocketTimeoutException sx){
					} catch (SocketException s) {
					} catch (UnknownHostException e) {
					} catch (IOException e) {
					} catch (Exception ex){
					}
				}
				else if(msgs[0].equals("replica")){
					lock.lock();
					try {
						List<QueryObject> queryObjects = new ArrayList<QueryObject>();
						Node node = hash_Map.get(msgs[1]);
						Log.v("debug test"," working here");
						Socket socket = null;
						DataOutputStream message1 = null;
							try {
								socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
										Integer.parseInt(node.successor_port));
								message1 = new DataOutputStream(socket.getOutputStream());
								String messageToSend = "replica" + "|" + hash1;
								message1.writeUTF(messageToSend);
								Log.v("debug test", " working here 1");
								ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
								List<QueryObject> queryObjects1 = (List<QueryObject>) objectInputStream.readObject();
								queryObjects.addAll(queryObjects1);
							}  catch (SocketTimeoutException sx){
					} catch (SocketException s) {
					} catch (UnknownHostException e) {
					} catch (IOException e) {
					} catch (Exception ex){
					}
						try {
							Log.v("debug test", " working here 2");
							Node node1 = hash_Map.get(node.successor);
							Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(node1.successor_port));
							DataOutputStream message2 = new DataOutputStream(socket1.getOutputStream());
							Log.v("debug test", " working here 3");
							String messageToSend1 = "replica" + "|" + hash1;
							message2.writeUTF(messageToSend1);
							Log.v("debug test", " working here 4");
							ObjectInputStream objectInputStream1 = new ObjectInputStream(socket1.getInputStream());
							List<QueryObject> queryObjects2 = (List<QueryObject>) objectInputStream1.readObject();
							Log.v("debug test", " working here 5");

							queryObjects.addAll(queryObjects2);
						} catch (SocketTimeoutException sx){
						} catch (SocketException s) {
						} catch (UnknownHostException e) {
						} catch (IOException e) {
						} catch (Exception ex){
						}
						if(queryObjects!=null) {
							for (QueryObject queryObject : queryObjects) {
								insertValues(queryObject.getKey(), queryObject.getValue(), "save", "local");
							}
						}
					}
					catch (Exception ex){
						Log.v(" null", " null pointer aya");
					}
					lock.unlock();
					return null;
				}
				else if(msgs[0].equals("preplica")){
					lock1.lock();
					try {
						Node node = hash_Map.get(hash1);
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(node.predeccessor_port));
						DataOutputStream message1 = new DataOutputStream(socket.getOutputStream());
						String messageToSend = "preplica" + "|" + "local";
						message1.writeUTF(messageToSend);
						ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
						List<QueryObject> queryObjects = (List<QueryObject>) objectInputStream.readObject();
						for (QueryObject queryObject : queryObjects) {
							Log.v("insert debug"," inserting values to place "+msgs[1]);
							insertValues(queryObject.getKey(), queryObject.getValue(), "other", node.predeccessor);
						}
						Node node1 = hash_Map.get(node.predeccessor);
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(node1.predeccessor_port));
						message1 = new DataOutputStream(socket.getOutputStream());
						messageToSend = "preplica" + "|" + "local";
						message1.writeUTF(messageToSend);
						objectInputStream = new ObjectInputStream(socket.getInputStream());
						queryObjects = (List<QueryObject>) objectInputStream.readObject();
						for (QueryObject queryObject : queryObjects) {
							Log.v("insert debug 1"," inserting values to place "+msgs[1]);
							insertValues(queryObject.getKey(), queryObject.getValue(), "other", node1.predeccessor);
						}
					} catch (SocketException s) {
					Log.e(" failure exception", "ClientTask UnknownHostException in port 3");
					lock1.unlock();
					return null;
				} catch (UnknownHostException e) {
//                    e.printStackTrace();
					Log.e("issue"," I hope the node failure woprks");
					return null;
				} catch (IOException e) {

					Log.e("issue"," I hope the node failure works");
					Log.v(" phata 3", "io phata");
					return null;
				}
					catch (Exception ex){
						Log.v(" exception thi"," exception is in null");
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
//            }
			return null;
		}
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			while (true) {
				ServerSocket serverSocket = sockets[0];
				Socket socket = null;
				Integer portClient = -1;
				try {
					socket = serverSocket.accept();
					DataInputStream messageReceived = new DataInputStream(socket.getInputStream());
					String message = messageReceived.readUTF();
					Log.v("info of server", message + "zzzzz");
					String[] messageArray = message.split(Pattern.quote("|"));
					Log.v("split"," message array length is "+messageArray.length+" and went till"+"3");

					Log.v("message is",messageArray[0]);
					if("insert".equals(messageArray[0])) {
						Log.v("test", "i am inside");
						String key = messageArray[1];
						String value = messageArray[2];
						if (messageArray[3].equals("save")) {
							Log.v(" insert request", " key is "+key+" valiue is "+value);
							insertValues(key, value,messageArray[3],"local");
						} else if (messageArray[3].equals("other")) {
							Log.v("insert debug 4"," inserting values to place "+messageArray[4]);
							Log.v(" insert request", " key is "+key+" valiue is "+value);
							insertValues(key, value,messageArray[3],messageArray[4]);
						}
					}
					else if("query".equals(messageArray[0])){
//                        MatrixCursor cursor = (MatrixCursor) query(null, null, messageArray[1],null,null);
						QueryObject queryObject = queryChecker(messageArray[1],messageArray[2]);
						Log.v(" query Object is",queryObject.getKey()+" val "+queryObject.getValue());
						ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
						objectOutputStream.writeObject(queryObject);
					}
					else if("query*".equals(messageArray[0])){
//                        MatrixCursor cursor = (MatrixCursor) query(null, null, messageArray[1],null,null);
						List<QueryObject> queryObjects = getAllFiles();
						ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
						objectOutputStream.writeObject(queryObjects);
					}
					else if("delete*".equals(messageArray[0])){
						File[] files = getContext().getFilesDir().listFiles();
						for (File file : files) {
							file.delete();
//                Log.v("query check is", " checking result cursor log "+resultCursor.getCount());
						}
						if(!messageArray[1].equals(successor)){
							Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(successor));
							DataOutputStream message_all = new DataOutputStream(socket2.getOutputStream());
							String messageToSendForAll = "delete*"+"|"+messageArray[1];
							message_all.writeUTF(messageToSendForAll);
						}
					}
					else if("delete".equals(messageArray[0])){
						deleteFromKey(messageArray[1],messageArray[2]);
					}
					else if("replica".equals(messageArray[0])){
						List<QueryObject> queryObjects = getFilesForFolder(messageArray[1]);
						Log.v("replica debug"," giving message back to "+messageArray[1]);
						ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
						objectOutputStream.writeObject(queryObjects);
						Log.v("replica debug"," gave message back to "+messageArray[1]);
					}
					else if("preplica".equals(messageArray[0])){
						List<QueryObject> queryObjects = getFilesForFolder(messageArray[1]);
						ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
						objectOutputStream.writeObject(queryObjects);
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
