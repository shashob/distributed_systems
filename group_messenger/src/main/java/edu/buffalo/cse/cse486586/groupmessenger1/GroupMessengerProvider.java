package edu.buffalo.cse.cse486586.groupmessenger1;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.net.Uri;
import android.util.Log;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.provider.BaseColumns;
import android.content.Context;
import java.util.Random;



/**
 * GroupMessengerProvider is a key-value table. Once again, please note that we do not implement
 * full support for SQL as a usual ContentProvider does. We re-purpose ContentProvider's interface
 * to use it as a key-value table.
 * 
 * Please read:
 * 
 * http://developer.android.com/guide/topics/providers/content-providers.html
 * http://developer.android.com/reference/android/content/ContentProvider.html
 * 
 * before you start to get yourself familiarized with ContentProvider.
 * 
 * There are two methods you need to implement---insert() and query(). Others are optional and
 * will not be tested.
 * 
 * @author stevko
 *
 */
public class GroupMessengerProvider extends ContentProvider {
    //https://www.educative.io/edpresso/how-to-generate-random-numbers-in-java
    //https://www.geeksforgeeks.org/generating-random-numbers-in-java/

    /*To increment the database version after each run of the app, I have used math.random() generators for the databases @author shashank*/

    public static int DATABASE_VERSION;
    static{
        DATABASE_VERSION = (int)(Math.random()*1000);
    }
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // You do not need to implement this.
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        /*
         * TODO: You need to implement this method. Note that values will have two columns (a key
         * column and a value column) and one row that contains the actual (key, value) pair to be
         * inserted.
         * 
         * For actual storage, you can use any option. If you know how to use SQL, then you can use
         * SQLite. But this is not a requirement. You can use other storage options, such as the
         * internal storage option that we used in PA1. If you want to use that option, please
         * take a look at the code for PA1.
         */


        Log.v("insert", values.toString());



        FeedReaderDbHelper dbHelper = new FeedReaderDbHelper(getContext());
        // Gets the data repository in write mode
        SQLiteDatabase db = dbHelper.getWritableDatabase();
        // Insert the new row, returning the primary key value of the new row
//        long newRowId = db.insert(FeedReaderContract.FeedEntry.TABLE_NAME, null, values);
        long newRowId = db.insertWithOnConflict(FeedReaderContract.FeedEntry.TABLE_NAME, null, values,SQLiteDatabase.CONFLICT_IGNORE);




        return uri;
    }

    @Override
    public boolean onCreate() {
        // If you need to perform any one-time initialization task, please do it here.
        return false;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        /*
         * TODO: You need to implement this method. Note that you need to return a Cursor object
         * with the right format. If the formatting is not correct, then it is not going to work.
         *
         * If you use SQLite, whatever is returned from SQLite is a Cursor object. However, you
         * still need to be careful because the formatting might still be incorrect.
         *
         * If you use a file storage option, then it is your job to build a Cursor * object. I
         * recommend building a MatrixCursor described at:
         * http://developer.android.com/reference/android/database/MatrixCursor.html
         */

        Log.v("query", selection);

        // Define a projection that specifies which columns from the database
        // you will actually use after this query.

        // Filter results WHERE "title" = 'My Title'
        String select = FeedReaderContract.FeedEntry.COLUMN_NAME_TITLE + " = ?";

        // How you want the results sorted in the resulting Cursor
        sortOrder = FeedReaderContract.FeedEntry.COLUMN_NAME_TITLE + " DESC";

        FeedReaderDbHelper dbHelper = new FeedReaderDbHelper(getContext());

        SQLiteDatabase db = dbHelper.getReadableDatabase();

        Cursor resultCursor = db.query(
                FeedReaderContract.FeedEntry.TABLE_NAME,   // The table to query
                projection,             // The array of columns to return (pass null to get all)
                select,              // The columns for the WHERE clause
                new String[] {selection},          // The values for the WHERE clause
                null,                   // don't group the rows
                null,                   // don't filter by row groups
                sortOrder               // The sort order
        );
        Log.v("query", DatabaseUtils.dumpCursorToString(resultCursor));

        return resultCursor;
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

    /* Inner class that defines the table contents */
//    static class FeedEntry implements BaseColumns {
//        public static final String TABLE_NAME = "entry";
//        public static final String COLUMN_NAME_TITLE = "key";
//        public static final String COLUMN_NAME_TITLE2 = "value";
//    }

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
