package edu.caltech.nanodb.storage;


import edu.caltech.nanodb.expressions.TypeConverter;
import java.util.Arrays;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SQLDataType;
import edu.caltech.nanodb.relations.Tuple;


/**
 * <p>
 * This class is a partial implementation of the {@link Tuple} interface that
 * handles reading and writing tuple data against a {@link DBPage} object.
 * This can be used to read and write tuples in a table file, keys in an index
 * file, etc.  It could also be used to store and manage tuples in memory,
 * although it's generally much faster and simpler to use an optimized in-memory
 * representation for tuples in memory.
 * </p>
 * <p>
 * Each tuple is stored in a layout like this:
 * </p>
 * <ul>
 *   <li>The first one or more bytes are dedicated to a <tt>NULL</tt>-bitmap,
 *       which records columns that are currently <tt>NULL</tt>.</li>
 *   <li>The remaining bytes are dedicated to storing the non-<tt>NULL</tt>
 *       values for the columns in the tuple.</li>
 * </ul>
 * <p>
 * In order to make this class' functionality generic, certain operations must
 * be implemented by subclasses:  specifically, any operation that changes a
 * tuple's size (e.g. writing a non-<tt>NULL</tt> value to a previously
 * <tt>NULL</tt> column or vice versa, or changing the size of a variable-size
 * column).  The issue with these operations is that they require page-level
 * data management, which is beyond the scope of what this class can provide.
 * Thus, concrete subclasses of this class can provide page-level data
 * management as needed.
 * </p>
 */
public abstract class PageTuple implements Tuple {

    /**
     * This value is used in {@link #valueOffsets} when a column value is set to
     * <tt>NULL</tt>.
     */
    public static final int NULL_OFFSET = 0;


    /**
     * The pin-count of this tuple.  Note that this tuple's pin-count will
     * likely be different from the backing {@code DBPage}'s pin-count,
     * particularly if multiple tuples from the {@code DBPage} are in use.
     */
    private int pinCount;


    /** The database page that contains the tuple's data. */
    private DBPage dbPage;


    /** The offset in the page of the tuple's start. */
    private int pageOffset;


    /** The schema of the tuple. */
    private Schema schema;


    /**
     * This array contains the cached offsets of each value in this tuple.
     * The array is populated when a tuple is constructed.  For columns with
     * a value of <tt>NULL</tt>, the offset will be 0.
     *
     * @see #NULL_OFFSET
     */
    private int[] valueOffsets;


    /**
     * The offset in the page where the tuple's data ends.  Note that this value
     * is <u>one byte past</u> the end of the tuple's data; as with most Java
     * sequences, the starting offset is inclusive and the ending offset is
     * exclusive.  Also, as a consequence, this value could be past the end of
     * the byte-array that the tuple resides in, if the tuple is at the end of
     * the byte-array.
     */
    private int endOffset;


    /**
     * Construct a new tuple object that is backed by the data in the database
     * page.  This tuple is able to be read from or written to.
     *
     * @param dbPage     the specific database page that holds the tuple
     * @param pageOffset the offset of the tuple's actual data in the page
     * @param schema     the details of the columns that appear within the tuple
     */
    public PageTuple(DBPage dbPage, int pageOffset, Schema schema) {

        if (dbPage == null)
            throw new NullPointerException("dbPage must be specified");

        if (pageOffset < 0 || pageOffset >= dbPage.getPageSize()) {
            throw new IllegalArgumentException("pageOffset must be in range [0, " +
                dbPage.getPageSize() + "); got " + pageOffset);
        }

        this.dbPage = dbPage;
        this.pageOffset = pageOffset;
        this.schema = schema;

        // Pin ourselves immediately so that we don't lose the DBPage.
        pin();

        valueOffsets = new int[schema.numColumns()];

        computeValueOffsets();
    }


    /**
     * Page tuples are backed by data pages managed by the Buffer Manager, so
     * this method always returns true.
     *
     * @return {@code true} since page tuples are backed by disk pages.
     */
    // um is this supposed to return true or false lmao
    public boolean isDiskBacked() {
        return false;
    }


    @Override
    public void pin() {
        dbPage.pin();
        pinCount++;
    }


    @Override
    public void unpin() {
        if (pinCount <= 0) {
            throw new IllegalStateException(
                "pinCount is not positive (value is " + pinCount + ")");
        }

        pinCount--;
        dbPage.unpin();

        // We don't invalidate the tuple here, so that we can retain the
        // location of the tuple.  Note, however, that if the DBPage's
        // pin-count has hit 0, the actual data backing the tuple will now be
        // unavailable.
    }


    @Override
    public int getPinCount() {
        return pinCount;
    }


    @Override
    public boolean isPinned() {
        return (pinCount > 0);
    }


    /**
     * This method returns an external reference to the tuple, which can be
     * stored and used to look up this tuple.  The external reference is
     * represented as a file-pointer.  The default implementation simply returns
     * a file-pointer to the tuple-data itself, but specific storage formats may
     * introduce a level of indirection into external references.
     *
     * @return a file-pointer that can be used to look up this tuple
     */
    public FilePointer getExternalReference() {
        return new FilePointer(dbPage.getPageNo(), pageOffset);
    }


    public DBPage getDBPage() {
        return dbPage;
    }


    public int getOffset() {
        return pageOffset;
    }


    public int getEndOffset() {
        return endOffset;
    }


    /**
     * Returns the storage-size of the tuple in bytes.
     *
     * @return the storage-size of the tuple in bytes.
     */
    public int getSize() {
        return endOffset - pageOffset;
    }


    public Schema getSchema() {
        return schema;
    }


    /**
     * This helper method checks the column index for being in the proper
     * range of values.
     *
     * @param colIndex the column index to check
     *
     * @throws java.lang.IllegalArgumentException if the specified column
     *         index is out of range.
     */
    private void checkColumnIndex(int colIndex) {
        if (colIndex < 0 || colIndex >= schema.numColumns()) {
            throw new IllegalArgumentException("Column index must be in " +
                "range [0," + (schema.numColumns() - 1) + "], got " + colIndex);
        }
    }


    /**
     * Returns the number of attributes in the tuple.  Note that this value
     * may be zero.
     */
    public int getColumnCount() {
        return schema.numColumns();
    }


    /**
     * This is a helper function to find out the current value of a column's
     * <tt>NULL</tt> flag.  It is not intended to be used to determine if a
     * column's value is <tt>NULL</tt> since the method does a lot of work;
     * instead, use the {@link #isNullValue} method which relies on cached
     * column information.
     *
     * @param colIndex the index of the column to retrieve the null-flag for
     *
     * @return <tt>true</tt> if the column is null, or <tt>false</tt> otherwise
     */
    private boolean getNullFlag(int colIndex) {
        checkColumnIndex(colIndex);

        // Skip to the byte that contains the NULL-flag for this specific column.
        int nullFlagOffset = pageOffset + (colIndex / 8);

        // Shift the flags in that byte right, so that the flag for the
        // requested column is in the least significant bit (LSB).
        int nullFlag = dbPage.readUnsignedByte(nullFlagOffset);
        nullFlag = nullFlag >> (colIndex % 8);

        // If the LSB is 1 then the column's value is NULL.
        return ((nullFlag & 0x01) != 0);
    }


    /**
     * This is a helper function to set or clear the value of a column's NULL
     * flag.
     *
     * @param colIndex the index of the column to retrieve the null-flag for
     *
     * @param value <tt>true</tt> if the column is null, or <tt>false</tt>
     *        otherwise
     */
    private void setNullFlag(int colIndex, boolean value) {
        checkColumnIndex(colIndex);

        // Skip to the byte that contains the NULL-flag for this specific column.
        int nullFlagOffset = pageOffset + (colIndex / 8);

        // Create a bit-mask for setting or clearing the specified NULL flag,
        // then set/clear the flag in the mask byte.
        int mask = 1 << (colIndex % 8);

        int nullFlag = dbPage.readUnsignedByte(nullFlagOffset);

        if (value)
            nullFlag = nullFlag | mask;
        else
            nullFlag = nullFlag & ~mask;

        dbPage.writeByte(nullFlagOffset, nullFlag);
    }


    /**
     * Returns the offset where the tuple's data actually starts.  This is
     * past the bytes used to store NULL-flags.
     *
     * @return the starting index of the tuple's data
     */
    private int getDataStartOffset() {
        // Compute how many bytes the NULL flags take, at the start of the
        // tuple data.
        int nullFlagBytes = getNullFlagsSize(schema.numColumns());
        return pageOffset + nullFlagBytes;
    }


    /**
     * This helper function computes and caches the offset of each column
     * value in the tuple.  If a column has a <tt>NULL</tt> value then
     * {@link #NULL_OFFSET} is used for the offset.
     */
    private void computeValueOffsets() {
        int numCols = schema.numColumns();

        int valOffset = getDataStartOffset();

        for (int iCol = 0; iCol < numCols; iCol++) {
            if (getNullFlag(iCol)) {
                // This column is marked as being NULL.
                valueOffsets[iCol] = NULL_OFFSET;
            }
            else {
                // This column is not NULL.  Store the current offset, then
                // move forward past this value's bytes.
                valueOffsets[iCol] = valOffset;

                ColumnType colType = schema.getColumnInfo(iCol).getType();
                valOffset += getColumnValueSize(colType, valOffset);
            }
        }

        endOffset = valOffset;
    }


    /**
     * Returns the number of bytes used by the column-value at the specified
     * offset, with the specified type.
     *
     * @param colType the column type, which includes both the basic SQL data
     *        type (e.g. <tt>VARCHAR</tt>) and the size/length of the column's
     *        type.
     *
     * @param valueOffset the offset in the data where the column's value
     *        actually starts
     *
     * @return the total number of bytes used by the column-value
     */
    private int getColumnValueSize(ColumnType colType, int valueOffset) {
        // VARCHAR is special - the storage size depends on the size of the
        // data value being stored.  In this case, read out the data length.
        int dataLength = 0;
        if (colType.getBaseType() == SQLDataType.VARCHAR)
            dataLength = dbPage.readUnsignedShort(valueOffset);

        return getStorageSize(colType, dataLength);
    }


    /**
     * Returns true if the specified column is currently set to the SQL
     * <tt>NULL</tt> value.
     *
     * @return <tt>true</tt> if the specified column is currently set to
     *         <tt>NULL</tt>, or <tt>false</tt> otherwise.
     */
    public boolean isNullValue(int colIndex) {
        checkColumnIndex(colIndex);
        return (valueOffsets[colIndex] == NULL_OFFSET);
    }


    /**
     * Returns the specified column's value as an <code>Object</code>
     * reference.  The actual type of the object depends on the column type,
     * and follows this mapping:
     * <ul>
     *   <li><tt>INTEGER</tt> produces {@link java.lang.Integer}</li>
     *   <li><tt>SMALLINT</tt> produces {@link java.lang.Short}</li>
     *   <li><tt>BIGINT</tt> produces {@link java.lang.Long}</li>
     *   <li><tt>TINYINT</tt> produces {@link java.lang.Byte}</li>
     *   <li><tt>FLOAT</tt> produces {@link java.lang.Float}</li>
     *   <li><tt>DOUBLE</tt> produces {@link java.lang.Double}</li>
     *   <li><tt>CHAR(<em>n</em>)</tt> produces {@link java.lang.String}</li>
     *   <li><tt>VARCHAR(<em>n</em>)</tt> produces {@link java.lang.String}</li>
     *   <li><tt>FILE_POINTER</tt> (internal) produces {@link FilePointer}</li>
     * </ul>
     */
    public Object getColumnValue(int colIndex) {
        checkColumnIndex(colIndex);

        Object value = null;
        if (!isNullValue(colIndex)) {
            int offset = valueOffsets[colIndex];

            ColumnType colType = schema.getColumnInfo(colIndex).getType();
            switch (colType.getBaseType()) {

            case INTEGER:
                value = Integer.valueOf(dbPage.readInt(offset));
                break;

            case SMALLINT:
                value = Short.valueOf(dbPage.readShort(offset));
                break;

            case BIGINT:
                value = Long.valueOf(dbPage.readLong(offset));
                break;

            case TINYINT:
                value = Byte.valueOf(dbPage.readByte(offset));
                break;

            case FLOAT:
                value = Float.valueOf(dbPage.readFloat(offset));
                break;

            case DOUBLE:
                value = Double.valueOf(dbPage.readDouble(offset));
                break;

            case CHAR:
                value = dbPage.readFixedSizeString(offset, colType.getLength());
                break;

            case VARCHAR:
                value = dbPage.readVarString65535(offset);
                break;

            case FILE_POINTER:
                value = new FilePointer(dbPage.readUnsignedShort(offset),
                                        dbPage.readUnsignedShort(offset + 2));
                break;

            default:
                throw new UnsupportedOperationException(
                    "Cannot currently store type " + colType.getBaseType());
            }
        }

        return value;
    }


    /**
     * Sets the column to the specified value, or <tt>NULL</tt> if the value is
     * the Java <tt>null</tt> value.
     *
     * @param colIndex The index of the column to set.
     *
     * @param value the value to set the column to, or <tt>null</tt> if the
     *        column should be set to the SQL <tt>NULL</tt> value.
     */
    public void setColumnValue(int colIndex, Object value) {
        checkColumnIndex(colIndex);

        if (value == null) {
            // Set the column-value to NULL.
            setNullColumnValue(colIndex);
        }
        else {
            // Update the value stored in the tuple to what was specified.
            setNonNullColumnValue(colIndex, value);
        }
    }


    /**
     * This helper function is used by the {@link #setColumnValue} method in
     * the specific case when a column is being set to the SQL <tt>NULL</tt>
     * value.
     *
     * @param iCol the index of the column to set to <tt>NULL</tt>
     */
    private void setNullColumnValue(int iCol) {
        /* TODO:  Implement!
         *
         * The column's flag in the tuple's null-bitmap must be set to true.
         * Also, the data occupied by the column's value must be removed.
         * There are many helpful methods that can be used for this method:
         *  - isNullValue() and setNullFlag() to check/change the null-bitmap
         *  - deleteTupleDataRange() to remove part of a tuple's data
         *
         * You will have to examine the column's type as well; you can use
         * the schema.getColumnInfo(iCol) method to determine the column's
         * type; schema.getColumnInfo(iCol).getType() to get the basic SQL
         * data type.  If the column is a variable-size column (e.g. VARCHAR)
         * then you may need to retrieve details from the column itself using
         * the dbPage member, and the getStorageSize() field.
         *
         * Finally, the valueOffsets array is extremely important, because it
         * contains the starting offset of every non-NULL column's data in the
         * tuple.  Setting a column's value to NULL will obviously affect at
         * least some of the value-offsets.  Make sure to update this array
         * properly as well.  (Note that columns whose value is NULL will have
         * the special NULL_OFFSET constant as their offset in the tuple.)
         */
        if (isNullValue(iCol))
            return; //if it's already Null, nothing to do

        setNullFlag(iCol, true); //flagging as null
        ColumnType datatype = schema.getColumnInfo(iCol).getType();

        int datalength = getColumnValueSize(datatype, valueOffsets[iCol]);
        //getting the number of bytes the column takes up

        deleteTupleDataRange(valueOffsets[iCol], datalength);
        // /deleting the tuple range

        pageOffset += datalength; //updating our offsets
        computeValueOffsets();
    }


    /**
     * This helper function is used by the {@link #setColumnValue} method in
     * the specific case when a column is being set to a non-<tt>NULL</tt>
     * value.
     *
     * @param iCol The index of the column to set.
     *
     * @param value the value to set the column to.
     *
     * @throws IllegalArgumentException if the specified value is {@code null}
     */
    private void setNonNullColumnValue(int iCol, Object value) {
        if (value == null)
            throw new IllegalArgumentException("value cannot be null");

        /* TODO:  Implement!
         *
         * This time, the column's flag in the tuple's null-bitmap must be set
         * to false (if it was true before).
         *
         * The trick is to figure out the size of the old column-value, and
         * the size of the new column-value, so that the right amount of space
         * can be made available for the new value.  If the column is a fixed-
         * size type (e.g. an INTEGER) then this is easy, but if the column is
         * a variable-size type (e.g. VARCHAR) then this will be more
         * involved.  As before, retrieving the column's type will be important
         * in implementing the method:  schema.getColumnInfo(iCol), and then
         * schema.getColumnInfo(iCol).getType() to get the basic type info.
         * You can use the getColumnValueSize() method to determine the size
         * of a value as well.
         *
         * As before, the valueOffsets array is extremely important to use and
         * modify correctly, so take care in how you manage it.
         *
         * The tuple's data in the page starts at the offset returned by the
         * getDataStartOffset() method; this is the offset past the tuple's
         * null-bitmask.
         *
         * Finally, once you have made space for the new column value, you can
         * write the value itself using the writeNonNullValue() method.
         */

        // info about the old value
        int offset = valueOffsets[iCol];
        ColumnInfo info = schema.getColumnInfo(iCol);
        ColumnType type = info.getType();

        // check if offset == NULL_OFFSET
        boolean isNullVal = isNullValue(iCol);

        if (isNullVal) {
            int p = iCol - 1;
            // while the value before iCol is null, decrement
            while (valueOffsets[p] == NULL_OFFSET && p >= 0) {
                p--;
            }
            // if the while loop ended before p = -1
            if (p >= 0) {
                // get the ColumnType of p
                ColumnType prev = schema.getColumnInfo(p).getType();
                // calculate the offset for the column following p
                offset = valueOffsets[p] + getColumnValueSize(prev, valueOffsets[p]);
            }
        }
        setNullFlag(iCol, false);

        // the size of the current column's value (0 if null)
        int size = isNullVal ? 0 : getColumnValueSize(type, offset);

        // length of the new object value as a string
        int dataLength = 0;
        // the storage size of the new object value
        int new_size = 0;

        // if the column has type VARCHAR, then we need to find the difference
        // between the sizes of the old and new values
        if (type.getBaseType().getTypeID() == (byte) 22) {
            // get the length of the object value as a string
            dataLength = TypeConverter.getStringValue(value).length();
            // get the actual size of the object value when in the database
            new_size = getStorageSize(type, dataLength);
        }
        // difference between the new VARCHAR and old VARCHAR sizes
        int diff = new_size - size;

        // either insert or delete space depending on whether the
        // new or old VARCHAR size was larger
        if (diff > 0) {
            insertTupleDataRange(offset, diff);
        } else if (diff < 0) {
            deleteTupleDataRange(offset, -diff);
        }

        // update the page offset and valueOffsets
        pageOffset -= diff;
        computeValueOffsets();
        writeNonNullValue(dbPage, offset-diff, type, value);
    }


    /**
     * This method computes and returns the number of bytes that are used to
     * store the null-flags in each tuple.
     *
     * @param numCols the total number of columns in the table
     *
     * @return the number of bytes used for the null-bitmap.
     *
     * @review (donnie) This is really a table-file-level computation, not a
     *         tuple-level computation.
     */
    public static int getNullFlagsSize(int numCols) {
        if (numCols < 0) {
            throw new IllegalArgumentException("numCols must be >= 0; got " +
                numCols);
        }

        int nullFlagsSize = 0;
        if (numCols > 0)
            nullFlagsSize = 1 + (numCols - 1) / 8;

        return nullFlagsSize;
    }


    /**
     * Returns the storage size of a particular column's (non-<tt>NULL</tt>)
     * value, in bytes.  The length of the value is required in cases where
     * the column value can be variable size, such as if the type is a
     * <tt>VARCHAR</tt>.  Note that the data-length is actually <em>not</em>
     * required when the type is <tt>CHAR</tt>, since <tt>CHAR</tt> fields
     * always have a specific size.
     *
     * @param colType the column's data type
     * @param dataLength for column-types that specify a length, this is the
     *        length value.
     *
     * @return the storage size of the data in bytes
     */
    public static int getStorageSize(ColumnType colType, int dataLength) {
        int size;

        switch (colType.getBaseType()) {

        case INTEGER:
        case FLOAT:
            size = 4;
            break;

        case SMALLINT:
            size = 2;
            break;

        case BIGINT:
        case DOUBLE:
            size = 8;
            break;

        case TINYINT:
            size = 1;
            break;

        case CHAR:
            // CHAR values are of a fixed size, but the size is specified in
            // the length field and there is no other storage required.
            size = colType.getLength();
            break;

        case VARCHAR:
            // VARCHAR values are of a variable size, but there is always a
            // two byte length specified at the start of the value.
            size = 2 + dataLength;
            break;

        case FILE_POINTER:
            // File-pointers are comprised of a two-byte page number and a
            // two-byte offset in the page.
            size = 4;
            break;

        default:
            throw new UnsupportedOperationException(
                "Cannot currently store type " + colType.getBaseType());
        }

        return size;
    }


    /**
     * This helper function takes a tuple (from an arbitrary source) and
     * computes how much space it would require to be stored in a heap table
     * file with the specified schema.  This is used to insert new tuples into
     * a table file by computing how much space will be needed, so that an
     * appropriate page can be found.
     *
     * @review (donnie) It doesn't make sense to have this be a non-static
     *         method, since a page-tuple references a specific tuple, not a
     *         data page.  However, having this as a static method on this
     *         class doesn't seem too bad.
     *
     * @param schema the schema of the tuple
     *
     * @param tuple the tuple to compute the storage size for
     *
     * @return the total size for storing the tuple's data in bytes
     */
    public static int getTupleStorageSize(Schema schema, Tuple tuple) {

        if (schema.numColumns() != tuple.getColumnCount()) {
            throw new IllegalArgumentException(
                "Tuple has different arity than target schema.");
        }

        int storageSize = getNullFlagsSize(schema.numColumns());
        int iCol = 0;
        for (ColumnInfo colInfo : schema.getColumnInfos()) {

            ColumnType colType = colInfo.getType();
            Object value = tuple.getColumnValue(iCol);

            // If the value is NULL (represented by Java's null here...) then
            // it takes no space.  Otherwise, compute the space taken by this
            // value.
            if (value != null) {
                // VARCHAR is special - the storage size depends on the size
                // of the data value being stored.
                int dataLength = 0;
                if (colType.getBaseType() == SQLDataType.VARCHAR) {
                    String strValue = TypeConverter.getStringValue(value);
                    dataLength = strValue.length();
                }

                storageSize += getStorageSize(colType, dataLength);
            }

            iCol++;
        }

        return storageSize;
    }


    public static int storeTuple(DBPage dbPage, int pageOffset,
                                 Schema schema, Tuple tuple) {

        if (schema.numColumns() != tuple.getColumnCount()) {
            throw new IllegalArgumentException(
            "Tuple has different arity than target schema.");
        }

        // Start writing data just past the NULL-flag bytes.
        int currOffset = pageOffset + getNullFlagsSize(schema.numColumns());
        int iCol = 0;
        for (ColumnInfo colInfo : schema.getColumnInfos()) {
            ColumnType colType = colInfo.getType();
            Object value = tuple.getColumnValue(iCol);
            int dataSize = 0;

            // If the value is NULL (represented by Java's null here) then set
            // the corresponding NULL-flag.  Otherwise, write the value.
            if (value == null) {
                setNullFlag(dbPage, pageOffset, iCol, true);
            }
            else {
                // Write in the data value.
                setNullFlag(dbPage, pageOffset, iCol, false);
                dataSize = writeNonNullValue(dbPage, currOffset, colType, value);
            }

            currOffset += dataSize;
            iCol++;
        }

        return currOffset;
    }


    /**
     * This is a helper function to set or clear the value of a column's
     * <tt>NULL</tt> flag.
     *
     * @param dbPage the file-page that the value will be written into
     *
     * @param tupleStart the byte-offset in the page where the tuple starts
     *
     * @param colIndex the index of the column to set the null-flag for
     *
     * @param value the new value for the null-flag
     */
    public static void setNullFlag(DBPage dbPage, int tupleStart,
        int colIndex, boolean value) {

        //checkColumnIndex(colIndex);

        // Skip to the byte that contains the NULL-flag for this specific column.
        int nullFlagOffset = tupleStart + (colIndex / 8);

        // Create a bit-mask for setting or clearing the specified NULL flag, then
        // set/clear the flag in the mask byte.
        int mask = 1 << (colIndex % 8);

        int nullFlag = dbPage.readUnsignedByte(nullFlagOffset);

        if (value)
            nullFlag = nullFlag | mask;
        else
            nullFlag = nullFlag & ~mask;

        dbPage.writeByte(nullFlagOffset, nullFlag);
    }



    /**
     * This helper function is used by the {@link #setColumnValue} method in
     * the specific case when a column is being set to a non-<tt>NULL</tt>
     * value.
     *
     * @param dbPage the file-page that the value will be written into
     * @param offset the actual byte-offset in the page where the value is
     *        written to
     * @param colType the type of the column that the value is being written for
     * @param value the non-<tt>null</tt> value to store
     *
     * @return The number of bytes written for the specified value.
     *
     * @throws NullPointerException if <tt>dbPage</tt> is <tt>null</tt>, or if
     *         <tt>value</tt> is <tt>null</tt>.
     */
    public static int writeNonNullValue(DBPage dbPage, int offset,
        ColumnType colType, Object value) {
        return dbPage.writeObject(offset, colType, value);
    }


    protected abstract void insertTupleDataRange(int off, int len);


    protected abstract void deleteTupleDataRange(int off, int len);


    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("PT[");

        boolean first = true;
        for (int i = 0; i < getColumnCount(); i++) {
            if (first)
                first = false;
            else
                buf.append(',');

            Object obj = getColumnValue(i);
            if (obj == null)
                buf.append("NULL");
            else
                buf.append(obj);
        }
        buf.append(']');

        return buf.toString();
    }

}

