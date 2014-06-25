/**
 * $Id$
 */
package com.eroi.migrate.generators;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import com.eroi.migrate.misc.Closer;
import com.eroi.migrate.misc.Log;
import com.eroi.migrate.misc.SchemaMigrationException;
import com.eroi.migrate.misc.Validator;
import com.eroi.migrate.schema.CascadeRule;
import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.ForeignKey;
import com.eroi.migrate.schema.Index;
import com.eroi.migrate.schema.Table;

/**
  * <p>Class OracleGenerator provides methods for creating statements to
  * create, alter, or drop tables.</p>
  *
  * @author Peter Svehla
  */
public class OracleGenerator extends GenericGenerator {

	private static Log log = Log.getLog(OracleGenerator.class);

	/**
	 * Constructor defers setup to parent.
	 *
	 * @param aConnection The db connection.
	 */
	public OracleGenerator(Connection aConnection) {
		super(aConnection);
	}

	/**
	 * Generates a statement to add a foreign key constraint.
	 *
	 * @param foreignKey The foreign key to add.
	 * @return The generated statement.
	 */
	@Override
	public String addForeignKey(ForeignKey foreignKey) {
		Validator.notNull(foreignKey, "Foreign key can not be null");

	    StringBuffer retVal = new StringBuffer();

	    String[] childColumns = wrapStrings(foreignKey.getChildColumns());
	    String[] parentColumns = wrapStrings(foreignKey.getParentColumns());

	    retVal.append("ALTER TABLE ")
	    	  .append(wrapName(foreignKey.getChildTable()))
	          .append(" ADD CONSTRAINT ")
	          .append(wrapName(foreignKey.getName()))
	          .append(" FOREIGN KEY  (")
	          .append(GeneratorHelper.makeStringList(childColumns))
	          .append(") REFERENCES ")
	          .append(wrapName(foreignKey.getParentTable()))
	          .append(" (")
	          .append(GeneratorHelper.makeStringList(parentColumns))
	          .append(")");

	    if (!CascadeRule.none.equals(foreignKey.getCascadeDeleteRule())) {
	    	switch (foreignKey.getCascadeDeleteRule()) {
	    		case cascade: retVal.append(" ON DELETE CASCADE"); break;
	    		case setnull: retVal.append(" ON DELETE SET NULL"); break;
	    		default:
	    			log.warn("Oracle does not support cascade rule "
	    				+ foreignKey.getCascadeDeleteRule()
	    				+ " on delete. I've created constraint "
	    				+ foreignKey.getName()
	    				+ " for you, but it doesn't have the delete cascade rule you wanted.");
    				break;
	    	}
	    }

	    if (!CascadeRule.none.equals(foreignKey.getCascadeUpdateRule())) {
	    	log.warn("Oracle does not support on update cascades for foreign keys. I've created constraint "
	    			+ foreignKey.getName()
	    			+ " for you, but it doesn't have the update cascade rule you wanted.");
	    }

	    return retVal.toString();
	}


	/**
	 * Generates a statement to add a new column to a table.
	 *
	 * Since Oracle does not support specifying the position of a new column, the position specifier
	 * parameter is ignored. Oracle just puts the new column at the end.
	 *
	 * @param column The column to add.
	 * @param tableName The table to add it to.
	 * @param afterColumn Ignored.
	 * @return The generated statement.
	 */
	@Override
	public String addColumnStatement(Column column, String tableName, String afterColumn) {

		if (afterColumn != null)
			log.warn("Add column: Oracle does not support specifying the position of a new column.");

		return addColumnStatement(column, tableName);
	}

	/**
	 * Generates a statement to add a new column to a table.
	 *
	 * Since Oracle does not support specifying the position of a new column, the position specifier
	 * parameter is ignored. Oracle just puts the new column at the end.
	 *
	 * @param column The column to add.
	 * @param tableName The table to add it to.
	 * @param position Ignored.
	 * @return The generated statement.
	 */
	@Override
	public String addColumnStatement(Column column, String tableName, int position) {
		log.warn("Add column: Oracle does not support specifying the position of a new column.");
		return addColumnStatement(column, tableName);
	}

	/**
	 * Generates a statement to add a new column to a table.
	 *
	 * @param column The column to add.
	 * @param tableName The table to add it to.
	 * @return The generated statement.
	 */
	private String addColumnStatement(Column column, String tableName) {

		Validator.notNull(column, "Column can not be null");
		Validator.notNull(tableName, "Table name can not be null");

		StringBuffer alter = new StringBuffer();

	    alter.append("ALTER TABLE ")
	         .append(wrapName(tableName))
	         .append(" ADD ")
	         .append(makeColumnString(column, false));

		return alter.toString();
	}

	/**
	 * Generates a CREATE TABLE statement.
	 *
	 * @param table The Table to generate the create statement for.
	 */
	@Override
	public String createTableStatement(Table table) {
    	return createTableStatement(table, null);
    }

	/**
	 * Generates a CREATE TABLE statement.
	 *
	 * @param table The Table to generate the create statement for.
	 * @param options Additional table properties.
	 * @return The CREATE TABLE statement.
	 */
	@Override
    public String createTableStatement(Table table, String options) {

		StringBuffer retVal = new StringBuffer();

		Validator.notNull(table, "Table can not be null");

		Column[] columns = table.getColumns();

		Validator.notNull(columns, "Columns can not be null");
		Validator.isTrue(columns.length > 0, "At least one column must exist");

		int numberOfAutoIncrementColumns = GeneratorHelper.countAutoIncrementColumns(columns);

		if (numberOfAutoIncrementColumns > 0)
			log.warn("Oracle does not support autoincrement columns. Your directive has been ignored.");

		boolean hasMultiplePrimaryKeys = GeneratorHelper.countPrimaryKeyColumns(columns) > 1;

		Validator.isTrue(GeneratorHelper.countPrimaryKeyColumns(columns) <= 32,
				"Oracle is limited to 32 PRIMARY KEY columns.");

		retVal.append("CREATE TABLE ").append(wrapName(table.getTableName())).append(" (");

		try {
		    for (int x = 0; x < columns.length; x++) {
		    	Column column = (Column)columns[x];

				if (x > 0) {
				    retVal.append(", ");
				}

				retVal.append(makeColumnString(column, hasMultiplePrimaryKeys));
		    }
		} catch (ClassCastException e) {
		    log.error("A table column couldn't be cast to a column: " + e.getMessage());

		    throw new SchemaMigrationException("A table column couldn't be cast to a column: "
		    		+ e.getMessage());
		}

		if (hasMultiplePrimaryKeys) {
		    retVal.append(", CONSTRAINT ").append(table.getTableName()).append("_pk PRIMARY KEY(");
		    Column[] primaryKeys = GeneratorHelper.getPrimaryKeyColumns(columns);

		    for (int x = 0; x < primaryKeys.length; x++) {
				Column column = (Column)primaryKeys[x];

				if (x > 0) {
				    retVal.append(", ");
				}

				retVal.append(wrapName(column.getColumnName()));
		    }
		    retVal.append(")");
		}

		retVal.append(")");

		if (options != null) {
	            retVal.append(" ").append(options);
	    }

	    return retVal.toString();
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean indexExists(String indexName, String tableName) {
		Validator.notNull(indexName, "Index name can not be null");
		Validator.notNull(tableName, "Table name can not be null");

		try {
			ResultSet resultSet = null;

			try {
				DatabaseMetaData databaseMetaData = this.connection.getMetaData();
				resultSet = databaseMetaData.getIndexInfo(null, null, tableName, false, true);

				if (resultSet != null) {
					while (resultSet.next()) {
						String name = resultSet.getString("INDEX_NAME");

						if (indexName.equals(name)) {
							return true;
						}
					}
				}
			} finally {
				Closer.close(resultSet);
			}
		} catch (SQLException exception) {
			log.error("Exception occoured in GenericGenerator.indexExists(indexName, tableName)!!", exception);
			throw new SchemaMigrationException(exception);
		}

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean tableExists(String tableName) {
		try {
			ResultSet tables = null;

			try {
				DatabaseMetaData databaseMetaData = this.connection.getMetaData();
				String schema = databaseMetaData.getUserName();
				tables = databaseMetaData.getTables(this.connection.getCatalog(), schema, tableName, null);

				if (tables != null) {
					while (tables.next()) {
						if (tableName.equalsIgnoreCase(tables.getString("TABLE_NAME"))) {
							return true;
						}
					}
				}
			} finally {
				Closer.close(tables);
			}

			return false;
		} catch (SQLException exception) {
            log.error("Error occoured in GenericGenerator.tableExists(tableName)", exception);
			throw new SchemaMigrationException(exception);
		}
	}

	/**
	 * Generates the clause for a column in a CREATE TABLE statement.
	 *
	 * @param column The column the string is being generated for.
	 *
	 * @param suppressPrimaryKey If a primary key consists of more than one column, a separate clause is
	 *                           used to generate the constraint. So we want to suppress PRIMARY KEY
	 *                           modifiers here.
	 *
	 * @return The clause for the column.
	 */
	private String makeColumnString(Column column, boolean suppressPrimaryKey) {

    	StringBuffer retVal = new StringBuffer();
    	retVal.append(wrapName(column.getColumnName())).append(" ");

    	int type = column.getColumnType();
		retVal.append(GeneratorHelper.getSqlName(type));

    	if (GeneratorHelper.needsLength(type)) {
    	    retVal.append("(").append(column.getLength()).append(")");
    	}

    	if (GeneratorHelper.acceptsScale(type) || type == Types.NUMERIC) {
    		String end = "";

    		if (column.getPrecision() != null) {
    			retVal.append("(").append(column.getPrecision());
    			end = ")";
    		}

    		if (column.getScale() != null) {
    			if (column.getPrecision() == null) {
    				log.error("Scale of column "
    						+ column.getColumnName()
    						+ " is defined, but precision isn't.");

    				throw new SchemaMigrationException("Scale of column "
    						+ column.getColumnName()
    						+ " is defined, but precision isn't.");
    			}

    			retVal.append(",").append(column.getScale());
    		}

    		retVal.append(end);
    	}

    	retVal.append(" ");

    	if (!column.isNullable()) {
    	    retVal.append("NOT NULL ");
    	}

    	if (column.getDefaultValue() != null) {
    	    retVal.append("DEFAULT '").append(column.getDefaultValue()).append("' ");
    	}

    	if (!suppressPrimaryKey && column.isPrimaryKey()) {
    	    retVal.append("PRIMARY KEY ");
    	}

    	return retVal.toString().trim();
    }

	/**
	 * Generates an ALTER TABLE <table> MODIFY (<column>) statement to modify a table's column.
	 *
	 * @param definition The column as it should be after the change.
	 * @param tableName The table the contains the column to be altered.
	 * @return The generated statement.
	 */
	@Override
	public String alterColumnStatement(Column definition, String tableName) {

		Validator.notNull(definition, "Column definition can not be null");
		Validator.notNull(tableName, "Table name can not be null");

		StringBuffer retVal = new StringBuffer("ALTER TABLE ");

		retVal.append(wrapName(tableName))
			.append(" MODIFY (")
			.append(makeColumnString(definition, false))
			.append(")");

		return retVal.toString();
	}

	/**
	 * Generates a ALTER TABLE <tableName> DROP COLUMN <columnName> statement, to drop a column in a table.
	 *
	 * @param columnName The name of the column to drop.
	 * @param tableName The table that contains the column.
	 * @return The generated statement.
	 */
	@Override
	public String dropColumnStatement(String columnName, String tableName) {

		Validator.notNull(columnName, "Column name can not be null");
		Validator.notNull(tableName, "Table name can not be null");

	    StringBuffer query = new StringBuffer();

	    query.append("ALTER TABLE ")
	    	.append(wrapName(tableName))
	    	.append(" DROP COLUMN ")
	    	.append(wrapName(columnName));

		return query.toString();
	}

	/**
	 * Generates a create index statement.
	 *
	 * CREATE [UNIQUE] INDEX <name> ON <index.table>(<index.columnName>[,...])
	 *
	 * @param index The index to create.
	 * @return The generated statement.
	 */
	@Override
	public String addIndex(Index index) {

		Validator.notNull(index, "Index can not be null");

		StringBuffer query = new StringBuffer("CREATE ");

		if (index.isUnique()) {
			query.append("UNIQUE ");
		}

		query.append("INDEX ").append(wrapName(index.getName())).append(" ");

		if (index.isPrimaryKey()) {
			log.warn("Oracle does not support primary key indexes, since they are created automatically "
					+ "for columns with a primary key contraint. The primary key property of index "
					+ index.getName()
					+ " has been ignored.");
		}

		query.append("ON ").append(wrapName(index.getTableName())).append("(");

		String[] columns = index.getColumnNames();
		Validator.isTrue(columns.length < 33, "Oracle does not support indexes on more than 32 columns");
		String comma = "";

		for (String column : columns) {
			query.append(comma).append(wrapName(column));
			comma = ", ";
		}

		query.append(")");

		return query.toString();
	}

	/**
	 * Generates a statement to rename a column.
	 *
	 * @param newColumnName The new column name.
	 * @param oldColumnName The old column name.
	 * @param tableName The table containing the column to be renamed.
	 * @return The generated statement.
	 */
	@Override
	public String renameColumn(String newColumnName, String oldColumnName, String tableName) {

		Validator.notNull(newColumnName, "New column name can not be null");
		Validator.notNull(oldColumnName, "Old column name can not be null");
		Validator.notNull(tableName, "Table name can not be null");

		StringBuffer query = new StringBuffer();

		query.append("ALTER TABLE ")
			.append(wrapName(tableName))
			.append(" RENAME COLUMN ")
			.append(wrapName(oldColumnName))
			.append(" TO ")
			.append(wrapName(newColumnName));

		return query.toString();
	}

	/**
	 * Generates a statement that renames a table: RENAME <table> TO <new name>.
	 *
	 * @param tableName The name of the table to rename.
	 * @param newName The table's new name.
	 * @return The generated statement.
	 */
	@Override
	public String renameTableStatement(String tableName, String newName) {
		Validator.notNull(tableName, "Table name must not be null");
		Validator.notNull(newName, "new Table name must not be null");

		StringBuffer retVal = new StringBuffer();
		retVal.append("RENAME ")
			.append(wrapName(tableName))
			.append(" TO ")
			.append(wrapName(newName));

		return retVal.toString();
	}
}
