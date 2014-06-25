package db.migrations;

import java.sql.SQLException;
import java.sql.Types;

import com.eroi.migrate.AbstractMigration;
import com.eroi.migrate.misc.SchemaMigrationException;
import com.eroi.migrate.schema.Column;

public class Migration_10 extends AbstractMigration {

	private String dbProduct;
	
	@Override
	protected void init() {
		try {
			this.dbProduct = getConfiguration().getConnection().getMetaData().getDatabaseProductName();
		} catch (SQLException e) {
			throw new SchemaMigrationException("Failed to retrieve DB Product Name");
		}
	}

	public void down() {
		throw new SchemaMigrationException(this.dbProduct + " is not supported");
	}

	public void up() {
		throw new SchemaMigrationException(this.dbProduct + " is not supported");
	}

	protected Column[] getColumns() {
		Column[] columns = new Column[2];
		
		columns[0] = new Column("id", Types.INTEGER, -1, true, false, null, true);
		columns[1] = new Column("desc", Types.VARCHAR, 50, false, true, null, false);
		
		return columns;
	}
	
	public static class Apache_Derby extends Migration_10 {
		
		@Override
		public void up() {
			createTable(table("apache_derby", getColumns()));
		}
		
		@Override
		public void down() {
			dropTable("apache_derby");
		}
	}
	
	public static class H2 extends Migration_10 {
		
		@Override
		public void up() {
			createTable(table("h2", getColumns()));
		}
		
		@Override
		public void down() {
			dropTable("h2");
		}
	}

	public static class MySQL extends Migration_10 {
		
		@Override
		public void up() {
			createTable(table("mysql", getColumns()));
		}
		
		@Override
		public void down() {
			dropTable("mysql");
		}
	}

	public static class PostgreSQL extends Migration_10 {
		
		@Override
		public void up() {
			createTable(table("postgresql", getColumns()));
		}
		
		@Override
		public void down() {
			dropTable("postgresql");
		}
	}
	
	public static class SQL_Anywhere extends Migration_10 {
		
		@Override
		public void up() {
			createTable(table("sql_anywhere", getColumns()));
		}
		
		@Override
		public void down() {
			dropTable("sql_anywhere");
		}
	}

	public static class Oracle extends Migration_10 {
		
		@Override
		public void up() {
			createTable(table("oracle", getColumns()));
		}
		
		@Override
		public void down() {
			dropTable("oracle");
		}
	}
}
