from connect_migrate.mapper.jdbc.database_inferrer import DatabaseInferrer
from connect_migrate.mapper.jdbc.url_parser import JdbcUrlParser


def make_inferrer():
    return DatabaseInferrer(JdbcUrlParser())


class TestInferDatabaseType:
    def test_mysql_url(self):
        assert make_inferrer().infer_database_type({"connection.url": "jdbc:mysql://h:3306/d"}) == "mysql"

    def test_mariadb_url_maps_to_mysql(self):
        # mariadb is grouped under the mysql family
        assert (
            make_inferrer().infer_database_type({"connection.url": "jdbc:mariadb://h:3306/d"})
            == "mysql"
        )

    def test_postgresql_url(self):
        assert (
            make_inferrer().infer_database_type({"connection.url": "jdbc:postgresql://h:5432/d"})
            == "postgresql"
        )

    def test_oracle_thin_url(self):
        assert (
            make_inferrer().infer_database_type({"connection.url": "jdbc:oracle:thin:@h:1521:orcl"})
            == "oracle"
        )

    def test_sqlserver_url(self):
        assert (
            make_inferrer().infer_database_type({"connection.url": "jdbc:sqlserver://h:1433"})
            == "sqlserver"
        )

    def test_snowflake_url(self):
        assert (
            make_inferrer().infer_database_type({"connection.url": "jdbc:snowflake://acct.snowflakecomputing.com"})
            == "snowflake"
        )

    def test_database_type_config_when_no_url(self):
        assert make_inferrer().infer_database_type({"database.type": "Postgres"}) == "postgres"

    def test_unknown_when_nothing_matches(self):
        assert make_inferrer().infer_database_type({"connection.url": "jdbc:weird://h"}) == "unknown"

    def test_no_connection_url_no_config_returns_unknown(self):
        assert make_inferrer().infer_database_type({}) == "unknown"
