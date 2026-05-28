from connect_migrate.mapper.jdbc.url_parser import JdbcUrlParser


def make_parser():
    return JdbcUrlParser()


class TestParseJdbcUrl:
    def test_mysql_standard(self):
        info = make_parser().parse_jdbc_url("jdbc:mysql://db.example.com:3306/mydb")
        assert info["host"] == "db.example.com"
        assert info["port"] == "3306"
        assert info["db.name"] == "mydb"

    def test_postgresql_standard(self):
        info = make_parser().parse_jdbc_url("jdbc:postgresql://pg.local:5432/orders")
        assert info["host"] == "pg.local"
        assert info["port"] == "5432"
        assert info["db.name"] == "orders"

    def test_sqlserver_standard(self):
        info = make_parser().parse_jdbc_url("jdbc:sqlserver://sql.example.com:1433/sales")
        assert info["host"] == "sql.example.com"
        assert info["port"] == "1433"
        assert info["db.name"] == "sales"

    def test_user_and_password_in_query(self):
        info = make_parser().parse_jdbc_url(
            "jdbc:mysql://h:3306/d?user=alice&password=secret"
        )
        assert info["user"] == "alice"
        assert info["password"] == "secret"

    def test_oracle_description_with_service_name(self):
        info = make_parser().parse_jdbc_url(
            "jdbc:oracle:thin:@(DESCRIPTION="
            "(ADDRESS=(PROTOCOL=TCPS)(HOST=oracle.example.com)(PORT=2484))"
            "(CONNECT_DATA=(SERVICE_NAME=ORCL))"
            "(SECURITY=(SSL_SERVER_CERT_DN=\"CN=oracle.example.com\")))"
        )
        assert info["host"] == "oracle.example.com"
        assert info["port"] == "2484"
        assert info["db.connection.type"].upper() == "SERVICE_NAME"
        assert info["db.name"] == "ORCL"
        assert "oracle.example.com" in info["ssl.server.cert.dn"]

    def test_no_port_returns_no_port_field(self):
        # Patterns require ``://host:port/`` — without a port the regex won't match.
        info = make_parser().parse_jdbc_url("jdbc:mysql://nodb")
        assert "port" not in info

    def test_empty_query_section_does_not_error(self):
        info = make_parser().parse_jdbc_url("jdbc:mysql://h:3306/d?")
        assert info["host"] == "h"


class TestParseMongodbUrl:
    def test_srv_with_credentials(self):
        info = make_parser().parse_mongodb_url(
            "mongodb+srv://alice:secret@cluster.mongodb.net/inventory?retryWrites=true"
        )
        assert info["user"] == "alice"
        assert info["password"] == "secret"
        assert info["host"] == "cluster.mongodb.net"
        assert info["database"] == "inventory"

    def test_plain_mongodb_with_credentials(self):
        info = make_parser().parse_mongodb_url("mongodb://bob:hunter2@mongo:27017/store")
        assert info["user"] == "bob"
        assert info["password"] == "hunter2"
        assert info["host"] == "mongo:27017"
        assert info["database"] == "store"

    def test_plain_mongodb_no_credentials(self):
        info = make_parser().parse_mongodb_url("mongodb://mongo:27017/store")
        assert "user" not in info
        assert "password" not in info
        assert info["host"] == "mongo:27017"
        assert info["database"] == "store"

    def test_unsupported_scheme_returns_empty(self):
        # Only mongodb:// and mongodb+srv:// are handled.
        info = make_parser().parse_mongodb_url("https://example.com")
        assert info == {}
