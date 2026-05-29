"""Unit tests for ConfigDeriverMixin (src/comparator/config_deriver.py).

Methods run on a composed ConnectorComparator instance built via the
`make_comparator` fixture (offline, stubbed semantic matcher).

Notes:
  * `_parse_jdbc_url` / `_parse_mongodb_connection_string` lowercase the URL,
    so extracted host/user/password/db come back lowercased.
  * StubSemanticMatcher.find_best_match returns None unless `.forced_match`
    is set to the SAME dict object that lives in fm_properties_dict (the code
    identity/equality-compares dict values to find the property name).
"""

import pytest


# ---------------------------------------------------------------------------
# connection.* derivations (JDBC + MongoDB)
# ---------------------------------------------------------------------------

class TestConnectionDerivations:
    def test_host_from_jdbc(self, make_comparator):
        c = make_comparator()
        uc = {"connection.url": "jdbc:mysql://db.example.com:3306/inventory?user=admin&password=secret"}
        assert c._derive_connection_host(uc, {}) == "db.example.com"

    def test_host_from_connection_uri_mongodb(self, make_comparator):
        c = make_comparator()
        uc = {"connection.uri": "mongodb://user:pass@mongo.host:27017/mydb"}
        assert c._derive_connection_host(uc, {}) == "mongo.host:27017"

    def test_host_from_mongodb_connection_string_key(self, make_comparator):
        c = make_comparator()
        uc = {"mongodb.connection.string": "mongodb+srv://u:p@cluster.mongodb.net/db"}
        assert c._derive_connection_host(uc, {}) == "cluster.mongodb.net"

    def test_host_from_connection_string_key(self, make_comparator):
        c = make_comparator()
        uc = {"connection.string": "mongodb://noauthhost:27017/db"}
        assert c._derive_connection_host(uc, {}) == "noauthhost:27017"

    def test_host_none_when_absent(self, make_comparator):
        c = make_comparator()
        assert c._derive_connection_host({}, {}) is None

    def test_host_non_jdbc_url_ignored(self, make_comparator):
        c = make_comparator()
        # not jdbc: prefix and not a mongo key -> None
        assert c._derive_connection_host({"connection.url": "http://x"}, {}) is None

    def test_port_from_jdbc(self, make_comparator):
        c = make_comparator()
        uc = {"connection.url": "jdbc:postgresql://localhost:5432/dbname"}
        assert c._derive_connection_port(uc, {}) == "5432"

    def test_port_none_when_no_jdbc(self, make_comparator):
        c = make_comparator()
        assert c._derive_connection_port({}, {}) is None
        assert c._derive_connection_port({"connection.url": "notjdbc"}, {}) is None

    def test_user_from_jdbc(self, make_comparator):
        c = make_comparator()
        uc = {"connection.url": "jdbc:mysql://h:3306/db?user=admin&password=secret"}
        assert c._derive_connection_user(uc, {}) == "admin"

    def test_user_from_mongodb_uri(self, make_comparator):
        c = make_comparator()
        uc = {"connection.uri": "mongodb://bob:secret@h:27017/db"}
        assert c._derive_connection_user(uc, {}) == "bob"

    def test_user_from_mongo_string_key(self, make_comparator):
        c = make_comparator()
        uc = {"connection.string": "mongodb://carol:pw@h:27017/db"}
        assert c._derive_connection_user(uc, {}) == "carol"

    def test_user_none(self, make_comparator):
        c = make_comparator()
        assert c._derive_connection_user({}, {}) is None

    def test_password_from_jdbc(self, make_comparator):
        c = make_comparator()
        uc = {"connection.url": "jdbc:mysql://h:3306/db?user=admin&password=secret"}
        assert c._derive_connection_password(uc, {}) == "secret"

    def test_password_from_mongodb_uri(self, make_comparator):
        c = make_comparator()
        uc = {"connection.uri": "mongodb://bob:topsecret@h:27017/db"}
        assert c._derive_connection_password(uc, {}) == "topsecret"

    def test_password_from_mongo_string_key(self, make_comparator):
        c = make_comparator()
        uc = {"mongodb.connection.string": "mongodb+srv://bob:pw123@c.mongodb.net/db"}
        assert c._derive_connection_password(uc, {}) == "pw123"

    def test_password_none(self, make_comparator):
        c = make_comparator()
        assert c._derive_connection_password({}, {}) is None

    def test_connection_database_from_jdbc(self, make_comparator):
        c = make_comparator()
        uc = {"connection.url": "jdbc:postgresql://h:5432/inventory"}
        assert c._derive_connection_database(uc, {}) == "inventory"

    def test_connection_database_none_non_jdbc(self, make_comparator):
        c = make_comparator()
        assert c._derive_connection_database({"connection.url": "x"}, {}) is None
        assert c._derive_connection_database({}, {}) is None


# ---------------------------------------------------------------------------
# _derive_db_name
# ---------------------------------------------------------------------------

class TestDbName:
    def test_from_jdbc(self, make_comparator):
        c = make_comparator()
        uc = {"connection.url": "jdbc:postgresql://h:5432/inventory"}
        assert c._derive_db_name(uc, {}) == "inventory"

    def test_from_mongodb_uri(self, make_comparator):
        c = make_comparator()
        uc = {"connection.uri": "mongodb://h:27017/mydatabase"}
        assert c._derive_db_name(uc, {}) == "mydatabase"

    def test_from_mongo_string_key(self, make_comparator):
        c = make_comparator()
        uc = {"connection.string": "mongodb://h:27017/dbx"}
        assert c._derive_db_name(uc, {}) == "dbx"

    def test_direct_db_name(self, make_comparator):
        c = make_comparator()
        assert c._derive_db_name({"db.name": "direct"}, {}) == "direct"

    def test_direct_database(self, make_comparator):
        c = make_comparator()
        assert c._derive_db_name({"database": "dbval"}, {}) == "dbval"

    def test_none(self, make_comparator):
        c = make_comparator()
        assert c._derive_db_name({}, {}) is None


# ---------------------------------------------------------------------------
# _derive_db_connection_type, ssl_server_cert_dn, database_server_name
# ---------------------------------------------------------------------------

class TestOracleAndDirect:
    ORACLE = ("jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)"
              "(HOST=oradb.example.com)(PORT=1521))(CONNECT_DATA="
              "(SERVICE_NAME=orclpdb))(SECURITY=(SSL_SERVER_CERT_DN="
              '"CN=oracle")))')

    def test_db_connection_type_from_jdbc_oracle(self, make_comparator):
        c = make_comparator()
        # Oracle branch matches against original_url with IGNORECASE, preserving case
        val = c._derive_db_connection_type({"connection.url": self.ORACLE}, {})
        assert val == "SERVICE_NAME"

    def test_db_connection_type_direct(self, make_comparator):
        c = make_comparator()
        assert c._derive_db_connection_type({"db.connection.type": "SID"}, {}) == "SID"

    def test_db_connection_type_none(self, make_comparator):
        c = make_comparator()
        assert c._derive_db_connection_type({}, {}) is None
        # standard jdbc has no db.connection.type
        assert c._derive_db_connection_type({"connection.url": "jdbc:mysql://h:3306/d"}, {}) is None

    def test_ssl_server_cert_dn_from_jdbc(self, make_comparator):
        c = make_comparator()
        val = c._derive_ssl_server_cert_dn({"connection.url": self.ORACLE}, {})
        assert val == 'CN=oracle'

    def test_ssl_server_cert_dn_direct(self, make_comparator):
        c = make_comparator()
        assert c._derive_ssl_server_cert_dn({"ssl.server.cert.dn": "CN=x"}, {}) == "CN=x"

    def test_ssl_server_cert_dn_none(self, make_comparator):
        c = make_comparator()
        assert c._derive_ssl_server_cert_dn({}, {}) is None

    def test_database_server_name_from_jdbc(self, make_comparator):
        c = make_comparator()
        uc = {"connection.url": "jdbc:postgresql://serverhost:5432/db"}
        assert c._derive_database_server_name(uc, {}) == "serverhost"

    def test_database_server_name_direct(self, make_comparator):
        c = make_comparator()
        assert c._derive_database_server_name({"database.server.name": "srv"}, {}) == "srv"

    def test_database_server_name_server_name(self, make_comparator):
        c = make_comparator()
        assert c._derive_database_server_name({"server.name": "srv2"}, {}) == "srv2"

    def test_database_server_name_none(self, make_comparator):
        c = make_comparator()
        assert c._derive_database_server_name({}, {}) is None


# ---------------------------------------------------------------------------
# format derivations (converter -> format)
# ---------------------------------------------------------------------------

class TestFormatDerivations:
    AVRO = "io.confluent.connect.avro.AvroConverter"
    JSON = "org.apache.kafka.connect.json.JsonConverter"
    STRING = "org.apache.kafka.connect.storage.StringConverter"

    def test_input_key_format_known_converter(self, make_comparator):
        c = make_comparator()
        assert c._derive_input_key_format({"key.converter": self.AVRO}, {}) == "AVRO"

    def test_input_key_format_unknown_converter_returned_asis(self, make_comparator):
        c = make_comparator()
        assert c._derive_input_key_format({"key.converter": "com.x.Y"}, {}) == "com.x.Y"

    def test_input_key_format_direct_key(self, make_comparator):
        c = make_comparator()
        assert c._derive_input_key_format({"key.format": "PROTOBUF"}, {}) == "PROTOBUF"

    def test_input_key_format_schema_registry_infer(self, make_comparator):
        c = make_comparator()
        assert c._derive_input_key_format({"key.converter.schemas.enable": "true"}, {}) == "JSON_SR"

    def test_input_key_format_template_default(self, make_comparator):
        c = make_comparator()
        defs = [{"name": "input.key.format", "default_value": "AVRO"}]
        assert c._derive_input_key_format({}, {}, defs) == "AVRO"

    def test_input_key_format_default_fallback(self, make_comparator):
        c = make_comparator()
        assert c._derive_input_key_format({}, {}) == "JSON"

    def test_input_data_format_value_converter(self, make_comparator):
        c = make_comparator()
        assert c._derive_input_data_format({"value.converter": self.STRING}, {}) == "STRING"

    def test_input_data_format_direct(self, make_comparator):
        c = make_comparator()
        assert c._derive_input_data_format({"value.format": "AVRO"}, {}) == "AVRO"

    def test_input_data_format_schema_infer(self, make_comparator):
        c = make_comparator()
        assert c._derive_input_data_format({"value.converter.schemas.enable": "1"}, {}) == "JSON_SR"

    def test_input_data_format_default(self, make_comparator):
        c = make_comparator()
        assert c._derive_input_data_format({}, {}) == "JSON"

    def test_output_key_format_converter(self, make_comparator):
        c = make_comparator()
        assert c._derive_output_key_format({"key.converter": self.JSON}, {}) == "JSON"

    def test_output_key_format_direct(self, make_comparator):
        c = make_comparator()
        assert c._derive_output_key_format({"output.key.format": "AVRO"}, {}) == "AVRO"

    def test_output_key_format_template_default(self, make_comparator):
        c = make_comparator()
        defs = [{"name": "output.key.format", "default_value": "PROTOBUF"}]
        assert c._derive_output_key_format({}, {}, defs) == "PROTOBUF"

    def test_output_key_format_default(self, make_comparator):
        c = make_comparator()
        assert c._derive_output_key_format({}, {}) == "JSON"

    def test_output_data_format_converter(self, make_comparator):
        c = make_comparator()
        assert c._derive_output_data_format({"value.converter": self.AVRO}, {}) == "AVRO"

    def test_output_data_format_direct(self, make_comparator):
        c = make_comparator()
        assert c._derive_output_data_format({"output.data.format": "JSON_SR"}, {}) == "JSON_SR"

    def test_output_data_format_default(self, make_comparator):
        c = make_comparator()
        assert c._derive_output_data_format({}, {}) == "JSON"

    def test_output_data_key_format_converter(self, make_comparator):
        c = make_comparator()
        assert c._derive_output_data_key_format({"key.converter": self.AVRO}, {}) == "AVRO"

    def test_output_data_key_format_direct(self, make_comparator):
        c = make_comparator()
        assert c._derive_output_data_key_format({"output.data.key.format": "AVRO"}, {}) == "AVRO"

    def test_output_data_key_format_from_fm_output_key(self, make_comparator):
        c = make_comparator()
        assert c._derive_output_data_key_format({}, {"output.key.format": "PROTOBUF"}) == "PROTOBUF"

    def test_output_data_key_format_template_default(self, make_comparator):
        c = make_comparator()
        defs = [{"name": "output.data.key.format", "default_value": "JSON_SR"}]
        assert c._derive_output_data_key_format({}, {}, defs) == "JSON_SR"

    def test_output_data_key_format_default(self, make_comparator):
        c = make_comparator()
        assert c._derive_output_data_key_format({}, {}) == "JSON"

    def test_output_data_value_format_converter(self, make_comparator):
        c = make_comparator()
        assert c._derive_output_data_value_format({"value.converter": self.STRING}, {}) == "STRING"

    def test_output_data_value_format_direct(self, make_comparator):
        c = make_comparator()
        assert c._derive_output_data_value_format({"output.data.value.format": "AVRO"}, {}) == "AVRO"

    def test_output_data_value_format_from_fm_output_data(self, make_comparator):
        c = make_comparator()
        assert c._derive_output_data_value_format({}, {"output.data.format": "AVRO"}) == "AVRO"

    def test_output_data_value_format_default(self, make_comparator):
        c = make_comparator()
        assert c._derive_output_data_value_format({}, {}) == "JSON"


# ---------------------------------------------------------------------------
# authentication / csfle
# ---------------------------------------------------------------------------

class TestAuthAndCsfle:
    def test_auth_plain(self, make_comparator):
        c = make_comparator()
        assert c._derive_authentication_method({"sasl.mechanism": "PLAIN"}, {}) == "PLAIN"

    def test_auth_scram(self, make_comparator):
        c = make_comparator()
        assert c._derive_authentication_method({"sasl.mechanism": "SCRAM-SHA-256"}, {}) == "SCRAM"

    def test_auth_oauth(self, make_comparator):
        c = make_comparator()
        assert c._derive_authentication_method({"authentication.type": "oauthbearer"}, {}) == "OAUTHBEARER"

    def test_auth_bearer(self, make_comparator):
        c = make_comparator()
        assert c._derive_authentication_method({"auth.method": "bearer-token"}, {}) == "OAUTHBEARER"

    def test_auth_ssl(self, make_comparator):
        c = make_comparator()
        assert c._derive_authentication_method({"security.protocol": "SSL"}, {}) == "SSL"

    def test_auth_other_returns_lowercased(self, make_comparator):
        c = make_comparator()
        assert c._derive_authentication_method({"auth.method": "Kerberos"}, {}) == "kerberos"

    def test_auth_template_default(self, make_comparator):
        c = make_comparator()
        defs = [{"name": "authentication.method", "default_value": "GSSAPI"}]
        assert c._derive_authentication_method({}, {}, defs) == "GSSAPI"

    def test_auth_default(self, make_comparator):
        c = make_comparator()
        assert c._derive_authentication_method({}, {}) == "PLAIN"

    def test_csfle_enabled_template_default(self, make_comparator):
        c = make_comparator()
        defs = [{"name": "csfle.enabled", "default_value": "true"}]
        assert c._derive_csfle_enabled({}, {}, defs) == "true"

    def test_csfle_enabled_default(self, make_comparator):
        c = make_comparator()
        assert c._derive_csfle_enabled({}, {}) == "false"

    def test_csfle_on_failure_template_default(self, make_comparator):
        c = make_comparator()
        defs = [{"name": "csfle.onFailure", "default_value": "CONTINUE"}]
        assert c._derive_csfle_on_failure({}, {}, defs) == "CONTINUE"

    def test_csfle_on_failure_default(self, make_comparator):
        c = make_comparator()
        assert c._derive_csfle_on_failure({}, {}) == "FAIL"


# ---------------------------------------------------------------------------
# ssl.mode
# ---------------------------------------------------------------------------

class TestSslMode:
    def test_direct_prefer(self, make_comparator):
        c = make_comparator()
        assert c._derive_ssl_mode({"ssl.mode": "preferred"}, {}) == "prefer"

    def test_direct_require(self, make_comparator):
        c = make_comparator()
        assert c._derive_ssl_mode({"ssl.mode": "required"}, {}) == "require"

    def test_direct_verify_ca(self, make_comparator):
        c = make_comparator()
        assert c._derive_ssl_mode({"ssl.mode": "verify_ca"}, {}) == "verify-ca"

    def test_direct_verify_full(self, make_comparator):
        c = make_comparator()
        assert c._derive_ssl_mode({"ssl.mode": "verifyfull"}, {}) == "verify-full"

    def test_direct_disabled(self, make_comparator):
        c = make_comparator()
        assert c._derive_ssl_mode({"ssl.mode": "none"}, {}) == "disabled"

    def test_db_specific_bool_true(self, make_comparator):
        c = make_comparator()
        assert c._derive_ssl_mode({"connection.sslmode": "true"}, {}) == "require"

    def test_db_specific_bool_false(self, make_comparator):
        c = make_comparator()
        assert c._derive_ssl_mode({"use.ssl": "no"}, {}) == "disabled"

    def test_db_specific_string_verify_full(self, make_comparator):
        c = make_comparator()
        assert c._derive_ssl_mode({"database.ssl.mode": "verify-full"}, {}) == "verify-full"

    def test_ssl_indicator_cert(self, make_comparator):
        c = make_comparator()
        assert c._derive_ssl_mode({"ssl.cert.file": "verify-cert.pem"}, {}) == "verify-ca"

    def test_ssl_indicator_plain_file(self, make_comparator):
        c = make_comparator()
        assert c._derive_ssl_mode({"ssl.truststore.file": "/tmp/ts.jks"}, {}) == "require"

    def test_url_sslmode_prefer(self, make_comparator):
        c = make_comparator()
        uc = {"connection.url": "jdbc:postgresql://h:5432/d?sslmode=prefer"}
        assert c._derive_ssl_mode(uc, {}) == "prefer"

    def test_url_ssl_true_default_require(self, make_comparator):
        c = make_comparator()
        uc = {"connection.url": "jdbc:mysql://h:3306/d?ssl=true"}
        assert c._derive_ssl_mode(uc, {}) == "require"

    def test_template_default(self, make_comparator):
        c = make_comparator()
        defs = [{"name": "ssl.mode", "default_value": "require"}]
        assert c._derive_ssl_mode({}, {}, defs) == "require"

    def test_default_prefer(self, make_comparator):
        c = make_comparator()
        assert c._derive_ssl_mode({}, {}) == "prefer"


# ---------------------------------------------------------------------------
# redis
# ---------------------------------------------------------------------------

class TestRedis:
    def test_hostname_direct(self, make_comparator):
        c = make_comparator()
        assert c._derive_redis_hostname({"redis.hostname": "rh"}, {}) == "rh"

    def test_hostname_from_host_with_port(self, make_comparator):
        c = make_comparator()
        assert c._derive_redis_hostname({"redis.host": "myredis:6379"}, {}) == "myredis"

    def test_hostname_from_plain_host(self, make_comparator):
        c = make_comparator()
        assert c._derive_redis_hostname({"host": "plainhost"}, {}) == "plainhost"

    def test_hostname_from_redis_hosts(self, make_comparator):
        c = make_comparator()
        assert c._derive_redis_hostname({"redis.hosts": "h1:6380"}, {}) == "h1"

    def test_hostname_from_redis_url_with_auth(self, make_comparator):
        c = make_comparator()
        uc = {"connection.url": "redis://user:pass@redishost:6379/0"}
        assert c._derive_redis_hostname(uc, {}) == "redishost"

    def test_hostname_from_redis_url_no_auth(self, make_comparator):
        c = make_comparator()
        uc = {"redis.connection.url": "redis://plainredis:6379/0"}
        assert c._derive_redis_hostname(uc, {}) == "plainredis"

    def test_hostname_none(self, make_comparator):
        c = make_comparator()
        assert c._derive_redis_hostname({}, {}) is None

    def test_port_direct(self, make_comparator):
        c = make_comparator()
        assert c._derive_redis_portnumber({"redis.portnumber": "1234"}, {}) == "1234"

    def test_port_from_redis_port(self, make_comparator):
        c = make_comparator()
        assert c._derive_redis_portnumber({"redis.port": "6390"}, {}) == "6390"

    def test_port_from_redis_hosts(self, make_comparator):
        c = make_comparator()
        assert c._derive_redis_portnumber({"redis.hosts": "h:6381/0"}, {}) == "6381"

    def test_port_from_redis_url_auth(self, make_comparator):
        c = make_comparator()
        uc = {"connection.url": "redis://u:p@h:6382/0"}
        assert c._derive_redis_portnumber(uc, {}) == "6382"

    def test_port_from_redis_url_no_auth(self, make_comparator):
        c = make_comparator()
        uc = {"connection.uri": "redis://h:6383/0"}
        assert c._derive_redis_portnumber(uc, {}) == "6383"

    def test_port_template_default(self, make_comparator):
        c = make_comparator()
        defs = [{"name": "redis.portnumber", "default_value": "7000"}]
        assert c._derive_redis_portnumber({}, {}, defs) == "7000"

    def test_port_default(self, make_comparator):
        c = make_comparator()
        assert c._derive_redis_portnumber({}, {}) == "6379"

    def test_ssl_mode_direct_enabled(self, make_comparator):
        c = make_comparator()
        assert c._derive_redis_ssl_mode({"redis.ssl.mode": "true"}, {}) == "enabled"

    def test_ssl_mode_direct_disabled(self, make_comparator):
        c = make_comparator()
        assert c._derive_redis_ssl_mode({"redis.ssl.mode": "off"}, {}) == "disabled"

    def test_ssl_mode_direct_server(self, make_comparator):
        c = make_comparator()
        assert c._derive_redis_ssl_mode({"redis.ssl.mode": "verify-server"}, {}) == "server"

    def test_ssl_mode_direct_server_client(self, make_comparator):
        c = make_comparator()
        assert c._derive_redis_ssl_mode({"redis.ssl.mode": "mutual"}, {}) == "server+client"

    def test_ssl_mode_direct_passthrough(self, make_comparator):
        c = make_comparator()
        assert c._derive_redis_ssl_mode({"redis.ssl.mode": "Custom"}, {}) == "Custom"

    def test_ssl_mode_flag_enabled(self, make_comparator):
        c = make_comparator()
        assert c._derive_redis_ssl_mode({"redis.ssl.enabled": "yes"}, {}) == "enabled"

    def test_ssl_mode_flag_disabled(self, make_comparator):
        c = make_comparator()
        assert c._derive_redis_ssl_mode({"use.ssl": "0"}, {}) == "disabled"

    def test_ssl_mode_indicator_keystore(self, make_comparator):
        c = make_comparator()
        assert c._derive_redis_ssl_mode({"redis.ssl.keystore.file": "ks.jks"}, {}) == "server+client"

    def test_ssl_mode_indicator_server_cert(self, make_comparator):
        c = make_comparator()
        assert c._derive_redis_ssl_mode({"redis.ssl.ca.file": "ca.pem"}, {}) == "server"

    def test_ssl_mode_url_rediss(self, make_comparator):
        c = make_comparator()
        assert c._derive_redis_ssl_mode({"connection.url": "rediss://h:6379"}, {}) == "enabled"

    def test_ssl_mode_url_redis_ssl_true(self, make_comparator):
        c = make_comparator()
        assert c._derive_redis_ssl_mode({"connection.url": "redis://h:6379?ssl=true"}, {}) == "enabled"

    def test_ssl_mode_template_default(self, make_comparator):
        c = make_comparator()
        defs = [{"name": "redis.ssl.mode", "default_value": "server"}]
        assert c._derive_redis_ssl_mode({}, {}, defs) == "server"

    def test_ssl_mode_default(self, make_comparator):
        c = make_comparator()
        assert c._derive_redis_ssl_mode({}, {}) == "disabled"


# ---------------------------------------------------------------------------
# Azure service bus
# ---------------------------------------------------------------------------

class TestServiceBus:
    CONN = ("Endpoint=sb://mybus.servicebus.windows.net/;"
            "SharedAccessKeyName=RootKey;SharedAccessKey=abc123==;"
            "EntityPath=myqueue")

    def test_namespace_direct(self, make_comparator):
        c = make_comparator()
        assert c._derive_servicebus_namespace({"azure.servicebus.namespace": "ns"}, {}) == "ns"

    def test_namespace_from_conn_str(self, make_comparator):
        c = make_comparator()
        assert c._derive_servicebus_namespace({"azure.servicebus.connection.string": self.CONN}, {}) == "mybus"

    def test_namespace_none(self, make_comparator):
        c = make_comparator()
        assert c._derive_servicebus_namespace({}, {}) is None

    def test_keyname_direct(self, make_comparator):
        c = make_comparator()
        assert c._derive_azure_servicebus_sas_keyname({"azure.servicebus.sas.keyname": "K"}, {}) == "K"

    def test_keyname_from_conn_str(self, make_comparator):
        c = make_comparator()
        assert c._derive_azure_servicebus_sas_keyname({"azure.servicebus.connection.string": self.CONN}, {}) == "RootKey"

    def test_keyname_none(self, make_comparator):
        c = make_comparator()
        assert c._derive_azure_servicebus_sas_keyname({}, {}) is None

    def test_key_direct(self, make_comparator):
        c = make_comparator()
        assert c._derive_azure_servicebus_sas_key({"azure.servicebus.sas.key": "secretkey"}, {}) == "secretkey"

    def test_key_from_conn_str(self, make_comparator):
        c = make_comparator()
        assert c._derive_azure_servicebus_sas_key({"azure.servicebus.connection.string": self.CONN}, {}) == "abc123=="

    def test_key_none(self, make_comparator):
        c = make_comparator()
        assert c._derive_azure_servicebus_sas_key({}, {}) is None

    def test_entity_direct(self, make_comparator):
        c = make_comparator()
        assert c._derive_azure_servicebus_entity_name({"azure.servicebus.entity.name": "ent"}, {}) == "ent"

    def test_entity_from_conn_str(self, make_comparator):
        c = make_comparator()
        assert c._derive_azure_servicebus_entity_name({"azure.servicebus.connection.string": self.CONN}, {}) == "myqueue"

    def test_entity_none(self, make_comparator):
        c = make_comparator()
        assert c._derive_azure_servicebus_entity_name({}, {}) is None


# ---------------------------------------------------------------------------
# subject name strategies
# ---------------------------------------------------------------------------

class TestSubjectNameStrategy:
    def test_match_from_fallback_with_fqcn(self, make_comparator):
        c = make_comparator()
        uc = {"key.subject.name.strategy": "io.confluent.kafka.serializers.subject.TopicNameStrategy"}
        val = c._derive_subject_name_strategy(uc, {}, None, "key.subject.name.strategy")
        assert val == "TopicNameStrategy"

    def test_match_from_template_recommended(self, make_comparator):
        c = make_comparator()
        defs = [{"name": "sns", "recommended_values": ["CustomStrategy"]}]
        uc = {"sns": "com.x.CustomStrategy"}
        assert c._derive_subject_name_strategy(uc, {}, defs, "sns") == "CustomStrategy"

    def test_no_match_returns_none(self, make_comparator):
        c = make_comparator()
        uc = {"sns": "com.x.UnknownStrategy"}
        assert c._derive_subject_name_strategy(uc, {}, None, "sns") is None

    def test_config_name_absent(self, make_comparator):
        c = make_comparator()
        assert c._derive_subject_name_strategy({}, {}, None, "sns") is None

    def test_no_config_name(self, make_comparator):
        c = make_comparator()
        assert c._derive_subject_name_strategy({"sns": "x"}, {}) is None

    def test_reference_match_fallback(self, make_comparator):
        c = make_comparator()
        uc = {"ref": "com.x.QualifiedReferenceSubjectNameStrategy"}
        val = c._derive_reference_subject_name_strategy(uc, {}, None, "ref")
        assert val == "QualifiedReferenceSubjectNameStrategy"

    def test_reference_no_match(self, make_comparator):
        c = make_comparator()
        assert c._derive_reference_subject_name_strategy({"ref": "Nope"}, {}, None, "ref") is None


# ---------------------------------------------------------------------------
# _apply_reverse_switch
# ---------------------------------------------------------------------------

class TestApplyReverseSwitch:
    def test_match(self, make_comparator):
        c = make_comparator()
        mapping = {"HIGH": "userhigh", "LOW": "userlow"}
        assert c._apply_reverse_switch(mapping, "userlow") == "LOW"

    def test_no_match(self, make_comparator):
        c = make_comparator()
        assert c._apply_reverse_switch({"A": "x"}, "y") is None

    def test_default_key_returns_none(self, make_comparator):
        c = make_comparator()
        assert c._apply_reverse_switch({"default": "dv"}, "dv") is None


# ---------------------------------------------------------------------------
# _do_semantic_matching
# ---------------------------------------------------------------------------

class TestDoSemanticMatching:
    def test_empty_list_noop(self, make_comparator):
        c = make_comparator()
        fm = {}
        c._do_semantic_matching(fm, set(), {}, [], {})
        assert fm == {}

    def test_forced_match_writes_fm(self, make_comparator):
        c = make_comparator()
        fm_prop = {"name": "fm.target", "type": "STRING"}
        template_defs = [fm_prop]
        c.semantic_matcher.forced_match = fm_prop
        fm = {}
        c._do_semantic_matching(fm, {"user.key"}, {"user.key": "v1"}, template_defs, {})
        assert fm == {"fm.target": "v1"}

    def test_already_present_skipped(self, make_comparator):
        c = make_comparator()
        fm_prop = {"name": "fm.target", "type": "STRING"}
        template_defs = [fm_prop]
        c.semantic_matcher.forced_match = fm_prop
        fm = {"fm.target": "existing"}
        c._do_semantic_matching(fm, {"user.key"}, {"user.key": "v1"}, template_defs, {})
        assert fm == {"fm.target": "existing"}

    def test_config_not_in_user_configs(self, make_comparator):
        c = make_comparator()
        fm_prop = {"name": "fm.target"}
        c.semantic_matcher.forced_match = fm_prop
        fm = {}
        c._do_semantic_matching(fm, {"user.key"}, {}, [fm_prop], {})
        assert fm == {}

    def test_no_match_returns_none(self, make_comparator):
        c = make_comparator()
        fm_prop = {"name": "fm.target"}
        # forced_match stays None -> no match
        fm = {}
        c._do_semantic_matching(fm, {"user.key"}, {"user.key": "v"}, [fm_prop], {})
        assert fm == {}


# ---------------------------------------------------------------------------
# _check_required_configs
# ---------------------------------------------------------------------------

class TestCheckRequiredConfigs:
    def test_missing_required_bool_appends_error(self, make_comparator):
        c = make_comparator()
        errors = []
        defs = [{"name": "req.cfg", "required": True}]
        c._check_required_configs({}, defs, errors)
        assert any("req.cfg" in e for e in errors)

    def test_missing_required_string_true_appends_error(self, make_comparator):
        c = make_comparator()
        errors = []
        defs = [{"name": "req.cfg", "required": "true"}]
        c._check_required_configs({}, defs, errors)
        assert len(errors) == 1

    def test_missing_required_with_default_uses_default(self, make_comparator):
        c = make_comparator()
        errors = []
        fm = {}
        defs = [{"name": "req.cfg", "required": True, "default_value": "dv"}]
        c._check_required_configs(fm, defs, errors)
        assert fm["req.cfg"] == "dv"
        assert errors == []

    def test_internal_skipped(self, make_comparator):
        c = make_comparator()
        errors = []
        defs = [{"name": "req.cfg", "required": True, "internal": True}]
        c._check_required_configs({}, defs, errors)
        assert errors == []

    def test_present_required_no_error(self, make_comparator):
        c = make_comparator()
        errors = []
        defs = [{"name": "req.cfg", "required": True}]
        c._check_required_configs({"req.cfg": "v"}, defs, errors)
        assert errors == []

    def test_not_required_no_error(self, make_comparator):
        c = make_comparator()
        errors = []
        defs = [{"name": "opt.cfg", "required": False}]
        c._check_required_configs({}, defs, errors)
        assert errors == []

    def test_recommended_values_violation(self, make_comparator):
        c = make_comparator()
        errors = []
        defs = [{"name": "fmt", "recommended_values": ["AVRO", "JSON"]}]
        c._check_required_configs({"fmt": "XML"}, defs, errors)
        assert any("recommended values" in e for e in errors)

    def test_recommended_values_case_insensitive_ok(self, make_comparator):
        c = make_comparator()
        errors = []
        defs = [{"name": "fmt", "recommended_values": ["AVRO"]}]
        c._check_required_configs({"fmt": "avro"}, defs, errors)
        assert errors == []

    def test_no_duplicate_errors(self, make_comparator):
        c = make_comparator()
        errors = []
        defs = [{"name": "req.cfg", "required": True}, {"name": "req.cfg", "required": True}]
        c._check_required_configs({}, defs, errors)
        assert len(errors) == 1


# ---------------------------------------------------------------------------
# _get_sm_property_from_template
# ---------------------------------------------------------------------------

class TestGetSmProperty:
    def test_from_configs(self, make_comparator):
        c = make_comparator()
        prop = {"name": "x", "type": "STRING"}
        tmpl = {"configs": [prop, {"name": "y"}]}
        assert c._get_sm_property_from_template("x", tmpl) is prop

    def test_from_groups(self, make_comparator):
        c = make_comparator()
        prop = {"name": "g1"}
        tmpl = {"groups": [{"name": "G", "configs": [prop]}]}
        assert c._get_sm_property_from_template("g1", tmpl) is prop

    def test_from_sections(self, make_comparator):
        c = make_comparator()
        prop = {"name": "s1"}
        tmpl = {"sections": [{"name": "S", "config_defs": [prop]}]}
        assert c._get_sm_property_from_template("s1", tmpl) is prop

    def test_none_when_empty(self, make_comparator):
        c = make_comparator()
        assert c._get_sm_property_from_template("x", {}) is None
        assert c._get_sm_property_from_template("x", None) is None

    def test_none_when_absent(self, make_comparator):
        c = make_comparator()
        tmpl = {"configs": [{"name": "other"}]}
        assert c._get_sm_property_from_template("x", tmpl) is None


# ---------------------------------------------------------------------------
# _load_semantic_matcher_from_path
# ---------------------------------------------------------------------------

class TestLoadSemanticMatcherFromPath:
    def test_none_path_returns(self, make_comparator):
        c = make_comparator()
        c.semantic_matcher_path = None
        # should simply return without error
        assert c._load_semantic_matcher_from_path() is None

    def test_nonexistent_path_warns_no_crash(self, make_comparator, tmp_path):
        c = make_comparator()
        c.semantic_matcher_path = str(tmp_path / "does_not_exist.py")
        assert c._load_semantic_matcher_from_path() is None


# ---------------------------------------------------------------------------
# placeholder / template default helpers
# ---------------------------------------------------------------------------

class TestPlaceholderHelpers:
    def test_is_placeholder_true(self, make_comparator):
        c = make_comparator()
        assert c._is_placeholder("${foo}") is True

    def test_is_placeholder_false(self, make_comparator):
        c = make_comparator()
        assert c._is_placeholder("foo") is False

    def test_extract_placeholder_name_with_brace(self, make_comparator):
        c = make_comparator()
        assert c._extract_placeholder_name("${connection.host}") == "connection.host"

    def test_extract_placeholder_name_no_close_brace(self, make_comparator):
        c = make_comparator()
        assert c._extract_placeholder_name("${nobrace") == "nobrace"

    def test_extract_placeholder_name_non_placeholder(self, make_comparator):
        c = make_comparator()
        assert c._extract_placeholder_name("plain") == "plain"

    def test_resolve_template_default_placeholder_resolved(self, make_comparator):
        c = make_comparator()
        fm = {"connection.host": "resolved.host"}
        assert c._resolve_template_default("${connection.host}", fm) == "resolved.host"

    def test_resolve_template_default_placeholder_missing(self, make_comparator):
        c = make_comparator()
        assert c._resolve_template_default("${missing}", {}) is None

    def test_resolve_template_default_non_placeholder(self, make_comparator):
        c = make_comparator()
        assert c._resolve_template_default("literal", {}) == "literal"

    def test_get_template_default_value_found(self, make_comparator):
        c = make_comparator()
        defs = [{"name": "x", "default_value": 5}]
        assert c._get_template_default_value(defs, "x") == "5"

    def test_get_template_default_value_none_when_no_default(self, make_comparator):
        c = make_comparator()
        defs = [{"name": "x"}]
        assert c._get_template_default_value(defs, "x") is None

    def test_get_template_default_value_not_found(self, make_comparator):
        c = make_comparator()
        defs = [{"name": "y", "default_value": "v"}]
        assert c._get_template_default_value(defs, "x") is None


# ---------------------------------------------------------------------------
# _derive_connection_url (Snowflake)
# ---------------------------------------------------------------------------

class TestDeriveConnectionUrl:
    def test_snowflake_extraction(self, make_comparator):
        c = make_comparator()
        uc = {"connection.url": "jdbc:snowflake://acct.snowflakecomputing.com/?db=X"}
        assert c._derive_connection_url(uc, {}) == "acct.snowflakecomputing.com/?db=X"

    def test_non_snowflake_jdbc_returns_none(self, make_comparator):
        c = make_comparator()
        uc = {"connection.url": "jdbc:mysql://h:3306/d"}
        assert c._derive_connection_url(uc, {}) is None

    def test_template_default_fallback(self, make_comparator):
        c = make_comparator()
        defs = [{"name": "connection.url", "default_value": "default.url"}]
        assert c._derive_connection_url({}, {}, defs) == "default.url"

    def test_none_when_absent(self, make_comparator):
        c = make_comparator()
        assert c._derive_connection_url({}, {}) is None
