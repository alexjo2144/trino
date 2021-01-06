package io.prestosql.jdbc;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import io.prestosql.plugin.password.PasswordAuthenticatorPlugin;
import io.prestosql.server.security.PasswordAuthenticatorManager;
import io.prestosql.server.testing.TestingPrestoServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import static com.google.common.io.Resources.getResource;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestPrestoDriverAssumeUser
{
    private final String USER = "user";
    private final String PASSWORD = "password";

    private TestingPrestoServer server;

    @BeforeClass
    public void setup()
            throws Exception
    {
        server = TestingPrestoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .put("http-server.authentication.type", "password")
                        .put("http-server.https.enabled", "true")
                        .put("http-server.https.keystore.path", getResource("localhost.keystore").getPath())
                        .put("http-server.https.keystore.key", "changeit")
                        .build())
                .build();

        server.installPlugin(new PasswordAuthenticatorPlugin());
        server.getInstance(Key.get(PasswordAuthenticatorManager.class)).loadPasswordAuthenticator();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
            throws Exception
    {
        server.close();
    }

    @Test
    public void testInvalidCredentials()
    {
        assertThrows(() -> trySelectCurrentUser(ImmutableMap.of()));
        assertThrows(() -> trySelectCurrentUser(ImmutableMap.of("user", "invalidUser", "password", PASSWORD)));
        assertThrows(() -> trySelectCurrentUser(ImmutableMap.of("user", USER, "password", "invalidPassword")));
        assertThrows(() -> trySelectCurrentUser(ImmutableMap.of("user", "invalidUser", "password", PASSWORD, "requestedUser", USER)));
    }

    @Test
    public void testQueryUserNotSpecified()
            throws SQLException
    {
        assertEquals(trySelectCurrentUser(ImmutableMap.of("user", USER, "password", PASSWORD)), USER);
    }

    @Test
    public void testAssumeUser()
            throws SQLException
    {
        assertEquals(trySelectCurrentUser(ImmutableMap.of("user", USER, "password", PASSWORD, "requestedUser", "differentUser")), "differentUser");
    }

    private String trySelectCurrentUser(Map<String, String> additionalProperties)
            throws SQLException
    {
        try (Connection connection = createConnection(additionalProperties);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT current_user")) {
            assertTrue(resultSet.next());
            return resultSet.getString(1);
        }
    }

    private Connection createConnection(Map<String, String> additionalProperties)
            throws SQLException
    {
        String url = format("jdbc:presto://localhost:%s", server.getHttpsAddress().getPort());
        Properties properties = new Properties();
        properties.setProperty("SSL", "true");
        properties.setProperty("SSLTrustStorePath", getResource("localhost.truststore").getPath());
        properties.setProperty("SSLTrustStorePassword", "changeit");
        additionalProperties.forEach(properties::setProperty);
        return DriverManager.getConnection(url, properties);
    }
}
