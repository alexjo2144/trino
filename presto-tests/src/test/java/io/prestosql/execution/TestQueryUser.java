/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.execution;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.client.ClientSession;
import io.prestosql.client.QueryError;
import io.prestosql.client.StatementClient;
import io.prestosql.plugin.base.security.FileBasedSystemAccessControl;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.testing.DistributedQueryRunner;
import okhttp3.OkHttpClient;
import org.testng.annotations.Test;

import java.time.ZoneId;
import java.util.Locale;
import java.util.Optional;

import static com.google.common.io.Resources.getResource;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.client.StatementClientFactory.newStatementClient;
import static io.prestosql.plugin.base.security.FileBasedAccessControlConfig.SECURITY_CONFIG_FILE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestQueryUser
{
    // On the tpch catalog Alice has allow all permissions, Bob has no permissions, and Charlie has read only permissions.
    @Test
    public void testReadAccessControl()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(TEST_SESSION)) {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            QueryError aliceQueryError = trySelectQuery("alice", queryRunner);
            assertNull(aliceQueryError);

            QueryError bobQueryError = trySelectQuery("bob", queryRunner);
            assertEquals(bobQueryError.getErrorType(), "USER_ERROR");
            assertEquals(bobQueryError.getErrorName(), "PERMISSION_DENIED");

            QueryError charlieQueryError = trySelectQuery("charlie", queryRunner);
            assertNull(charlieQueryError);
        }
    }

    private static QueryError trySelectQuery(String assumedUser, DistributedQueryRunner queryRunner)
    {
        OkHttpClient httpClient = new OkHttpClient();
        try {
            ClientSession clientSession = new ClientSession(
                    queryRunner.getCoordinator().getBaseUrl(),
                    "user",
                    assumedUser,
                    "source",
                    Optional.empty(),
                    ImmutableSet.of(),
                    null,
                    null,
                    null,
                    null,
                    ZoneId.of("America/Los_Angeles"),
                    Locale.ENGLISH,
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    null,
                    new Duration(2, MINUTES),
                    true);

            // start query
            StatementClient client = newStatementClient(httpClient, clientSession, "SELECT * FROM tpch.tiny.nation");

            // wait for query to be fully scheduled
            while (client.isRunning() && !client.currentStatusInfo().getStats().isScheduled()) {
                client.advance();
            }

            return client.currentStatusInfo().getError();
        }
        finally {
            // close the client since, query is not managed by the client protocol
            httpClient.dispatcher().executorService().shutdown();
            httpClient.connectionPool().evictAll();
        }
    }

    public static DistributedQueryRunner createQueryRunner(Session session)
            throws Exception
    {
        String securityConfigFile = getResource("access_control_rules.json").getPath();
        SystemAccessControl accessControl = new FileBasedSystemAccessControl.Factory().create(ImmutableMap.of(SECURITY_CONFIG_FILE, securityConfigFile));
        return DistributedQueryRunner.builder(session)
                .setNodeCount(1)
                .setSystemAccessControl(accessControl)
                .build();
    }
}
