/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule;

import org.opensearch.Version;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchScrollAction;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.rule.RuleAttribute;
import org.opensearch.rule.action.CreateRuleRequest;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkloadGroupRuleRoutingServiceTests extends OpenSearchTestCase {

    public void testValidateActionAttributeIgnoresRulesWithoutAction() {
        WorkloadGroupRuleRoutingService service = newService(Version.V_3_9_0);
        service.validateActionAttribute(Map.of(RuleAttribute.INDEX_PATTERN, Set.of("logs*")));
        service.validateActionAttribute(null);
    }

    public void testValidateActionAttributeRejectsMixedVersionCluster() {
        WorkloadGroupRuleRoutingService service = newService(Version.V_3_9_0);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> service.validateActionAttribute(Map.of(RuleAttribute.ACTION, Set.of(SearchAction.NAME)))
        );
        assertTrue(e.getMessage(), e.getMessage().contains(Version.V_3_9_0.toString()));
    }

    public void testValidateActionAttributeAcceptsTransportActionNames() {
        WorkloadGroupRuleRoutingService service = newService(WorkloadGroupRuleRoutingService.ACTION_ATTRIBUTE_MIN_VERSION);
        service.validateActionAttribute(Map.of(RuleAttribute.ACTION, Set.of("indices:data/read/msearch", "indices:data/write/*")));
    }

    public void testValidateActionAttributeRejectsValuesThatNeverMatch() {
        WorkloadGroupRuleRoutingService service = newService(WorkloadGroupRuleRoutingService.ACTION_ATTRIBUTE_MIN_VERSION);
        for (String value : List.of("_msearch", "indices:data/write/bulk[s]", SearchScrollAction.NAME, SearchScrollAction.NAME + "*")) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> service.validateActionAttribute(Map.of(RuleAttribute.ACTION, Set.of(value)))
            );
            assertTrue(e.getMessage(), e.getMessage().contains("[" + value + "]"));
        }
    }

    public void testCreateRuleRequestFailsForInvalidActionAttribute() {
        WorkloadGroupRuleRoutingService service = newService(WorkloadGroupRuleRoutingService.ACTION_ATTRIBUTE_MIN_VERSION);
        Rule rule = Rule.builder()
            .id("action_rule")
            .description("msearch traffic")
            .featureType(new WorkloadGroupFeatureType(featureValue -> {}, new HashMap<>()))
            .featureValue("msearch_group")
            .attributeMap(Map.of(RuleAttribute.ACTION, Set.of("_msearch")))
            .updatedAt("2025-02-24T07:42:10.123456Z")
            .build();
        AtomicReference<Exception> failure = new AtomicReference<>();
        service.handleCreateRuleRequest(new CreateRuleRequest(rule), ActionListener.wrap(r -> fail("expected failure"), failure::set));
        assertTrue(failure.get() instanceof IllegalArgumentException);
    }

    private static WorkloadGroupRuleRoutingService newService(Version oldestNodeVersion) {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(new DiscoveryNode("old", buildNewFakeTransportAddress(), oldestNodeVersion))
            .add(new DiscoveryNode("new", buildNewFakeTransportAddress(), Version.CURRENT))
            .build();
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("test")).nodes(nodes).build());
        return new WorkloadGroupRuleRoutingService(mock(Client.class), clusterService);
    }
}
