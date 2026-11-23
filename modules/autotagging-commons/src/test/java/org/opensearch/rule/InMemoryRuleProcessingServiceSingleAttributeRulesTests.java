/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.rule.attribute_extractor.AttributeExtractor;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.rule.storage.AttributeValueStoreFactory;
import org.opensearch.rule.storage.DefaultAttributeValueStore;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.opensearch.rule.attribute_extractor.AttributeExtractor.LogicalOperator.OR;

/**
 * A group with an index-only rule and a username-only rule must tag a request only when one of the two rules matches it.
 */
public class InMemoryRuleProcessingServiceSingleAttributeRulesTests extends OpenSearchTestCase {
    private static final Attribute INDEX = () -> "index_pattern";
    private static final Attribute USERNAME = () -> "username";

    private static final FeatureType FEATURE_TYPE = new FeatureType() {
        @Override
        public String getName() {
            return "index_and_username";
        }

        @Override
        public Map<Attribute, Integer> getOrderedAttributes() {
            return Map.of(INDEX, 1, USERNAME, 2);
        }
    };

    private InMemoryRuleProcessingService service;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        service = new InMemoryRuleProcessingService(
            new AttributeValueStoreFactory(FEATURE_TYPE, DefaultAttributeValueStore::new),
            FEATURE_TYPE.getOrderedAttributes()
        );
    }

    public void testIndexOnlyRuleAndUsernameOnlyRuleOfSameGroup() {
        service.add(rule("index_rule", "g", Map.of(INDEX, Set.of("logs-*"))));
        service.add(rule("user_rule", "g", Map.of(USERNAME, Set.of("alice"))));

        // Neither rule matches: the index rule needs logs-*, the user rule needs alice
        assertEquals(Optional.empty(), service.evaluateLabel(request("orders", "bob")));
        // One rule matches
        assertEquals(Optional.of("g"), service.evaluateLabel(request("logs-1", "bob")));
        assertEquals(Optional.of("g"), service.evaluateLabel(request("orders", "alice")));
        assertEquals(Optional.of("g"), service.evaluateLabel(request("logs-1", "alice")));
    }

    public void testIndexOnlyRuleAndUsernameOnlyRuleOfSameGroupWithoutUsername() {
        service.add(rule("index_rule", "g", Map.of(INDEX, Set.of("logs-*"))));
        service.add(rule("user_rule", "g", Map.of(USERNAME, Set.of("alice"))));

        // The request carries no username, as when the principal extractor returns no values
        assertEquals(Optional.empty(), service.evaluateLabel(requestWithoutUsername("orders")));
    }

    public void testIndexOnlyRuleAndUsernameOnlyRuleOfDifferentGroups() {
        // Control: with one rule per group the group-id intersection was already empty before the rule-id change
        service.add(rule("index_rule", "g1", Map.of(INDEX, Set.of("logs-*"))));
        service.add(rule("user_rule", "g2", Map.of(USERNAME, Set.of("alice"))));

        assertEquals(Optional.empty(), service.evaluateLabel(request("orders", "bob")));
        assertEquals(Optional.of("g1"), service.evaluateLabel(request("logs-1", "bob")));
        assertEquals(Optional.of("g2"), service.evaluateLabel(request("orders", "alice")));
    }

    public void testDeletingUsernameOnlyRuleKeepsIndexOnlyRuleTagging() {
        Rule userRule = rule("user_rule", "g", Map.of(USERNAME, Set.of("alice")));
        service.add(rule("index_rule", "g", Map.of(INDEX, Set.of("logs-*"))));
        service.add(userRule);
        service.remove(userRule);

        assertEquals(Optional.empty(), service.evaluateLabel(request("orders", "bob")));
        assertEquals(Optional.empty(), service.evaluateLabel(request("orders", "alice")));
        assertEquals(Optional.of("g"), service.evaluateLabel(request("logs-1", "bob")));
    }

    private static Rule rule(String id, String group, Map<Attribute, Set<String>> attributes) {
        return new Rule(id, "test description", attributes, FEATURE_TYPE, group, "2025-02-24T07:42:10.123456Z");
    }

    private static List<AttributeExtractor<String>> request(String index, String username) {
        List<AttributeExtractor<String>> extractors = new ArrayList<>();
        extractors.add(extractor(INDEX, index));
        extractors.add(extractor(USERNAME, username));
        return extractors;
    }

    private static List<AttributeExtractor<String>> requestWithoutUsername(String index) {
        List<AttributeExtractor<String>> extractors = new ArrayList<>();
        extractors.add(extractor(INDEX, index));
        extractors.add(new AttributeExtractor<>() {
            @Override
            public Attribute getAttribute() {
                return USERNAME;
            }

            @Override
            public Iterable<String> extract() {
                return List.of();
            }

            @Override
            public LogicalOperator getLogicalOperator() {
                return OR;
            }
        });
        return extractors;
    }

    private static AttributeExtractor<String> extractor(Attribute attribute, String value) {
        return new AttributeExtractor<>() {
            @Override
            public Attribute getAttribute() {
                return attribute;
            }

            @Override
            public Iterable<String> extract() {
                return List.of(value);
            }

            @Override
            public LogicalOperator getLogicalOperator() {
                return OR;
            }
        };
    }
}
