/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.opensearch.action.search.MultiSearchAction;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchScrollAction;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.indices.SystemIndices;
import org.opensearch.plugin.wlm.rule.WorkloadGroupFeatureType;
import org.opensearch.plugin.wlm.spi.AttributeExtractorExtension;
import org.opensearch.rule.InMemoryRuleProcessingService;
import org.opensearch.rule.RuleAttribute;
import org.opensearch.rule.attribute_extractor.AttributeExtractor;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.rule.storage.AttributeValueStoreFactory;
import org.opensearch.rule.storage.DefaultAttributeValueStore;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.WlmMode;
import org.opensearch.wlm.WorkloadGroupTask;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.opensearch.plugin.wlm.WorkloadManagementPlugin.PRINCIPAL_ATTRIBUTE_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AutoTaggingActionFilterTests extends OpenSearchTestCase {

    AutoTaggingActionFilter autoTaggingActionFilter;
    InMemoryRuleProcessingService ruleProcessingService;
    ThreadPool threadPool;

    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("AutoTaggingActionFilterTests");
        AttributeValueStoreFactory attributeValueStoreFactory = new AttributeValueStoreFactory(
            WLMFeatureType.WLM,
            DefaultAttributeValueStore::new
        );
        ruleProcessingService = spy(new InMemoryRuleProcessingService(attributeValueStoreFactory, null));
        autoTaggingActionFilter = new AutoTaggingActionFilter(
            ruleProcessingService,
            threadPool,
            new HashMap<>(),
            mock(WlmClusterSettingValuesProvider.class),
            WLMFeatureType.WLM
        );
    }

    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testOrder() {
        assertEquals(Integer.MAX_VALUE, autoTaggingActionFilter.order());
    }

    public void testApplyForValidRequest() {
        SearchRequest request = mock(SearchRequest.class);
        ActionFilterChain<ActionRequest, ActionResponse> mockFilterChain = mock(TestActionFilterChain.class);
        when(request.indices()).thenReturn(new String[] { "foo" });
        try (ThreadContext.StoredContext context = threadPool.getThreadContext().stashContext()) {
            when(ruleProcessingService.evaluateLabel(anyList())).thenReturn(Optional.of("TestQG_ID"));
            autoTaggingActionFilter.apply(mock(Task.class), "Test", request, ActionRequestMetadata.empty(), null, mockFilterChain);

            assertEquals("TestQG_ID", threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER));
            verify(ruleProcessingService, times(1)).evaluateLabel(anyList());
        }
    }

    public void testApplyDoesNotOverwriteExistingHeader() {
        SearchRequest request = mock(SearchRequest.class);
        ActionFilterChain<ActionRequest, ActionResponse> mockFilterChain = mock(TestActionFilterChain.class);
        when(request.indices()).thenReturn(new String[] { "lookupidx" });
        try (ThreadContext.StoredContext context = threadPool.getThreadContext().stashContext()) {
            threadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, "OuterQG_ID");
            when(ruleProcessingService.evaluateLabel(anyList())).thenReturn(Optional.of("NestedQG_ID"));

            // Must not throw "value for key [workloadGroupId] already present"
            autoTaggingActionFilter.apply(mock(Task.class), "Test", request, ActionRequestMetadata.empty(), null, mockFilterChain);

            assertEquals("OuterQG_ID", threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER));
        }
    }

    public void testApplyOverridesNodeDefaultWorkloadGroup() {
        Settings settings = Settings.builder()
            .put("request.headers." + WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, "NodeDefaultQG_ID")
            .build();
        ThreadPool defaultHeaderPool = new TestThreadPool("AutoTaggingActionFilterDefaultHeaderTests", settings);
        try {
            AutoTaggingActionFilter filter = new AutoTaggingActionFilter(
                ruleProcessingService,
                defaultHeaderPool,
                new HashMap<>(),
                mock(WlmClusterSettingValuesProvider.class),
                WLMFeatureType.WLM
            );
            SearchRequest request = mock(SearchRequest.class);
            ActionFilterChain<ActionRequest, ActionResponse> mockFilterChain = mock(TestActionFilterChain.class);
            when(request.indices()).thenReturn(new String[] { "foo" });
            try (ThreadContext.StoredContext context = defaultHeaderPool.getThreadContext().stashContext()) {
                assertEquals(
                    "NodeDefaultQG_ID",
                    defaultHeaderPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER)
                );
                when(ruleProcessingService.evaluateLabel(anyList())).thenReturn(Optional.of("TestQG_ID"));

                filter.apply(mock(Task.class), "Test", request, ActionRequestMetadata.empty(), null, mockFilterChain);

                assertEquals("TestQG_ID", defaultHeaderPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER));
            }
        } finally {
            defaultHeaderPool.shutdownNow();
        }
    }

    public void testApplyForInValidRequest() {
        ActionFilterChain<ActionRequest, ActionResponse> mockFilterChain = mock(TestActionFilterChain.class);
        CancelTasksRequest request = new CancelTasksRequest();
        autoTaggingActionFilter.apply(mock(Task.class), "Test", request, ActionRequestMetadata.empty(), null, mockFilterChain);

        verify(ruleProcessingService, times(0)).evaluateLabel(anyList());
    }

    public void testApplySetsThrottlePrincipalOnTaskWhenExtractorPresent() {
        // A feature type that includes a "principal" attribute + a matching extractor extension in the map.
        Attribute principalAttr = PRINCIPAL_ATTRIBUTE;
        FeatureType featureTypeWithPrincipal = PRINCIPAL_FEATURE_TYPE;
        AttributeExtractor<String> principalExtractor = principalExtractor(() -> List.of("username|alice", "role|admin"));
        AttributeExtractorExtension extension = () -> principalExtractor;
        Map<Attribute, AttributeExtractorExtension> extensions = Map.of(principalAttr, extension);

        InMemoryRuleProcessingService svc = spy(
            new InMemoryRuleProcessingService(
                new AttributeValueStoreFactory(featureTypeWithPrincipal, DefaultAttributeValueStore::new),
                null
            )
        );
        AutoTaggingActionFilter filter = new AutoTaggingActionFilter(
            svc,
            threadPool,
            extensions,
            mock(WlmClusterSettingValuesProvider.class),
            featureTypeWithPrincipal
        );

        SearchRequest request = mock(SearchRequest.class);
        when(request.indices()).thenReturn(new String[] { "foo" });
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(TestActionFilterChain.class);
        WorkloadGroupTask task = newWorkloadGroupTask();
        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            when(svc.evaluateLabel(anyList())).thenReturn(Optional.of("QG"));
            filter.apply(task, "Test", request, ActionRequestMetadata.empty(), null, chain);

            // Both tokens are joined onto the task.
            assertEquals(
                "username|alice" + WorkloadGroupTask.WORKLOAD_GROUP_PRINCIPAL_VALUE_DELIMITER + "role|admin",
                task.getThrottlePrincipal()
            );
            // Not in the thread context, which is serialized to every shard and remote cluster.
            assertNull(threadPool.getThreadContext().getHeader("workloadGroupPrincipal"));
        }
    }

    public void testApplyTwiceOnOneThreadContextIsTolerated() {
        // The filter can run twice on one ThreadContext (an _msearch draining its queue from a response handler), and
        // putHeader throws on a repeat, so the principal must live on the task.
        Attribute principalAttr = PRINCIPAL_ATTRIBUTE;
        FeatureType featureTypeWithPrincipal = PRINCIPAL_FEATURE_TYPE;
        AtomicInteger extractCalls = new AtomicInteger();
        AttributeExtractor<String> principalExtractor = principalExtractor(() -> {
            extractCalls.incrementAndGet();
            return List.of("username|alice");
        });
        AttributeExtractorExtension extension = () -> principalExtractor;
        InMemoryRuleProcessingService svc = spy(
            new InMemoryRuleProcessingService(
                new AttributeValueStoreFactory(featureTypeWithPrincipal, DefaultAttributeValueStore::new),
                null
            )
        );
        AutoTaggingActionFilter filter = new AutoTaggingActionFilter(
            svc,
            threadPool,
            Map.of(principalAttr, extension),
            mock(WlmClusterSettingValuesProvider.class),
            featureTypeWithPrincipal
        );

        SearchRequest request = mock(SearchRequest.class);
        when(request.indices()).thenReturn(new String[] { "foo" });
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(TestActionFilterChain.class);
        WorkloadGroupTask first = newWorkloadGroupTask();
        WorkloadGroupTask second = newWorkloadGroupTask();
        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            // No label, so only the principal is under test.
            when(svc.evaluateLabel(anyList())).thenReturn(Optional.empty());
            filter.apply(first, "Test", request, ActionRequestMetadata.empty(), null, chain);
            filter.apply(second, "Test", request, ActionRequestMetadata.empty(), null, chain);

            // Each sub-request carries its own principal, and neither run threw on a duplicate key.
            assertEquals("username|alice", first.getThrottlePrincipal());
            assertEquals("username|alice", second.getThrottlePrincipal());
            assertNull(threadPool.getThreadContext().getHeader("workloadGroupPrincipal"));
            // extract() may be single-use, so it must be called once per request.
            assertEquals("extract() must be invoked once per request", 2, extractCalls.get());
        }
    }

    public void testApplyLeavesThrottlePrincipalUnsetWhenNoExtractor() {
        // No principal extractor, so the principal stays null and username/role throttling fails open.
        SearchRequest request = mock(SearchRequest.class);
        when(request.indices()).thenReturn(new String[] { "foo" });
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(TestActionFilterChain.class);
        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            when(ruleProcessingService.evaluateLabel(anyList())).thenReturn(Optional.of("QG"));
            WorkloadGroupTask task = newWorkloadGroupTask();
            autoTaggingActionFilter.apply(task, "Test", request, ActionRequestMetadata.empty(), null, chain);
            assertNull(task.getThrottlePrincipal());
        }
    }

    public void testApplyForScrollRequestWithOriginalIndices() {
        SearchScrollRequest request = mock(SearchScrollRequest.class);
        ActionFilterChain<ActionRequest, ActionResponse> chain = mock(TestActionFilterChain.class);

        @SuppressWarnings("unchecked")
        ActionRequestMetadata<ActionRequest, ActionResponse> metadata = mock(ActionRequestMetadata.class);
        when(request.originalIndicesOrEmpty()).thenReturn(new String[] { "logs-scroll-index" });

        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            doAnswer(inv -> {
                @SuppressWarnings("unchecked")
                List<AttributeExtractor<String>> extractors = inv.getArgument(0);

                assertNotNull(extractors);
                assertEquals(1, extractors.size());

                AttributeExtractor<String> ex = extractors.get(0);
                assertEquals(RuleAttribute.INDEX_PATTERN, ex.getAttribute());

                List<String> values = new ArrayList<>();
                ex.extract().forEach(values::add);
                assertEquals(List.of("logs-scroll-index"), values);

                return Optional.of("ScrollQG_ID");
            }).when(ruleProcessingService).evaluateLabel(any());

            autoTaggingActionFilter.apply(mock(Task.class), "Test", request, metadata, null, chain);

            assertEquals("ScrollQG_ID", threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER));
            verify(ruleProcessingService, times(1)).evaluateLabel(anyList());
        }
    }

    public void testApplyMatchesRuleOnRequestAction() {
        InMemoryRuleProcessingService svc = newActionRuleProcessingService();
        svc.add(newRule(Map.of(RuleAttribute.ACTION, Set.of(SearchAction.NAME)), "search_group"));
        AutoTaggingActionFilter filter = newActionFilter(svc);

        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            filter.apply(mock(Task.class), SearchAction.NAME, new SearchRequest("foo"), ActionRequestMetadata.empty(), null, noopChain());
            assertEquals("search_group", threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER));
        }

        // Later scroll pages are matched as the _search they continue
        SearchScrollRequest scrollRequest = mock(SearchScrollRequest.class);
        when(scrollRequest.originalIndicesOrEmpty()).thenReturn(new String[] { "foo" });
        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            filter.apply(mock(Task.class), SearchScrollAction.NAME, scrollRequest, ActionRequestMetadata.empty(), null, noopChain());
            assertEquals("search_group", threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER));
        }
    }

    public void testApplyMatchesFanOutSearchOnOriginatingAction() {
        InMemoryRuleProcessingService svc = newActionRuleProcessingService();
        svc.add(newRule(Map.of(RuleAttribute.ACTION, Set.of(MultiSearchAction.NAME)), "msearch_group"));
        svc.add(newRule(Map.of(RuleAttribute.ACTION, Set.of(SearchAction.NAME)), "search_group"));
        AutoTaggingActionFilter filter = newActionFilter(svc);

        List<String> childLabels = new ArrayList<>();
        ActionFilterChain<ActionRequest, ActionResponse> msearchChain = (task, action, request, listener) -> {
            // Sub-search of the msearch
            filter.apply(mock(Task.class), SearchAction.NAME, new SearchRequest("foo"), ActionRequestMetadata.empty(), null, noopChain());
            childLabels.add(threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER));
        };
        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            filter.apply(
                mock(Task.class),
                MultiSearchAction.NAME,
                new MultiSearchRequest(),
                ActionRequestMetadata.empty(),
                null,
                msearchChain
            );

            assertEquals(List.of("msearch_group"), childLabels);
            assertNull(threadPool.getThreadContext().getTransient(AutoTaggingActionFilter.ORIGINATING_ACTION_TRANSIENT));
        }
    }

    public void testApplyKeepsOutermostOriginatingAction() {
        String outerAction = "indices:data/read/msearch/template";
        InMemoryRuleProcessingService svc = newActionRuleProcessingService();
        svc.add(newRule(Map.of(RuleAttribute.ACTION, Set.of(outerAction)), "template_group"));
        svc.add(newRule(Map.of(RuleAttribute.ACTION, Set.of(MultiSearchAction.NAME)), "msearch_group"));
        AutoTaggingActionFilter filter = newActionFilter(svc);

        List<String> childLabels = new ArrayList<>();
        ActionFilterChain<ActionRequest, ActionResponse> searchChain = (task, action, request, listener) -> {
            filter.apply(mock(Task.class), SearchAction.NAME, new SearchRequest("foo"), ActionRequestMetadata.empty(), null, noopChain());
            childLabels.add(threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER));
        };
        ActionFilterChain<ActionRequest, ActionResponse> msearchChain = (task, action, request, listener) -> filter.apply(
            mock(Task.class),
            MultiSearchAction.NAME,
            new MultiSearchRequest(),
            ActionRequestMetadata.empty(),
            null,
            searchChain
        );
        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            filter.apply(mock(Task.class), outerAction, new MultiSearchRequest(), ActionRequestMetadata.empty(), null, msearchChain);
            assertEquals(List.of("template_group"), childLabels);
        }
    }

    public void testApplyMatchesWildcardActionCombinedWithIndex() {
        InMemoryRuleProcessingService svc = newActionRuleProcessingService();
        svc.add(
            newRule(
                Map.of(RuleAttribute.ACTION, Set.of("indices:data/write/*"), RuleAttribute.INDEX_PATTERN, Set.of("logs*")),
                "write_logs_group"
            )
        );
        AutoTaggingActionFilter filter = newActionFilter(svc);

        SearchScrollRequest scrollRequest = mock(SearchScrollRequest.class);
        when(scrollRequest.originalIndicesOrEmpty()).thenReturn(new String[] { "logs-1" });
        List<String> childLabels = new ArrayList<>();
        ActionFilterChain<ActionRequest, ActionResponse> byQueryChain = (task, action, request, listener) -> {
            filter.apply(mock(Task.class), SearchScrollAction.NAME, scrollRequest, ActionRequestMetadata.empty(), null, noopChain());
            childLabels.add(threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER));
        };
        ActionRequest byQueryRequest = mock(ActionRequest.class);
        String byQueryAction = "indices:data/write/delete/byquery";
        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            filter.apply(mock(Task.class), byQueryAction, byQueryRequest, ActionRequestMetadata.empty(), null, byQueryChain);
            assertEquals(List.of("write_logs_group"), childLabels);
        }

        // Same API but a different index does not match
        when(scrollRequest.originalIndicesOrEmpty()).thenReturn(new String[] { "metrics-1" });
        childLabels.clear();
        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            filter.apply(mock(Task.class), byQueryAction, byQueryRequest, ActionRequestMetadata.empty(), null, byQueryChain);
            assertEquals(1, childLabels.size());
            assertNull(childLabels.get(0));
        }
    }

    public void testApplyIndexRuleTakesPriorityOverActionOnlyRule() {
        InMemoryRuleProcessingService svc = newActionRuleProcessingService();
        svc.add(newRule(Map.of(RuleAttribute.INDEX_PATTERN, Set.of("foo")), "index_group"));
        svc.add(newRule(Map.of(RuleAttribute.ACTION, Set.of(SearchAction.NAME)), "search_group"));
        AutoTaggingActionFilter filter = newActionFilter(svc);

        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            filter.apply(mock(Task.class), SearchAction.NAME, new SearchRequest("foo"), ActionRequestMetadata.empty(), null, noopChain());
            assertEquals("index_group", threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER));
        }
        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            filter.apply(mock(Task.class), SearchAction.NAME, new SearchRequest("bar"), ActionRequestMetadata.empty(), null, noopChain());
            assertEquals("search_group", threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER));
        }
    }

    public void testApplyActionRulesIgnoreSystemSearches() {
        // Registers the built-in .tasks system index
        new SystemIndices(Map.of());
        InMemoryRuleProcessingService svc = newActionRuleProcessingService();
        svc.add(newRule(Map.of(RuleAttribute.ACTION, Set.of("indices:data/read/*")), "read_group"));
        svc.add(newRule(Map.of(RuleAttribute.INDEX_PATTERN, Set.of(".tasks")), "tasks_group"));
        AutoTaggingActionFilter filter = newActionFilter(svc);

        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            filter.apply(
                mock(Task.class),
                SearchAction.NAME,
                new SearchRequest(".tasks"),
                ActionRequestMetadata.empty(),
                null,
                noopChain()
            );
            assertEquals("tasks_group", threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER));
        }
        svc.add(newRule(Map.of(RuleAttribute.ACTION, Set.of(SearchAction.NAME)), "search_group"));
        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            threadPool.getThreadContext().markAsSystemContext();
            filter.apply(mock(Task.class), SearchAction.NAME, new SearchRequest("foo"), ActionRequestMetadata.empty(), null, noopChain());
            assertNull(threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER));
        }
        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            filter.apply(mock(Task.class), SearchAction.NAME, new SearchRequest("foo"), ActionRequestMetadata.empty(), null, noopChain());
            assertEquals("search_group", threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER));
        }
    }

    public void testApplyWithoutIndicesOnlyMatchesRulesWithoutIndexPattern() {
        InMemoryRuleProcessingService svc = newActionRuleProcessingService();
        svc.add(newRule(Map.of(RuleAttribute.INDEX_PATTERN, Set.of("logs*")), "index_group"));
        AutoTaggingActionFilter filter = newActionFilter(svc);
        SearchScrollRequest scrollRequest = mock(SearchScrollRequest.class);
        when(scrollRequest.originalIndicesOrEmpty()).thenReturn(new String[0]);

        svc.add(newRule(Map.of(RuleAttribute.ACTION, Set.of(MultiSearchAction.NAME)), "msearch_group"));
        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            filter.apply(mock(Task.class), SearchScrollAction.NAME, scrollRequest, ActionRequestMetadata.empty(), null, noopChain());
            assertNull(threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER));
        }

        svc.add(newRule(Map.of(RuleAttribute.ACTION, Set.of(SearchAction.NAME)), "search_group"));
        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            filter.apply(mock(Task.class), SearchScrollAction.NAME, scrollRequest, ActionRequestMetadata.empty(), null, noopChain());
            assertEquals("search_group", threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER));
        }
        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            filter.apply(mock(Task.class), SearchAction.NAME, new SearchRequest(), ActionRequestMetadata.empty(), null, noopChain());
            assertEquals("search_group", threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER));
        }
    }

    public void testApplyRecordsOriginatingActionOnlyWhenNeeded() {
        InMemoryRuleProcessingService svc = newActionRuleProcessingService();
        WlmClusterSettingValuesProvider settingValuesProvider = mock(WlmClusterSettingValuesProvider.class);
        AutoTaggingActionFilter filter = new AutoTaggingActionFilter(
            svc,
            threadPool,
            new HashMap<>(),
            settingValuesProvider,
            WORKLOAD_GROUP_FEATURE_TYPE
        );
        List<Object> recorded = new ArrayList<>();
        ActionFilterChain<ActionRequest, ActionResponse> recordingChain = (task, action, request, listener) -> recorded.add(
            threadPool.getThreadContext().getTransient(AutoTaggingActionFilter.ORIGINATING_ACTION_TRANSIENT)
        );
        Runnable applyMsearch = () -> {
            try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
                filter.apply(
                    mock(Task.class),
                    MultiSearchAction.NAME,
                    new MultiSearchRequest(),
                    ActionRequestMetadata.empty(),
                    null,
                    recordingChain
                );
            }
        };

        // No rule sets an action
        applyMsearch.run();
        svc.add(newRule(Map.of(RuleAttribute.ACTION, Set.of(MultiSearchAction.NAME)), "msearch_group"));
        // WLM disabled
        when(settingValuesProvider.getWlmMode()).thenReturn(WlmMode.DISABLED);
        applyMsearch.run();
        // Shard level sub-action
        when(settingValuesProvider.getWlmMode()).thenReturn(WlmMode.ENABLED);
        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            filter.apply(
                mock(Task.class),
                "indices:data/write/bulk[s]",
                mock(ActionRequest.class),
                ActionRequestMetadata.empty(),
                null,
                recordingChain
            );
        }
        assertEquals(Arrays.asList(null, null, null), recorded);

        applyMsearch.run();
        assertEquals(MultiSearchAction.NAME, recorded.get(3));
    }

    public void testApplyScrollWithoutIndicesMatchesPrincipalRuleWhenNoActionRules() {
        Attribute principalAttr = PRINCIPAL_ATTRIBUTE;
        WorkloadGroupFeatureType featureType = new WorkloadGroupFeatureType(featureValue -> {}, new HashMap<>(Map.of(principalAttr, 1)));
        InMemoryRuleProcessingService svc = new InMemoryRuleProcessingService(
            new AttributeValueStoreFactory(featureType, DefaultAttributeValueStore::new),
            featureType.getOrderedAttributes()
        );
        AttributeExtractorExtension extension = () -> principalExtractor(() -> List.of("alice"));
        AutoTaggingActionFilter filter = new AutoTaggingActionFilter(
            svc,
            threadPool,
            Map.of(principalAttr, extension),
            mock(WlmClusterSettingValuesProvider.class),
            featureType
        );
        svc.add(
            new Rule(
                "principal_rule",
                "test description",
                Map.of(principalAttr, Set.of("alice"), RuleAttribute.INDEX_PATTERN, Set.of("logs*")),
                featureType,
                "principal_group",
                "2025-02-24T07:42:10.123456Z"
            )
        );
        SearchScrollRequest scrollRequest = mock(SearchScrollRequest.class);
        when(scrollRequest.originalIndicesOrEmpty()).thenReturn(new String[0]);

        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            filter.apply(mock(Task.class), SearchScrollAction.NAME, scrollRequest, ActionRequestMetadata.empty(), null, noopChain());
            assertEquals("principal_group", threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER));
        }
    }

    public void testApplyRestoresCallerContextForListener() {
        InMemoryRuleProcessingService svc = newActionRuleProcessingService();
        svc.add(newRule(Map.of(RuleAttribute.ACTION, Set.of(MultiSearchAction.NAME)), "msearch_group"));
        AutoTaggingActionFilter filter = newActionFilter(svc);
        ThreadContext threadContext = threadPool.getThreadContext();

        AtomicReference<ActionListener<ActionResponse>> capturedListener = new AtomicReference<>();
        AtomicReference<Supplier<ThreadContext.StoredContext>> childContext = new AtomicReference<>();
        ActionFilterChain<ActionRequest, ActionResponse> asyncChain = (task, action, request, listener) -> {
            capturedListener.set(listener);
            childContext.set(threadContext.newRestorableContext(false));
        };
        List<Object> seenByListener = new ArrayList<>();
        ActionListener<ActionResponse> listener = ActionListener.wrap(
            response -> seenByListener.add(threadContext.getTransient(AutoTaggingActionFilter.ORIGINATING_ACTION_TRANSIENT)),
            e -> fail(e.getMessage())
        );
        try (ThreadContext.StoredContext ctx = threadContext.stashContext()) {
            filter.apply(
                mock(Task.class),
                MultiSearchAction.NAME,
                new MultiSearchRequest(),
                ActionRequestMetadata.empty(),
                listener,
                asyncChain
            );
            // Respond from a context forked inside the msearch
            try (ThreadContext.StoredContext forked = childContext.get().get()) {
                assertEquals(MultiSearchAction.NAME, threadContext.getTransient(AutoTaggingActionFilter.ORIGINATING_ACTION_TRANSIENT));
                capturedListener.get().onResponse(null);
            }
        }
        assertEquals(1, seenByListener.size());
        assertNull(seenByListener.get(0));
    }

    private static final WorkloadGroupFeatureType WORKLOAD_GROUP_FEATURE_TYPE = new WorkloadGroupFeatureType(
        featureValue -> {},
        new HashMap<>()
    );

    private static InMemoryRuleProcessingService newActionRuleProcessingService() {
        return new InMemoryRuleProcessingService(
            new AttributeValueStoreFactory(WORKLOAD_GROUP_FEATURE_TYPE, DefaultAttributeValueStore::new),
            WORKLOAD_GROUP_FEATURE_TYPE.getOrderedAttributes()
        );
    }

    private AutoTaggingActionFilter newActionFilter(InMemoryRuleProcessingService svc) {
        return new AutoTaggingActionFilter(
            svc,
            threadPool,
            new HashMap<>(),
            mock(WlmClusterSettingValuesProvider.class),
            WORKLOAD_GROUP_FEATURE_TYPE
        );
    }

    private static Rule newRule(Map<Attribute, Set<String>> attributeMap, String label) {
        return new Rule(
            randomAlphaOfLength(5),
            "test description",
            attributeMap,
            WORKLOAD_GROUP_FEATURE_TYPE,
            label,
            "2025-02-24T07:42:10.123456Z"
        );
    }

    private static ActionFilterChain<ActionRequest, ActionResponse> noopChain() {
        return (task, action, request, listener) -> {};
    }

    public enum WLMFeatureType implements FeatureType {
        WLM;

        @Override
        public String getName() {
            return "";
        }

        @Override
        public Map<Attribute, Integer> getOrderedAttributes() {
            return Map.of(TestAttribute.TEST_ATTRIBUTE, 1);
        }
    }

    public enum TestAttribute implements Attribute {
        TEST_ATTRIBUTE("test_attribute"),
        INVALID_ATTRIBUTE("invalid_attribute");

        private final String name;

        TestAttribute(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public void validateAttribute() {}

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }

    private static final Attribute PRINCIPAL_ATTRIBUTE = new Attribute() {
        @Override
        public String getName() {
            return PRINCIPAL_ATTRIBUTE_NAME;
        }

        @Override
        public void validateAttribute() {}

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    };

    private static final FeatureType PRINCIPAL_FEATURE_TYPE = new FeatureType() {
        @Override
        public String getName() {
            return "wlm";
        }

        @Override
        public Map<Attribute, Integer> getOrderedAttributes() {
            return Map.of(PRINCIPAL_ATTRIBUTE, 1);
        }
    };

    private static AttributeExtractor<String> principalExtractor(Supplier<List<String>> values) {
        return new AttributeExtractor<>() {
            @Override
            public Attribute getAttribute() {
                return PRINCIPAL_ATTRIBUTE;
            }

            @Override
            public Iterable<String> extract() {
                return values.get();
            }

            @Override
            public LogicalOperator getLogicalOperator() {
                return LogicalOperator.OR;
            }
        };
    }

    private static WorkloadGroupTask newWorkloadGroupTask() {
        return new WorkloadGroupTask(1L, "transport", "Test", "test task", TaskId.EMPTY_TASK_ID, Map.of());
    }

    private static class TestActionFilterChain implements ActionFilterChain<ActionRequest, ActionResponse> {
        @Override
        public void proceed(Task task, String action, ActionRequest request, ActionListener<ActionResponse> listener) {

        }
    }
}
