/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.action.support.ContextPreservingActionListener;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.Strings;
import org.opensearch.indices.SystemIndexRegistry;
import org.opensearch.plugin.wlm.rule.attribute_extractor.ActionExtractor;
import org.opensearch.plugin.wlm.rule.attribute_extractor.IndicesExtractor;
import org.opensearch.plugin.wlm.spi.AttributeExtractorExtension;
import org.opensearch.rule.InMemoryRuleProcessingService;
import org.opensearch.rule.RuleAttribute;
import org.opensearch.rule.attribute_extractor.AttributeExtractor;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.WlmMode;
import org.opensearch.wlm.WorkloadGroupTask;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.opensearch.plugin.wlm.WorkloadManagementPlugin.PRINCIPAL_ATTRIBUTE_NAME;

/**
 * This class is responsible to evaluate and assign the WORKLOAD_GROUP_ID header in ThreadContext
 */
public class AutoTaggingActionFilter implements ActionFilter {
    /**
     * Thread context transient holding the outermost action of a request
     */
    static final String ORIGINATING_ACTION_TRANSIENT = "_wlm_autotagging_originating_action";
    /**
     * Attribute value that only matches rules without a value for the attribute
     */
    private static final String UNCONSTRAINED_VALUE = "";
    private final InMemoryRuleProcessingService ruleProcessingService;
    private final ThreadPool threadPool;
    private final Map<Attribute, AttributeExtractorExtension> attributeExtensions;
    private final WlmClusterSettingValuesProvider wlmClusterSettingValuesProvider;
    private final FeatureType featureType;
    private final boolean supportsActionAttribute;

    /**
     * Main constructor
     * @param ruleProcessingService provides access to in memory view of rules
     * @param threadPool to access assign the label
     * @param attributeExtensions
     * @param wlmClusterSettingValuesProvider
     * @param featureType
     */
    public AutoTaggingActionFilter(
        InMemoryRuleProcessingService ruleProcessingService,
        ThreadPool threadPool,
        Map<Attribute, AttributeExtractorExtension> attributeExtensions,
        WlmClusterSettingValuesProvider wlmClusterSettingValuesProvider,
        FeatureType featureType
    ) {
        this.ruleProcessingService = ruleProcessingService;
        this.threadPool = threadPool;
        this.attributeExtensions = attributeExtensions;
        this.wlmClusterSettingValuesProvider = wlmClusterSettingValuesProvider;
        this.featureType = featureType;
        this.supportsActionAttribute = featureType.isValidAttribute(RuleAttribute.ACTION);
    }

    @Override
    public int order() {
        return Integer.MAX_VALUE;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionRequestMetadata<Request, Response> actionRequestMetadata,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        final boolean isSearchRequest = request instanceof SearchRequest;
        final boolean isSearchScrollRequest = request instanceof SearchScrollRequest;
        final boolean isValidRequest = isSearchRequest || isSearchScrollRequest;

        if (wlmClusterSettingValuesProvider.getWlmMode() == WlmMode.DISABLED) {
            chain.proceed(task, action, request, listener);
            return;
        }
        final ThreadContext threadContext = threadPool.getThreadContext();
        final boolean evaluateAction = supportsActionAttribute && ruleProcessingService.hasRulesConstraining(RuleAttribute.ACTION);
        if (!isValidRequest) {
            if (evaluateAction) {
                proceedRecordingOriginatingAction(threadContext, task, action, request, listener, chain);
            } else {
                chain.proceed(task, action, request, listener);
            }
            return;
        }
        final String[] requestIndices = isSearchRequest
            ? ((IndicesRequest) request).indices()
            : ((SearchScrollRequest) request).originalIndicesOrEmpty();
        final String[] indices = requestIndices == null ? Strings.EMPTY_ARRAY : requestIndices;
        List<AttributeExtractor<String>> attributeExtractors = new ArrayList<>();
        if (evaluateAction) {
            attributeExtractors.add(new ActionExtractor(getAction(threadContext, action, isSearchScrollRequest, indices)));
        }
        if (indices.length == 0 && evaluateAction) {
            // Unknown indices only match rules without an index_pattern
            attributeExtractors.add(indexPatternExtractor(List.of(UNCONSTRAINED_VALUE)));
        } else if (isSearchRequest) {
            attributeExtractors.add(new IndicesExtractor((IndicesRequest) request));
        } else if (indices.length > 0) {
            // Scroll: recover the original user-provided indices from ParsedScrollId
            attributeExtractors.add(indexPatternExtractor(Arrays.asList(indices)));
        }

        List<String> principalValues = null;
        if (featureType.getAllowedAttributesRegistry().containsKey(PRINCIPAL_ATTRIBUTE_NAME)) {
            Attribute attribute = featureType.getAllowedAttributesRegistry().get(PRINCIPAL_ATTRIBUTE_NAME);
            assert attributeExtensions.containsKey(attribute);
            final AttributeExtractor<String> extractor = attributeExtensions.get(attribute).getAttributeExtractor();
            // Materialize once: extract() has no re-iterability contract, and an empty second read would disable throttling.
            final List<String> values = new ArrayList<>();
            extractor.extract().forEach(values::add);
            principalValues = values;
            attributeExtractors.add(new AttributeExtractor<>() {
                @Override
                public Attribute getAttribute() {
                    return extractor.getAttribute();
                }

                @Override
                public Iterable<String> extract() {
                    return values;
                }

                @Override
                public LogicalOperator getLogicalOperator() {
                    return extractor.getLogicalOperator();
                }
            });
        }

        Optional<String> label = ruleProcessingService.evaluateLabel(attributeExtractors);
        if (threadContext.getRequestHeadersOnly().containsKey(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER) == false) {
            label.ifPresent(s -> threadContext.putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, s));
        }
        // Carried on the task, not the thread context; see WorkloadGroupTask#setThrottlePrincipal.
        if (principalValues != null && task instanceof WorkloadGroupTask) {
            String principal = String.join(WorkloadGroupTask.WORKLOAD_GROUP_PRINCIPAL_VALUE_DELIMITER, principalValues);
            if (principal.isEmpty() == false) {
                ((WorkloadGroupTask) task).setThrottlePrincipal(principal);
            }
        }
        chain.proceed(task, action, request, listener);
    }

    private static AttributeExtractor<String> indexPatternExtractor(Iterable<String> values) {
        return new AttributeExtractor<>() {
            @Override
            public Attribute getAttribute() {
                return RuleAttribute.INDEX_PATTERN;
            }

            @Override
            public Iterable<String> extract() {
                return values;
            }

            @Override
            public LogicalOperator getLogicalOperator() {
                return LogicalOperator.AND;
            }
        };
    }

    /**
     * Returns the action a search or scroll is matched on
     */
    private static String getAction(ThreadContext threadContext, String action, boolean isScroll, String[] indices) {
        if (threadContext.isSystemContext() || targetsOnlySystemIndices(indices)) {
            return UNCONSTRAINED_VALUE;
        }
        final String originatingAction = threadContext.getTransient(ORIGINATING_ACTION_TRANSIENT);
        if (originatingAction != null) {
            return originatingAction;
        }
        return isScroll ? SearchAction.NAME : action;
    }

    private static boolean targetsOnlySystemIndices(String[] indices) {
        return indices.length > 0 && Arrays.stream(indices).allMatch(SystemIndexRegistry::matchesSystemIndexPattern);
    }

    /**
     * Records the outermost action so the searches it fans out to are matched on it
     */
    private <Request extends ActionRequest, Response extends ActionResponse> void proceedRecordingOriginatingAction(
        ThreadContext threadContext,
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        if (action.indexOf('[') >= 0 || threadContext.getTransient(ORIGINATING_ACTION_TRANSIENT) != null) {
            chain.proceed(task, action, request, listener);
            return;
        }
        final ActionListener<Response> restoringListener = new ContextPreservingActionListener<>(
            threadContext.newRestorableContext(true),
            listener
        );
        try (ThreadContext.StoredContext ignored = threadContext.newStoredContext(true)) {
            threadContext.putTransient(ORIGINATING_ACTION_TRANSIENT, action);
            chain.proceed(task, action, request, restoringListener);
        }
    }
}
