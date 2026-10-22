/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.attribute_extractor;

import org.opensearch.rule.RuleAttribute;
import org.opensearch.rule.attribute_extractor.AttributeExtractor;
import org.opensearch.rule.autotagging.Attribute;

import java.util.List;

import static org.opensearch.rule.attribute_extractor.AttributeExtractor.LogicalOperator.AND;

/**
 * This class extracts the transport action name of the API a request was sent to
 */
public class ActionExtractor implements AttributeExtractor<String> {
    private final String action;

    /**
     * Default constructor
     * @param action
     */
    public ActionExtractor(String action) {
        this.action = action;
    }

    @Override
    public Attribute getAttribute() {
        return RuleAttribute.ACTION;
    }

    @Override
    public Iterable<String> extract() {
        return List.of(action);
    }

    @Override
    public LogicalOperator getLogicalOperator() {
        return AND;
    }
}
