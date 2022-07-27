/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.manager.service.mq;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.GroupOperateType;
import org.apache.inlong.manager.common.enums.MQType;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.pojo.cluster.ClusterInfo;
import org.apache.inlong.manager.common.pojo.cluster.pulsar.PulsarClusterInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.group.pulsar.InlongPulsarInfo;
import org.apache.inlong.manager.common.pojo.pulsar.PulsarTopicBean;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.workflow.form.process.ProcessForm;
import org.apache.inlong.manager.common.pojo.workflow.form.process.StreamResourceProcessForm;
import org.apache.inlong.manager.service.cluster.InlongClusterService;
import org.apache.inlong.manager.service.mq.util.PulsarOperator;
import org.apache.inlong.manager.service.mq.util.PulsarUtils;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.QueueOperateListener;
import org.apache.inlong.manager.workflow.event.task.TaskEvent;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Create task listener for Pulsar Topic
 */
@Slf4j
@Component
public class CreatePulsarTopicTaskListener implements QueueOperateListener {

    @Autowired
    private InlongClusterService clusterService;
    @Autowired
    private PulsarOperator pulsarOperator;

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public boolean accept(WorkflowContext context) {
        ProcessForm processForm = context.getProcessForm();
        if (!(processForm instanceof StreamResourceProcessForm)) {
            return false;
        }
        StreamResourceProcessForm streamResourceProcessForm = (StreamResourceProcessForm) processForm;
        GroupOperateType operateType = streamResourceProcessForm.getGroupOperateType();
        if (operateType != GroupOperateType.INIT) {
            return false;
        }

        InlongGroupInfo groupInfo = streamResourceProcessForm.getGroupInfo();
        MQType mqType = MQType.forType(groupInfo.getMqType());
        String groupId = groupInfo.getInlongGroupId();
        String streamId = streamResourceProcessForm.getStreamInfo().getInlongStreamId();
        if (mqType == MQType.PULSAR || mqType == MQType.TDMQ_PULSAR) {
            InlongPulsarInfo pulsarInfo = (InlongPulsarInfo) groupInfo;
            boolean enable = InlongConstants.ENABLE_CREATE_RESOURCE.equals(pulsarInfo.getEnableCreateResource());
            if (enable) {
                log.info("need to create pulsar topic as the createResource was true for groupId [{}] streamId [{}]",
                        groupId, streamId);
                return true;
            } else {
                log.info("skip to create pulsar topic as the createResource was false for groupId [{}] streamId [{}]",
                        groupId, streamId);
                return false;
            }
        }
        return false;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {
        StreamResourceProcessForm form = (StreamResourceProcessForm) context.getProcessForm();
        InlongGroupInfo groupInfo = form.getGroupInfo();
        InlongStreamInfo streamInfo = form.getStreamInfo();
        final String groupId = streamInfo.getInlongGroupId();
        final String streamId = streamInfo.getInlongStreamId();
        log.info("begin to create pulsar topic for groupId={}, streamId={}", groupId, streamId);

        try {
            InlongPulsarInfo pulsarInfo = (InlongPulsarInfo) groupInfo;
            String pulsarTopic = streamInfo.getMqResource();
            String clusterTag = pulsarInfo.getInlongClusterTag();
            ClusterInfo clusterInfo = clusterService.getOne(clusterTag, null, ClusterType.PULSAR);
            PulsarClusterInfo pulsarCluster = (PulsarClusterInfo) clusterInfo;
            String tenant = pulsarCluster.getTenant();
            if (StringUtils.isEmpty(tenant)) {
                tenant = InlongConstants.DEFAULT_PULSAR_TENANT;
            }

            try (PulsarAdmin pulsarAdmin = PulsarUtils.getPulsarAdmin(pulsarCluster)) {
                PulsarTopicBean topicBean = PulsarTopicBean.builder()
                        .tenant(tenant)
                        .namespace(pulsarInfo.getMqResource())
                        .topicName(pulsarTopic)
                        .queueModule(pulsarInfo.getQueueModule())
                        .numPartitions(pulsarInfo.getPartitionNum())
                        .build();
                pulsarOperator.createTopic(pulsarAdmin, topicBean);
            }
        } catch (Exception e) {
            String msg = String.format("failed to create pulsar topic for groupId=%s, streamId=%s", groupId, streamId);
            log.error(msg, e);
            throw new WorkflowListenerException(msg);
        }

        log.info("success to create pulsar topic for groupId={}, streamId={}", groupId, streamId);
        return ListenerResult.success();
    }

}
