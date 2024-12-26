/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.controller

import kafka.server.KafkaConfig
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition

import scala.collection.Set
import scala.collection.mutable

trait DeletionClient {
  def deleteTopic(topic: String, epochZkVersion: Int): Unit
  def deleteTopicDeletions(topics: Seq[String], epochZkVersion: Int): Unit
  def mutePartitionModifications(topic: String): Unit
  def sendMetadataUpdate(partitions: Set[TopicPartition]): Unit
}

class ControllerDeletionClient(controller: KafkaController, zkClient: KafkaZkClient) extends DeletionClient {
  override def deleteTopic(topic: String, epochZkVersion: Int): Unit = {

    // 删除 /brokers/topics/[topic_name]
    zkClient.deleteTopicZNode(topic, epochZkVersion)

    // 删除 /config/topics/[topic_name]
    zkClient.deleteTopicConfigs(Seq(topic), epochZkVersion)

    // 删除 /admin/delete_topics/[topic_name]
    zkClient.deleteTopicDeletions(Seq(topic), epochZkVersion)
  }

  override def deleteTopicDeletions(topics: Seq[String], epochZkVersion: Int): Unit = {
    zkClient.deleteTopicDeletions(topics, epochZkVersion)
  }

  // 不再监听 /brokers/topics/[topic_name] 的数据变更
  override def mutePartitionModifications(topic: String): Unit = {
    controller.unregisterPartitionModificationsHandlers(Seq(topic))
  }

  // 向集群中其他 Broker 发送指定分区的元数据更新请求
  override def sendMetadataUpdate(partitions: Set[TopicPartition]): Unit = {
    controller.sendUpdateMetadataRequest(controller.controllerContext.liveOrShuttingDownBrokerIds.toSeq, partitions)
  }
}

/**
 * This manages the state machine for topic deletion.
 *
 * 这个类管理着 topic 删除的状态机
 *
 * 1. TopicCommand issues topic deletion by creating a new admin path /admin/delete_topics/<topic>
 * 2. The controller listens for child changes on /admin/delete_topic and starts topic deletion for the respective topics
 * 3. The controller's ControllerEventThread handles topic deletion. A topic will be ineligible
 *    for deletion in the following scenarios -
  *   3.1 broker hosting one of the replicas for that topic goes down
  *   3.2 partition reassignment for partitions of that topic is in progress
 * 4. Topic deletion is resumed when -
 *    4.1 broker hosting one of the replicas for that topic is started
 *    4.2 partition reassignment for partitions of that topic completes
 * 5. Every replica for a topic being deleted is in either of the 3 states -
 *    5.1 TopicDeletionStarted Replica enters TopicDeletionStarted phase when onPartitionDeletion is invoked.
 *        This happens when the child change watch for /admin/delete_topics fires on the controller. As part of this state
 *        change, the controller sends StopReplicaRequests to all replicas. It registers a callback for the
 *        StopReplicaResponse when deletePartition=true thereby invoking a callback when a response for delete replica
 *        is received from every replica)
 *    5.2 TopicDeletionSuccessful moves replicas from
 *        TopicDeletionStarted->TopicDeletionSuccessful depending on the error codes in StopReplicaResponse
 *    5.3 TopicDeletionFailed moves replicas from
 *        TopicDeletionStarted->TopicDeletionFailed depending on the error codes in StopReplicaResponse.
 *        In general, if a broker dies and if it hosted replicas for topics being deleted, the controller marks the
 *        respective replicas in TopicDeletionFailed state in the onBrokerFailure callback. The reason is that if a
 *        broker fails before the request is sent and after the replica is in TopicDeletionStarted state,
 *        it is possible that the replica will mistakenly remain in TopicDeletionStarted state and topic deletion
 *        will not be retried when the broker comes back up.
 * 6. A topic is marked successfully deleted only if all replicas are in TopicDeletionSuccessful
 *    state. Topic deletion teardown mode deletes all topic state from the controllerContext
 *    as well as from zookeeper. This is the only time the /brokers/topics/<topic> path gets deleted. On the other hand,
 *    if no replica is in TopicDeletionStarted state and at least one replica is in TopicDeletionFailed state, then
 *    it marks the topic for deletion retry.
 *
 * 1. TopicCommand 通过在 zk 上创建 /admin/delete_topics/[topic_name] 来发起主题删除任务
 * 2. Controller 监听着 /admin/delete_topic 下子节点的变化，当发生变动时，就会触发删除操作
 * 3. Controller 的 ControllerEventThread 会去真正执行删除逻辑；
 *    注意当 topic 处于下列情况时，不会被删除：
 *    3.1 持有该 topic 的某个 replica 的 broker 宕机了
 *    3.2 该主题的 partition 正在进行分区重新分配 (partition reassignment)
 * 4. 上述不删除的情况可以恢复执行，当
 *    4.1 持有该 topic 的某个 replica 的 broker 恢复上线
 *    4.2 分区重新分配执行完毕
 * 5. 每个待删除的 topic 都会处于以下三种状态之一:
 *    5.1 TopicDeletionStarted - 当调用 onPartitionDeletion 方法时，replica 就处于该状态；
 *        这通常是在 /admin/delete_topics 子节点发生变化时发生的;
 *        在这个过程中:
 *        - controller 会发送 StopReplicaRequest 给所有副本
 *        - 如果请求中 deletePartition=true，控制器会注册回调函数，
 *          等待每个副本的 StopReplicaResponse 返回时触发
 *    5.2 TopicDeletionSuccessful - 根据 StopReplicaResponse 中的状态码，将副本的状态调整至此
 *    5.3 TopicDeletionFailed - 根据 StopReplicaResponse 中的状态码，将副本的状态调整至此
 *        通常来说，如果 broker 宕机，并且它上面管理着待删除的 topic，那么 controller
 *        会在 onBrokerFailure callback 中将副本标记为 TopicDeletionFailed 状态。
 *        原因是，如果 broker 恢复上线后，未标记的副本可能会错误地保持在 TopicDeletionStarted 状态，
 *        导致主题删除流程无法重试
 * 6. 只有所有的 replica 都处于 TopicDeletionSuccessful 状态，主题删除才会标记为成功。
 *    在此阶段：
 *    Kafka 会从 controllerContext 和 ZooKeeper 中清除所有与该主题相关的状态信息。
 *    只有在此时， /brokers/topics/[topic] 这个路径才会删除。
 *    如果：
 *      - 没有副本处于 TopicDeletionStarted 状态；
 *      - 至少有一个副本处于 TopicDeletionFailed 状态；
 *    则主题会被标记为重试删除。
 *
 * @param controller
 */
class TopicDeletionManager(config: KafkaConfig,
                           controllerContext: ControllerContext,
                           replicaStateMachine: ReplicaStateMachine,
                           partitionStateMachine: PartitionStateMachine,
                           client: DeletionClient) extends Logging {
  this.logIdent = s"[Topic Deletion Manager ${config.brokerId}] "
  val isDeleteTopicEnabled: Boolean = config.deleteTopicEnable

  def init(initialTopicsToBeDeleted: Set[String], initialTopicsIneligibleForDeletion: Set[String]): Unit = {
    info(s"Initializing manager with initial deletions: $initialTopicsToBeDeleted, " +
      s"initial ineligible deletions: $initialTopicsIneligibleForDeletion")

    if (isDeleteTopicEnabled) {

      // append 到 topicsToBeDeleted 列表中
      controllerContext.queueTopicDeletion(initialTopicsToBeDeleted)

      // 确保 append 到 topicsIneligibleForDeletion 中的 topic 是
      // 应该删除 但 不能立刻删除 的
      controllerContext.topicsIneligibleForDeletion ++= initialTopicsIneligibleForDeletion & controllerContext.topicsToBeDeleted

    } else {
      // if delete topic is disabled clean the topic entries under /admin/delete_topics
      // 如果没有开启删除 topic 功能，则删除 /admin/delete_topics 下的内容

      info(s"Removing $initialTopicsToBeDeleted since delete topic is disabled")
      client.deleteTopicDeletions(initialTopicsToBeDeleted.toSeq, controllerContext.epochZkVersion)
    }
  }

  // 选举完成之后，尝试触发删除操作
  def tryTopicDeletion(): Unit = {
    if (isDeleteTopicEnabled) {
      resumeDeletions()
    }
  }

  /**
   * Invoked by the child change listener on /admin/delete_topics to queue up the topics for deletion. The topic gets added
   * to the topicsToBeDeleted list and only gets removed from the list when the topic deletion has completed successfully
   * i.e. all replicas of all partitions of that topic are deleted successfully.
   *
   * 当监听器发现 /admin/delete_topics 内容有变化时，会调用此方法。
   * topic 会被加入到 topicsToBeDeleted 列表中，并且只有在完全删除之后，才会被移除。
   *
   * @param topics Topics that should be deleted
   */
  def enqueueTopicsForDeletion(topics: Set[String]): Unit = {
    if (isDeleteTopicEnabled) {
      controllerContext.queueTopicDeletion(topics)
      resumeDeletions()
    }
  }

  /**
   * Invoked when any event that can possibly resume topic deletion occurs. These events include -
   * 1. New broker starts up. Any replicas belonging to topics queued up for deletion can be deleted since the broker is up
   * 2. Partition reassignment completes. Any partitions belonging to topics queued up for deletion finished reassignment
   *
   * 当任何事件导致删除操作可以恢复时，调用此方法。这些事件包括：
   * 1. 宕机的 broker 恢复了。任何属于待删除主题的副本都可以重新执行删除任务
   * 2. 分区重新分配完成。任何属于待删除主题的分区完成重新分配(Ineligible -> eligible)
   *
   * @param topics Topics for which deletion can be resumed
   */
  def resumeDeletionForTopics(topics: Set[String] = Set.empty): Unit = {
    if (isDeleteTopicEnabled) {
      val topicsToResumeDeletion = topics & controllerContext.topicsToBeDeleted
      if (topicsToResumeDeletion.nonEmpty) {
        controllerContext.topicsIneligibleForDeletion --= topicsToResumeDeletion
        resumeDeletions()
      }
    }
  }

  /**
   * Invoked when a broker that hosts replicas for topics to be deleted goes down. Also invoked when the callback for
   * StopReplicaResponse receives an error code for the replicas of a topic to be deleted. As part of this, the replicas
   * are moved from ReplicaDeletionStarted to ReplicaDeletionIneligible state. Also, the topic is added to the list of topics
   * ineligible for deletion until further notice.
   * @param replicas Replicas for which deletion has failed
   */
  def failReplicaDeletion(replicas: Set[PartitionAndReplica]): Unit = {
    if (isDeleteTopicEnabled) {
      val replicasThatFailedToDelete = replicas.filter(r => isTopicQueuedUpForDeletion(r.topic))
      if (replicasThatFailedToDelete.nonEmpty) {
        val topics = replicasThatFailedToDelete.map(_.topic)
        debug(s"Deletion failed for replicas ${replicasThatFailedToDelete.mkString(",")}. Halting deletion for topics $topics")
        replicaStateMachine.handleStateChanges(replicasThatFailedToDelete.toSeq, ReplicaDeletionIneligible)
        markTopicIneligibleForDeletion(topics, reason = "replica deletion failure")
        resumeDeletions()
      }
    }
  }

  /**
   * Halt delete topic if -
   * 1. replicas being down
   * 2. partition reassignment in progress for some partitions of the topic
   * @param topics Topics that should be marked ineligible for deletion. No op if the topic is was not previously queued up for deletion
   */
  def markTopicIneligibleForDeletion(topics: Set[String], reason: => String): Unit = {
    if (isDeleteTopicEnabled) {
      val newTopicsToHaltDeletion = controllerContext.topicsToBeDeleted & topics
      controllerContext.topicsIneligibleForDeletion ++= newTopicsToHaltDeletion
      if (newTopicsToHaltDeletion.nonEmpty)
        info(s"Halted deletion of topics ${newTopicsToHaltDeletion.mkString(",")} due to $reason")
    }
  }

  private def isTopicIneligibleForDeletion(topic: String): Boolean = {
    if (isDeleteTopicEnabled) {
      controllerContext.topicsIneligibleForDeletion.contains(topic)
    } else
      true
  }

  private def isTopicDeletionInProgress(topic: String): Boolean = {
    if (isDeleteTopicEnabled) {
      controllerContext.isAnyReplicaInState(topic, ReplicaDeletionStarted)
    } else
      false
  }

  def isTopicQueuedUpForDeletion(topic: String): Boolean = {
    if (isDeleteTopicEnabled) {
      controllerContext.isTopicQueuedUpForDeletion(topic)
    } else
      false
  }

  /**
   * Invoked by the StopReplicaResponse callback when it receives no error code for a replica of a topic to be deleted.
   * As part of this, the replicas are moved from ReplicaDeletionStarted to ReplicaDeletionSuccessful state. Tears down
   * the topic if all replicas of a topic have been successfully deleted
   * @param replicas Replicas that were successfully deleted by the broker
   */
  def completeReplicaDeletion(replicas: Set[PartitionAndReplica]): Unit = {
    val successfullyDeletedReplicas = replicas.filter(r => isTopicQueuedUpForDeletion(r.topic))
    debug(s"Deletion successfully completed for replicas ${successfullyDeletedReplicas.mkString(",")}")
    replicaStateMachine.handleStateChanges(successfullyDeletedReplicas.toSeq, ReplicaDeletionSuccessful)
    resumeDeletions()
  }

  /**
   * Topic deletion can be retried if -
   * 1. Topic deletion is not already complete
   * 2. Topic deletion is currently not in progress for that topic
   * 3. Topic is currently marked ineligible for deletion
   * @param topic Topic
   * @return Whether or not deletion can be retried for the topic
   */
  private def isTopicEligibleForDeletion(topic: String): Boolean = {
    controllerContext.isTopicQueuedUpForDeletion(topic) &&
      !isTopicDeletionInProgress(topic) &&
      !isTopicIneligibleForDeletion(topic)
  }

  /**
   * If the topic is queued for deletion but deletion is not currently under progress, then deletion is retried for that topic
   * To ensure a successful retry, reset states for respective replicas from ReplicaDeletionIneligible to OfflineReplica state
   * @param topics Topics for which deletion should be retried
   */
  private def retryDeletionForIneligibleReplicas(topics: Set[String]): Unit = {
    // reset replica states from ReplicaDeletionIneligible to OfflineReplica
    val failedReplicas = topics.flatMap(controllerContext.replicasInState(_, ReplicaDeletionIneligible))
    debug(s"Retrying deletion of topics ${topics.mkString(",")} since replicas ${failedReplicas.mkString(",")} were not successfully deleted")
    replicaStateMachine.handleStateChanges(failedReplicas.toSeq, OfflineReplica)
  }

  private def completeDeleteTopic(topic: String): Unit = {

    // deregister partition change listener on the deleted topic. This is to prevent the partition change listener
    // firing before the new topic listener when a deleted topic gets auto created
    //
    // 不再监听 /brokers/topics/[topic_name] 的数据变更。防止删除完成之前，用户客户端自动创建 topic。
    // (zkClient.unregisterZNodeChangeHandler(handler.path)))
    client.mutePartitionModifications(topic)

    val replicasForDeletedTopic = controllerContext.replicasInState(topic, ReplicaDeletionSuccessful)

    // controller will remove this replica from the state machine as well as its partition assignment cache

    // 更新 replica 状态机，将 topic 设置为 NonExistentReplica 状态
    replicaStateMachine.handleStateChanges(replicasForDeletedTopic.toSeq, NonExistentReplica)

    // 删除 zk 上与这个 topic 相关的所有节点
    client.deleteTopic(topic, controllerContext.epochZkVersion)

    // 清除 ctx 中与这个 topic 相关的所有内容
    controllerContext.removeTopic(topic)
  }

  /**
   * Invoked with the list of topics to be deleted
   * It invokes onPartitionDeletion for all partitions of a topic.
   * The updateMetadataRequest is also going to set the leader for the topics being deleted to
   * {@link LeaderAndIsr#LeaderDuringDelete}. This lets each broker know that this topic is being deleted and can be
   * removed from their caches.
   *
   * 删除传入的 topic 列表
   *
   * 这个方法会对 topic 的每个 partition 调用 onPartitionDeletion。
   * updateMetadataRequest 会将 leader 从 LeaderAndIsr.LeaderDuringDelete 中删除。
   * 这会告知其他 broker 这个 topic 将要被删除了，因此其他 broker 可以从缓存中删除这个 topic 了
   */
  private def onTopicDeletion(topics: Set[String]): Unit = {
    val unseenTopicsForDeletion = topics.diff(controllerContext.topicsWithDeletionStarted)

    // 首次执行删除，将 topic 添加到 topicsWithDeletionStarted 列表中
    // 表示开始删除
    if (unseenTopicsForDeletion.nonEmpty) {
      val unseenPartitionsForDeletion = unseenTopicsForDeletion.flatMap(controllerContext.partitionsForTopic)

      // 由于状态机严格限制分区状态之间的流转关系，
      // 所以这里先将分区标记为 offline 状态，再将分区标记为 NonExistent 状态
      partitionStateMachine.handleStateChanges(unseenPartitionsForDeletion.toSeq, OfflinePartition)
      partitionStateMachine.handleStateChanges(unseenPartitionsForDeletion.toSeq, NonExistentPartition)

      // adding of unseenTopicsForDeletion to topics with deletion started must be done after the partition
      // state changes to make sure the offlinePartitionCount metric is properly updated
      //
      // 一定要先更新 partition 的状态
      // 再去把还没开始删除的 topic 列表加入到 topicsWithDeletionStarted 中；
      // 否则会影响到 offlinePartitionCount 这个 metric 的值
      //
      // 将 topic 加入到 topicsWithDeletionStarted 列表中
      controllerContext.beginTopicDeletion(unseenTopicsForDeletion)
    }

    // send update metadata so that brokers stop serving data for topics to be deleted
    // 发送更新元数据的请求，brokers 将停止处理这些 topic 的数据
    client.sendMetadataUpdate(topics.flatMap(controllerContext.partitionsForTopic))

    // 删除 partition
    onPartitionDeletion(topics)
  }

  /**
   * Invoked by onTopicDeletion with the list of partitions for topics to be deleted
   * It does the following -
   * 1. Move all dead replicas directly to ReplicaDeletionIneligible state. Also mark the respective topics ineligible
   *    for deletion if some replicas are dead since it won't complete successfully anyway
   * 2. Move all replicas for the partitions to OfflineReplica state. This will send StopReplicaRequest to the replicas
   *    and LeaderAndIsrRequest to the leader with the shrunk ISR. When the leader replica itself is moved to OfflineReplica state,
   *    it will skip sending the LeaderAndIsrRequest since the leader will be updated to -1
   * 3. Move all replicas to ReplicaDeletionStarted state. This will send StopReplicaRequest with deletePartition=true. And
   *    will delete all persistent data from all replicas of the respective partitions
   *
   * 1. 把宕机的 replica 设置为 ReplicaDeletionIneligible 状态。
   *    其他相关的 topics 也都设置为该状态。因为 broker 挂了，这个请求无法完成。
   * 2. 将分区的所有 replica 设置为 OfflineReplica。
   *    向其他 replica 发送 StopReplicaRequest 请求
   *    并向 leader 副本发送 LeaderAndIsrRequest 请求，参数中携带了发生减员的 ISR。
   *    当 leader 副本被设置为 OfflineReplica 时，不会发送 LeaderAndIsrRequest，
   *    因为 leader 已经不存在了(被更新为-1)。
   * 3. 将所有的 replica 加入 ReplicaDeletionStarted 列表。
   *    发送 StopReplicaRequest(deletePartition=true) 给 replica，
   *    然后从磁盘中删除数据
   *
   */
  private def onPartitionDeletion(topicsToBeDeleted: Set[String]): Unit = {
    val allDeadReplicas = mutable.ListBuffer.empty[PartitionAndReplica]
    val allReplicasForDeletionRetry = mutable.ListBuffer.empty[PartitionAndReplica]
    val allTopicsIneligibleForDeletion = mutable.Set.empty[String]

    topicsToBeDeleted.foreach { topic =>

      // 此处的 deadReplicas 指
      // 在 shuttingDownBrokerIds 中的 broker (收到关机请求 ApiKeys.CONTROLLED_SHUTDOWN 的 broker)
      // 或者 无法正常响应 LeaderAndIsr 请求的 broker
      val (aliveReplicas, deadReplicas) = controllerContext.replicasForTopic(topic).partition { r =>
        controllerContext.isReplicaOnline(r.replica, r.topicPartition)
      }

      val successfullyDeletedReplicas = controllerContext.replicasInState(topic, ReplicaDeletionSuccessful)
      val replicasForDeletionRetry = aliveReplicas.diff(successfullyDeletedReplicas)

      // 统计宕机/关机的 broker，设置为 ReplicaDeletionIneligible
      allDeadReplicas ++= deadReplicas

      // 统计尚未删除的 replica，执行删除任务
      allReplicasForDeletionRetry ++= replicasForDeletionRetry

      // 大原则，关机/挂了的 broker 不能删，因为它们处理不了请求
      if (deadReplicas.nonEmpty) {
        debug(s"Dead Replicas (${deadReplicas.mkString(",")}) found for topic $topic")
        allTopicsIneligibleForDeletion += topic
      }
    }

    // 统计宕机/关机的 broker，设置为 ReplicaDeletionIneligible
    replicaStateMachine.handleStateChanges(allDeadReplicas, ReplicaDeletionIneligible)

    // send stop replica to all followers that are not in the OfflineReplica state so they stop sending fetch requests to the leader
    replicaStateMachine.handleStateChanges(allReplicasForDeletionRetry, OfflineReplica)
    replicaStateMachine.handleStateChanges(allReplicasForDeletionRetry, ReplicaDeletionStarted)

    if (allTopicsIneligibleForDeletion.nonEmpty) {
      markTopicIneligibleForDeletion(allTopicsIneligibleForDeletion, reason = "offline replicas")
    }
  }

  private def resumeDeletions(): Unit = {

    // 拷贝一份待删除 topic 列表
    val topicsQueuedForDeletion = Set.empty[String] ++ controllerContext.topicsToBeDeleted

    val topicsEligibleForRetry = mutable.Set.empty[String]
    val topicsEligibleForDeletion = mutable.Set.empty[String]

    if (topicsQueuedForDeletion.nonEmpty)
      info(s"Handling deletion for topics ${topicsQueuedForDeletion.mkString(",")}")

    // 逐个检查 topic 及其 replica 所处的状态，
    // 判断 是否可以删除，是否需要重试
    topicsQueuedForDeletion.foreach { topic =>

      // if all replicas are marked as deleted successfully, then topic deletion is done
      // 如果所有的 replica 都被标记为 删除成功，就认为 topic 删除完成
      if (controllerContext.areAllReplicasInState(topic, ReplicaDeletionSuccessful)) {

        // clear up all state for this topic from controller cache and zookeeper
        completeDeleteTopic(topic)

        info(s"Deletion of topic $topic successfully completed")
      } else if (!controllerContext.isAnyReplicaInState(topic, ReplicaDeletionStarted)) {
        // 相当于 controllerContext.allReplicaNotInState(topic, ReplicaDeletionStarted))

        // if you come here, then no replica is in TopicDeletionStarted and all replicas are not in
        // TopicDeletionSuccessful. That means, that either given topic haven't initiated deletion
        // or there is at least one failed replica (which means topic deletion should be retried).
        //
        // 到这里，
        // 所有的 replica 都不在 ReplicaDeletionStarted 状态，
        // 并且存在 replica 不在 ReplicaDeletionSuccessful 状态，
        // 这表示，
        // 这个 topic 可能还没有初始化删除任务，或者有一个 replica 删除失败了
        // 即，处于 Ineligible to delete 状态
        //
        // 这时候就需要重试删除任务
        if (controllerContext.isAnyReplicaInState(topic, ReplicaDeletionIneligible)) {
          topicsEligibleForRetry += topic
        }
      }

      // Add topic to the eligible set if it is eligible for deletion.
      //
      // 下列条件都满足时，可以删除 topic
      // 1. topicsToBeDeleted 中包含 topic
      // 2. replica 都不处于 ReplicaDeletionStarted 状态
      // 3. topicsIneligibleForDeletion 中不包含 topic
      if (isTopicEligibleForDeletion(topic)) {
        info(s"Deletion of topic $topic (re)started")
        topicsEligibleForDeletion += topic
      }
    }

    // 将 ReplicaDeletionIneligible 状态的 replica，设置为 OfflineReplica
    if (topicsEligibleForRetry.nonEmpty) {
      retryDeletionForIneligibleReplicas(topicsEligibleForRetry)
    }

    // 执行 topic deletion
    if (topicsEligibleForDeletion.nonEmpty) {
      onTopicDeletion(topicsEligibleForDeletion)
    }
  }
}
