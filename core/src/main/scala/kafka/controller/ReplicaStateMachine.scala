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

import kafka.api.LeaderAndIsr
import kafka.common.StateChangeFailedException
import kafka.server.KafkaConfig
import kafka.utils.Implicits._
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zk.TopicPartitionStateZNode
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.zookeeper.KeeperException.Code
import scala.collection.{Seq, mutable}

abstract class ReplicaStateMachine(controllerContext: ControllerContext) extends Logging {
  /**
   * Invoked on successful controller election.
   */
  def startup(): Unit = {
    info("Initializing replica state")
    initializeReplicaState()
    info("Triggering online replica state changes")
    val (onlineReplicas, offlineReplicas) = controllerContext.onlineAndOfflineReplicas
    handleStateChanges(onlineReplicas.toSeq, OnlineReplica)
    info("Triggering offline replica state changes")
    handleStateChanges(offlineReplicas.toSeq, OfflineReplica)
    debug(s"Started replica state machine with initial state -> ${controllerContext.replicaStates}")
  }

  /**
   * Invoked on controller shutdown.
   */
  def shutdown(): Unit = {
    info("Stopped replica state machine")
  }

  /**
   * Invoked on startup of the replica's state machine to set the initial state for replicas of all existing partitions
   * in zookeeper
   */
  private def initializeReplicaState(): Unit = {
    controllerContext.allPartitions.foreach { partition =>
      val replicas = controllerContext.partitionReplicaAssignment(partition)
      replicas.foreach { replicaId =>
        val partitionAndReplica = PartitionAndReplica(partition, replicaId)
        if (controllerContext.isReplicaOnline(replicaId, partition)) {
          controllerContext.putReplicaState(partitionAndReplica, OnlineReplica)
        } else {
          // mark replicas on dead brokers as failed for topic deletion, if they belong to a topic to be deleted.
          // This is required during controller failover since during controller failover a broker can go down,
          // so the replicas on that broker should be moved to ReplicaDeletionIneligible to be on the safer side.
          controllerContext.putReplicaState(partitionAndReplica, ReplicaDeletionIneligible)
        }
      }
    }
  }

  def handleStateChanges(replicas: Seq[PartitionAndReplica], targetState: ReplicaState): Unit
}

/**
 * This class represents the state machine for replicas. It defines the states that a replica can be in, and
 * transitions to move the replica to another legal state. The different states that a replica can be in are -
 * 1. NewReplica        : The controller can create new replicas during partition reassignment. In this state, a
 *                        replica can only get become follower state change request.  Valid previous
 *                        state is NonExistentReplica
 * 2. OnlineReplica     : Once a replica is started and part of the assigned replicas for its partition, it is in this
 *                        state. In this state, it can get either become leader or become follower state change requests.
 *                        Valid previous state are NewReplica, OnlineReplica or OfflineReplica
 * 3. OfflineReplica    : If a replica dies, it moves to this state. This happens when the broker hosting the replica
 *                        is down. Valid previous state are NewReplica, OnlineReplica
 * 4. ReplicaDeletionStarted: If replica deletion starts, it is moved to this state. Valid previous state is OfflineReplica
 * 5. ReplicaDeletionSuccessful: If replica responds with no error code in response to a delete replica request, it is
 *                        moved to this state. Valid previous state is ReplicaDeletionStarted
 * 6. ReplicaDeletionIneligible: If replica deletion fails, it is moved to this state. Valid previous states are
 *                        ReplicaDeletionStarted and OfflineReplica
 * 7. NonExistentReplica: If a replica is deleted successfully, it is moved to this state. Valid previous state is
 *                        ReplicaDeletionSuccessful
 */
class ZkReplicaStateMachine(config: KafkaConfig,
                            stateChangeLogger: StateChangeLogger,
                            controllerContext: ControllerContext,
                            zkClient: KafkaZkClient,
                            controllerBrokerRequestBatch: ControllerBrokerRequestBatch)
  extends ReplicaStateMachine(controllerContext) with Logging {

  private val controllerId = config.brokerId
  this.logIdent = s"[ReplicaStateMachine controllerId=$controllerId] "

  override def handleStateChanges(replicas: Seq[PartitionAndReplica], targetState: ReplicaState): Unit = {
    if (replicas.nonEmpty) {
      try {
        // 确保缓存中没有未发送的请求，如果有的话会抛出异常
        controllerBrokerRequestBatch.newBatch()

        // 进行状态转换。
        // 注意这里是根据 replicaId 分组执行的，
        // 即一次将一个 broker 的请求都处理完，避免多次发送请求。
        //
        // 状态转换过程后，如果有以下三类请求需要发送，
        // 1. LeaderAndIsr
        // 2. UpdateMetadata
        // 3. StopReplica
        // 会先暂存起来，然后通过下文的 sendRequestsToBrokers 方法发送给其他 broker
        replicas.groupBy(_.replica).forKeyValue { (replicaId, replicas) =>
          doHandleStateChanges(replicaId, replicas, targetState)
        }

        // 将上文暂存的请求发送给 broker
        controllerBrokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch)

      } catch {
        case e: ControllerMovedException =>
          error(s"Controller moved to another broker when moving some replicas to $targetState state", e)
          throw e
        case e: Throwable => error(s"Error while moving some replicas to $targetState state", e)
      }
    }
  }

  /**
   * This API exercises the replica's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state. Valid state transitions are:
   * NonExistentReplica --> NewReplica
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   *   partition to every live broker
   *
   * NewReplica -> OnlineReplica
   * --add the new replica to the assigned replica list if needed
   *
   * OnlineReplica,OfflineReplica -> OnlineReplica
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   *   partition to every live broker
   *
   * NewReplica,OnlineReplica,OfflineReplica,ReplicaDeletionIneligible -> OfflineReplica
   * --send StopReplicaRequest to the replica (w/o deletion)
   * --remove this replica from the isr and send LeaderAndIsr request (with new isr) to the leader replica and
   *   UpdateMetadata request for the partition to every live broker.
   *
   * OfflineReplica -> ReplicaDeletionStarted
   * --send StopReplicaRequest to the replica (with deletion)
   *
   * ReplicaDeletionStarted -> ReplicaDeletionSuccessful
   * -- mark the state of the replica in the state machine
   *
   * ReplicaDeletionStarted -> ReplicaDeletionIneligible
   * -- mark the state of the replica in the state machine
   *
   * ReplicaDeletionSuccessful -> NonExistentReplica
   * -- remove the replica from the in memory partition replica assignment cache
   *
   * @param replicaId The replica for which the state transition is invoked
   * @param replicas The partitions on this replica for which the state transition is invoked
   * @param targetState The end state that the replica should be moved to
   */
  private def doHandleStateChanges(replicaId: Int, replicas: Seq[PartitionAndReplica], targetState: ReplicaState): Unit = {
    val stateLogger = stateChangeLogger.withControllerEpoch(controllerContext.epoch)
    val traceEnabled = stateLogger.isTraceEnabled

    // 新加入的 replica，其状态都会被设置为 NonExistentReplica，然后再开始流转
    replicas.foreach(replica => controllerContext.putReplicaStateIfNotExists(replica, NonExistentReplica))

    // 状态机之间有严格的流转关系，这里过略掉非法的状态流转
    val (validReplicas, invalidReplicas) = controllerContext.checkValidReplicaStateChange(replicas, targetState)
    invalidReplicas.foreach(replica => logInvalidTransition(replica, targetState))

    // 根据目标状态，流转状态，执行具体操作
    targetState match {
      case NewReplica =>
        validReplicas.foreach { replica =>
          val partition = replica.topicPartition
          val currentState = controllerContext.replicaState(replica)

          controllerContext.partitionLeadershipInfo(partition) match {
            case Some(leaderIsrAndControllerEpoch) =>
              if (leaderIsrAndControllerEpoch.leaderAndIsr.leader == replicaId) {
                val exception = new StateChangeFailedException(s"Replica $replicaId for partition $partition cannot be moved to NewReplica state as it is being requested to become leader")
                logFailedStateChange(replica, currentState, OfflineReplica, exception)
              } else {
                controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(replicaId),
                  replica.topicPartition,
                  leaderIsrAndControllerEpoch,
                  controllerContext.partitionFullReplicaAssignment(replica.topicPartition),
                  isNew = true)
                if (traceEnabled)
                  logSuccessfulTransition(stateLogger, replicaId, partition, currentState, NewReplica)
                controllerContext.putReplicaState(replica, NewReplica)
              }
            case None =>
              if (traceEnabled)
                logSuccessfulTransition(stateLogger, replicaId, partition, currentState, NewReplica)
              controllerContext.putReplicaState(replica, NewReplica)
          }
        }
      case OnlineReplica =>
        validReplicas.foreach { replica =>
          val partition = replica.topicPartition
          val currentState = controllerContext.replicaState(replica)

          currentState match {
            case NewReplica =>
              val assignment = controllerContext.partitionFullReplicaAssignment(partition)
              if (!assignment.replicas.contains(replicaId)) {
                error(s"Adding replica ($replicaId) that is not part of the assignment $assignment")
                val newAssignment = assignment.copy(replicas = assignment.replicas :+ replicaId)
                controllerContext.updatePartitionFullReplicaAssignment(partition, newAssignment)
              }
            case _ =>
              controllerContext.partitionLeadershipInfo(partition) match {
                case Some(leaderIsrAndControllerEpoch) =>
                  controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(replicaId),
                    replica.topicPartition,
                    leaderIsrAndControllerEpoch,
                    controllerContext.partitionFullReplicaAssignment(partition), isNew = false)
                case None =>
              }
          }
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, partition, currentState, OnlineReplica)
          controllerContext.putReplicaState(replica, OnlineReplica)
        }
      case OfflineReplica =>

        // 这里的 validReplicas 类型是 PartitionAndReplica

        // 准备发送 StopReplica 请求，注意这里不是直接发送，只是将请求先暂存起来
        // 等状态机处理完后，再统一发送。
        // (broker 在收到 StopReplica 请求后，会从 ctx 中移除 replica 的信息，并停止拉取数据)
        validReplicas.foreach { replica =>
          controllerBrokerRequestBatch.addStopReplicaRequestForBrokers(Seq(replicaId), replica.topicPartition, deletePartition = false)
        }

        // 查询 ctx 中的分区 leader 信息
        val (replicasWithLeadershipInfo, replicasWithoutLeadershipInfo) = validReplicas.partition { replica =>
          controllerContext.partitionLeadershipInfo(replica.topicPartition).isDefined
        }

        // 对于 ctx 中有 leader 信息的分区，将 replica 从 ISR 中移除
        // 更新 ctx，同时更新到 zk 的节点:
        // /brokers/topics/[topic_name]/partitions/[partition_id]/state
        val updatedLeaderIsrAndControllerEpochs = removeReplicasFromIsr(replicaId, replicasWithLeadershipInfo.map(_.topicPartition))

        updatedLeaderIsrAndControllerEpochs.forKeyValue { (partition, leaderIsrAndControllerEpoch) =>
          stateLogger.info(s"Partition $partition state changed to $leaderIsrAndControllerEpoch after removing replica $replicaId from the ISR as part of transition to $OfflineReplica")

          // 如果不是待删除的 topic，需要发送 LeaderAndIsr 请求给其余 broker
          if (!controllerContext.isTopicQueuedUpForDeletion(partition.topic)) {
            val recipients = controllerContext.partitionReplicaAssignment(partition).filterNot(_ == replicaId)
            controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(recipients,
              partition,
              leaderIsrAndControllerEpoch,
              controllerContext.partitionFullReplicaAssignment(partition), isNew = false)
          }

          // 记录日志
          val replica = PartitionAndReplica(partition, replicaId)
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, partition, currentState, OfflineReplica)

          // 最终设置为 offline
          controllerContext.putReplicaState(replica, OfflineReplica)
        }

        // 对于 ctx 中找不到 leader 信息的分区，记录日志并设置为 offline
        replicasWithoutLeadershipInfo.foreach { replica =>
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, OfflineReplica)

          // 由于找不到 leader 信息，所以这个方法里仅记录日志，什么都不做
          // "Leader not yet assigned for partition $partition
          //  Skip sending UpdateMetadataRequest."
          controllerBrokerRequestBatch.addUpdateMetadataRequestForBrokers(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(replica.topicPartition))

          // 最终设置为 offline
          controllerContext.putReplicaState(replica, OfflineReplica)
        }

      case ReplicaDeletionStarted =>
        validReplicas.foreach { replica =>
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, ReplicaDeletionStarted)

          // 设置为 ReplicaDeletionStarted
          controllerContext.putReplicaState(replica, ReplicaDeletionStarted)

          // enqueue stopReplica 请求
          // 注意这里 deletePartition = true，所以
          // broker 在收到 StopReplica 请求后，会停止拉取数据，
          // 还会将对应的分区(副本)目录加上 -delete 后缀，最终会删除掉整个 log 目录
          controllerBrokerRequestBatch.addStopReplicaRequestForBrokers(Seq(replicaId), replica.topicPartition, deletePartition = true)

          // 这里有一个疑问，发送了StopReplica，为什么不发送 UpdateMetadata 请求更新元数据呢？
          // 我理解是，ReplicaDeletionStarted 状态，是由 OfflineReplica 状态流转过来的，
          // 在 OfflineReplica 状态下，已经发送了 UpdateMetadata 更新过元数据了，
          // 上面这段代码只是做状态的流转，并没有其他变更，所以不需要再发送 UpdateMetadata 请求
        }
      case ReplicaDeletionIneligible =>
        validReplicas.foreach { replica =>
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, ReplicaDeletionIneligible)
          controllerContext.putReplicaState(replica, ReplicaDeletionIneligible)
        }
      case ReplicaDeletionSuccessful =>
        validReplicas.foreach { replica =>
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, ReplicaDeletionSuccessful)
          controllerContext.putReplicaState(replica, ReplicaDeletionSuccessful)
        }
      case NonExistentReplica =>

        // 这里的 replica 类型是 PartitionAndReplica
        validReplicas.foreach { replica =>

          // 查出 PartitionAndReplica 当前 state
          val currentState = controllerContext.replicaState(replica)

          // 从 ctx 中删除当前 replica
          val newAssignedReplicas = controllerContext
            // 根据 topic 和 partition，查出所有的 ReplicaAssignment 列表
            .partitionFullReplicaAssignment(replica.topicPartition)
            // 删除当前 replicaId
            .removeReplica(replica.replica)
          controllerContext.updatePartitionFullReplicaAssignment(replica.topicPartition, newAssignedReplicas)

          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, NonExistentReplica)

          // 从状态机列表中删除当前 replica 的状态机，它的生命周期就结束了
          controllerContext.removeReplicaState(replica)
        }
    }
  }

  /**
   * Repeatedly attempt to remove a replica from the isr of multiple partitions until there are no more remaining partitions
   * to retry.
   * @param replicaId The replica being removed from isr of multiple partitions
   * @param partitions The partitions from which we're trying to remove the replica from isr
   * @return The updated LeaderIsrAndControllerEpochs of all partitions for which we successfully removed the replica from isr.
   */
  private def removeReplicasFromIsr(
    replicaId: Int,
    partitions: Seq[TopicPartition]
  ): Map[TopicPartition, LeaderIsrAndControllerEpoch] = {
    var results = Map.empty[TopicPartition, LeaderIsrAndControllerEpoch]
    var remaining = partitions
    while (remaining.nonEmpty) {
      val (finishedRemoval, removalsToRetry) = doRemoveReplicasFromIsr(replicaId, remaining)
      remaining = removalsToRetry

      finishedRemoval.foreach {
        // 移出 isr 过程中出现异常，则记录日志
        case (partition, Left(e)) =>
            val replica = PartitionAndReplica(partition, replicaId)
            val currentState = controllerContext.replicaState(replica)
            logFailedStateChange(replica, currentState, OfflineReplica, e)

        // 成功移出 isr，记录结果
        case (partition, Right(leaderIsrAndEpoch)) =>
          results += partition -> leaderIsrAndEpoch
      }
    }
    results
  }

  /**
   * Try to remove a replica from the isr of multiple partitions.
   * Removing a replica from isr updates partition state in zookeeper.
   *
   * @param replicaId The replica being removed from isr of multiple partitions
   * @param partitions The partitions from which we're trying to remove the replica from isr
   * @return A tuple of two elements:
   *         1. The updated Right[LeaderIsrAndControllerEpochs] of all partitions for which we successfully
   *         removed the replica from isr. Or Left[Exception] corresponding to failed removals that should
   *         not be retried
   *         2. The partitions that we should retry due to a zookeeper BADVERSION conflict. Version conflicts can occur if
   *         the partition leader updated partition state while the controller attempted to update partition state.
   */
  private def doRemoveReplicasFromIsr(
    replicaId: Int,
    partitions: Seq[TopicPartition]
  ): (Map[TopicPartition, Either[Exception, LeaderIsrAndControllerEpoch]], Seq[TopicPartition]) = {

    // 查询 /brokers/topics/[topic_name]/partitions/[partition_id]/state 下的数据
    // 数据长这样:
    // {
    //    "controller_epoch": 9,
    //    "leader": 0,
    //    "version": 1,
    //    "leader_epoch": 1,
    //    "isr": [0, 1]
    // }
    //
    // 如果在 zk 上找不到 node，就把 partition 放入 partitionsWithNoLeaderAndIsrInZk 中
    val (leaderAndIsrs, partitionsWithNoLeaderAndIsrInZk) = getTopicPartitionStatesFromZk(partitions)

    // 找出待移除的条目 (即 isr 中包含 replicaId 的条目)
    val (leaderAndIsrsWithReplica, leaderAndIsrsWithoutReplica) = leaderAndIsrs.partition { case (_, result) =>
      result.map { leaderAndIsr =>
        leaderAndIsr.isr.contains(replicaId)
      }.getOrElse(false)
    }

    // 调整 leader 和 ISR
    val adjustedLeaderAndIsrs: Map[TopicPartition, LeaderAndIsr] = leaderAndIsrsWithReplica.flatMap {
      case (partition, result) =>
        result.toOption.map { leaderAndIsr =>
          // 确定 leader。如果待移除的 replicaId 是 leader，则将 leader_id 设置为 -1
          val newLeader = if (replicaId == leaderAndIsr.leader) LeaderAndIsr.NoLeader else leaderAndIsr.leader
          // 确定 isr。如果 isr 中只有一个元素，而且是 replicaId，则不变。否则，从查询出的 isr 中移除 replicaId
          val adjustedIsr = if (leaderAndIsr.isr.size == 1) leaderAndIsr.isr else leaderAndIsr.isr.filter(_ != replicaId)
          partition -> leaderAndIsr.newLeaderAndIsr(newLeader, adjustedIsr)
        }
    }

    // 更新 zk 上的数据
    // /brokers/topics/[topic_name]/partitions/[partition_id]/state
    val UpdateLeaderAndIsrResult(finishedPartitions, updatesToRetry) = zkClient.updateLeaderAndIsr(
      adjustedLeaderAndIsrs, controllerContext.epoch, controllerContext.epochZkVersion)

    // zk 上找不到 node 并且 partition 不在删除队列中，找出这部分数据
    val exceptionsForPartitionsWithNoLeaderAndIsrInZk: Map[TopicPartition, Either[Exception, LeaderIsrAndControllerEpoch]] =
      partitionsWithNoLeaderAndIsrInZk.iterator.flatMap { partition =>
        if (!controllerContext.isTopicQueuedUpForDeletion(partition.topic)) {
          val exception = new StateChangeFailedException(
            s"Failed to change state of replica $replicaId for partition $partition since the leader and isr " +
            "path in zookeeper is empty"
          )
          Option(partition -> Left(exception))
        } else None
      }.toMap

    // 收集: 不需要做删除操作的 ++ 执行完删除操作的
    val leaderIsrAndControllerEpochs: Map[TopicPartition, Either[Exception, LeaderIsrAndControllerEpoch]] = {

      // 更新 ctx 中的 LeaderAndIsr 信息
      (leaderAndIsrsWithoutReplica ++ finishedPartitions).map { case (partition, result) =>
        (partition, result.map { leaderAndIsr =>
          val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch)
          controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
          leaderIsrAndControllerEpoch
        })
      }
    }

    if (isDebugEnabled) {
      updatesToRetry.foreach { partition =>
        debug(s"Controller failed to remove replica $replicaId from ISR of partition $partition. " +
          s"Attempted to write state ${adjustedLeaderAndIsrs(partition)}, but failed with bad ZK version. This will be retried.")
      }
    }

    // 返回结果
    // Map[TopicPartition, Either[Exception, LeaderIsrAndControllerEpoch]]
    // 对于执行成功的, value 是 LeaderIsrAndControllerEpoch
    // 对于没成功的(不属于删除队列，且在 zk 上不存在), value 是 Exception
    (leaderIsrAndControllerEpochs ++ exceptionsForPartitionsWithNoLeaderAndIsrInZk, updatesToRetry)
  }

  /**
   * Gets the partition state from zookeeper
   * @param partitions the partitions whose state we want from zookeeper
   * @return A tuple of two values:
   *         1. The Right(LeaderAndIsrs) of partitions whose state we successfully read from zookeeper.
   *         The Left(Exception) to failed zookeeper lookups or states whose controller epoch exceeds our current epoch
   *         2. The partitions that had no leader and isr state in zookeeper. This happens if the controller
   *         didn't finish partition initialization.
   */
  private def getTopicPartitionStatesFromZk(
    partitions: Seq[TopicPartition]
  ): (Map[TopicPartition, Either[Exception, LeaderAndIsr]], Seq[TopicPartition]) = {
    val getDataResponses = try {
      zkClient.getTopicPartitionStatesRaw(partitions)
    } catch {
      case e: Exception =>
        return (partitions.iterator.map(_ -> Left(e)).toMap, Seq.empty)
    }

    val partitionsWithNoLeaderAndIsrInZk = mutable.Buffer.empty[TopicPartition]
    val result = mutable.Map.empty[TopicPartition, Either[Exception, LeaderAndIsr]]

    getDataResponses.foreach[Unit] { getDataResponse =>
      val partition = getDataResponse.ctx.get.asInstanceOf[TopicPartition]
      if (getDataResponse.resultCode == Code.OK) {
        TopicPartitionStateZNode.decode(getDataResponse.data, getDataResponse.stat) match {
          case None =>
            partitionsWithNoLeaderAndIsrInZk += partition
          case Some(leaderIsrAndControllerEpoch) =>
            if (leaderIsrAndControllerEpoch.controllerEpoch > controllerContext.epoch) {
              val exception = new StateChangeFailedException(
                "Leader and isr path written by another controller. This probably " +
                s"means the current controller with epoch ${controllerContext.epoch} went through a soft failure and " +
                s"another controller was elected with epoch ${leaderIsrAndControllerEpoch.controllerEpoch}. Aborting " +
                "state change by this controller"
              )
              result += (partition -> Left(exception))
            } else {
              result += (partition -> Right(leaderIsrAndControllerEpoch.leaderAndIsr))
            }
        }
      } else if (getDataResponse.resultCode == Code.NONODE) {
        partitionsWithNoLeaderAndIsrInZk += partition
      } else {
        result += (partition -> Left(getDataResponse.resultException.get))
      }
    }

    (result.toMap, partitionsWithNoLeaderAndIsrInZk)
  }

  private def logSuccessfulTransition(logger: StateChangeLogger, replicaId: Int, partition: TopicPartition,
                                      currState: ReplicaState, targetState: ReplicaState): Unit = {
    logger.trace(s"Changed state of replica $replicaId for partition $partition from $currState to $targetState")
  }

  private def logInvalidTransition(replica: PartitionAndReplica, targetState: ReplicaState): Unit = {
    val currState = controllerContext.replicaState(replica)
    val e = new IllegalStateException(s"Replica $replica should be in the ${targetState.validPreviousStates.mkString(",")} " +
      s"states before moving to $targetState state. Instead it is in $currState state")
    logFailedStateChange(replica, currState, targetState, e)
  }

  private def logFailedStateChange(replica: PartitionAndReplica, currState: ReplicaState, targetState: ReplicaState, t: Throwable): Unit = {
    stateChangeLogger.withControllerEpoch(controllerContext.epoch)
      .error(s"Controller $controllerId epoch ${controllerContext.epoch} initiated state change of replica ${replica.replica} " +
        s"for partition ${replica.topicPartition} from $currState to $targetState failed", t)
  }
}

sealed trait ReplicaState {
  def state: Byte
  def validPreviousStates: Set[ReplicaState]
}

case object NewReplica extends ReplicaState {
  val state: Byte = 1
  val validPreviousStates: Set[ReplicaState] = Set(NonExistentReplica)
}

case object OnlineReplica extends ReplicaState {
  val state: Byte = 2
  val validPreviousStates: Set[ReplicaState] = Set(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible)
}

case object OfflineReplica extends ReplicaState {
  val state: Byte = 3
  val validPreviousStates: Set[ReplicaState] = Set(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible)
}

case object ReplicaDeletionStarted extends ReplicaState {
  val state: Byte = 4
  val validPreviousStates: Set[ReplicaState] = Set(OfflineReplica)
}

case object ReplicaDeletionSuccessful extends ReplicaState {
  val state: Byte = 5
  val validPreviousStates: Set[ReplicaState] = Set(ReplicaDeletionStarted)
}

case object ReplicaDeletionIneligible extends ReplicaState {
  val state: Byte = 6
  val validPreviousStates: Set[ReplicaState] = Set(OfflineReplica, ReplicaDeletionStarted)
}

case object NonExistentReplica extends ReplicaState {
  val state: Byte = 7
  val validPreviousStates: Set[ReplicaState] = Set(ReplicaDeletionSuccessful)
}
