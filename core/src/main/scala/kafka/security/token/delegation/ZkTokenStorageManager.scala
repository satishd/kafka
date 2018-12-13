/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.security.token.delegation

import kafka.common.{NotificationHandler, ZkNodeChangeNotificationListener}
import kafka.utils.Logging
import kafka.zk.{DelegationTokenChangeNotificationSequenceZNode, DelegationTokenChangeNotificationZNode, DelegationTokensZNode, KafkaZkClient}
import org.apache.kafka.common.security.token.delegation.{DelegationToken, TokenInformation}

class ZkTokenStorageManager(zkClient: KafkaZkClient, masterKeyStr: String) extends TokenStorageManager with Logging {

  private var tokenChangeListener: ZkNodeChangeNotificationListener = _
  private var masterKey : Array[Byte] = _

  override def registerNotificationHandler(notificationHandler: NotificationHandler): Unit = {
    tokenChangeListener = new ZkNodeChangeNotificationListener(zkClient, DelegationTokenChangeNotificationZNode.path, DelegationTokenChangeNotificationSequenceZNode.SequenceNumberPrefix, notificationHandler)
    tokenChangeListener.init()
  }

  override def init(): Unit = {
    zkClient.createDelegationTokenPaths()
    masterKey = zkClient.createOrGetDelegationTokenMasterKey(masterKeyStr)
  }

  override def fetchMasterKey() : Option[Array[Byte]] = {
    zkClient.getDelegationTokenMasterKey()
  }

  override def fetchAllTokens(): Seq[String] = {
    zkClient.getChildren(DelegationTokensZNode.path)
  }

  override def getDelegationTokenInfo(tokenId: String): Option[TokenInformation] = {
    zkClient.getDelegationTokenInfo(tokenId)
  }

  override def setOrCreateDelegationToken(token: DelegationToken): Unit = {
    zkClient.setOrCreateDelegationToken(token)
  }

  override def createTokenChangeNotification(tokenId: String): Unit = {
    zkClient.createTokenChangeNotification(tokenId)
  }

  override def deleteDelegationToken(tokenId: String): Unit = {
    zkClient.deleteDelegationToken(tokenId)
  }

  def close(): Unit = {
    if(tokenChangeListener != null) tokenChangeListener.close()
  }
}
