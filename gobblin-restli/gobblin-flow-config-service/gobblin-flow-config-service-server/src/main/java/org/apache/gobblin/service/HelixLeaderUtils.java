/*
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

package org.apache.gobblin.service;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.LiveInstance;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.linkedin.data.DataMap;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.util.ConfigUtils;


/**
 * Utils for storing/parsing helix URL in the helix instance name
 */
@Slf4j
public class HelixLeaderUtils {
  public static String HELIX_INSTANCE_NAME_SEPARATOR = "@";

  /**
   */
  private static String getUrlFromHelixInstanceName(String helixInstanceName) {
    if (!helixInstanceName.contains(HELIX_INSTANCE_NAME_SEPARATOR)) {
      return null;
    } else {
      return helixInstanceName.substring(helixInstanceName.indexOf(HELIX_INSTANCE_NAME_SEPARATOR) + 1);
    }
  }

  private static String getLeaderUrl(HelixManager helixManager) {
    PropertyKey key = helixManager.getHelixDataAccessor().keyBuilder().controllerLeader();
    LiveInstance leader = helixManager.getHelixDataAccessor().getProperty(key);
    return getUrlFromHelixInstanceName(leader.getInstanceName());
  }

  /**
   * If this host is not the leader, throw a {@link RestLiServiceException}, and include the URL of the leader host in
   * the message and in the errorDetails under the key {@link ServiceConfigKeys#LEADER_URL}.
   */
  public static void throwErrorIfNotLeader(Optional<HelixManager> helixManager)  {
    if (helixManager.isPresent() && !helixManager.get().isLeader()) {
      String leaderUrl = getLeaderUrl(helixManager.get());
      if (leaderUrl == null) {
        throw new RuntimeException("Request sent to slave node but could not get leader node URL");
      }
      RestLiServiceException exception = new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "Request must be sent to leader node at URL " + leaderUrl);
      exception.setErrorDetails(new DataMap(ImmutableMap.of(ServiceConfigKeys.LEADER_URL, leaderUrl)));
      throw exception;
    }
  }

  /**
   * Build helix instance name by getting {@link org.apache.gobblin.service.ServiceConfigKeys#HELIX_INSTANCE_NAME_KEY}
   * and appending the host, port, and service name with a separator
   */
  public static String buildHelixInstanceName(Config config, String defaultInstanceName) {
    String helixInstanceName = ConfigUtils.getString(config, ServiceConfigKeys.HELIX_INSTANCE_NAME_KEY, defaultInstanceName);

    String url = "";
    try {
      url = HELIX_INSTANCE_NAME_SEPARATOR + ConfigUtils.getString(config, ServiceConfigKeys.SERVICE_URL_PREFIX, "https://")
          + InetAddress.getLocalHost().getHostName() + ":" + ConfigUtils.getString(config, ServiceConfigKeys.SERVICE_PORT, "")
          + "/" + ConfigUtils.getString(config, ServiceConfigKeys.SERVICE_NAME, "");
    } catch (UnknownHostException e) {
      log.warn("Failed to append URL to helix instance name", e);
    }

    return helixInstanceName + url;
  }
}