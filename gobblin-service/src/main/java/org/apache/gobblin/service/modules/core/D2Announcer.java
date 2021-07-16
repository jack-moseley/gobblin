package org.apache.gobblin.service.modules.core;


/**
 * Interface for marking up/down D2 servers on gobblin service startup. This is only required if using delayed announcement.
 */
public interface D2Announcer {
  /**
   * Mark up D2 servers
   */
  void markUpServers();

  /**
   * Mark down D2 servers
   */
  void markDownServers();
}
