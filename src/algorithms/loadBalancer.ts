/**
 * Advanced Load Balancing Algorithm
 * Implements round-robin, least-connections, and weighted strategies
 */

interface Server {
  id: string;
  address: string;
  port: number;
  weight: number;
  activeConnections: number;
  healthScore: number;
}

interface LoadBalanceStrategy {
  name: 'round-robin' | 'least-connections' | 'weighted';
  selectServer(servers: Server[]): Server | null;
}

class LoadBalancer {
  private servers: Map<string, Server>;
  private currentIndex: number = 0;
  private strategy: LoadBalanceStrategy;

  constructor(strategy: 'round-robin' | 'least-connections' | 'weighted' = 'round-robin') {
    this.servers = new Map();
    this.strategy = this.createStrategy(strategy);
  }

  /**
   * Add server to load balancer
   */
  addServer(server: Server): void {
    this.servers.set(server.id, server);
  }

  /**
   * Remove server from load balancer
   */
  removeServer(serverId: string): void {
    this.servers.delete(serverId);
  }

  /**
   * Get next server based on strategy
   */
  getNextServer(): Server | null {
    const healthyServers = Array.from(this.servers.values())
      .filter(s => s.healthScore > 0.5);

    if (healthyServers.length === 0) return null;
    return this.strategy.selectServer(healthyServers);
  }

  /**
   * Record request on server
   */
  recordRequest(serverId: string): void {
    const server = this.servers.get(serverId);
    if (server) {
      server.activeConnections++;
    }
  }

  /**
   * Release connection
   */
  releaseConnection(serverId: string): void {
    const server = this.servers.get(serverId);
    if (server && server.activeConnections > 0) {
      server.activeConnections--;
    }
  }

  /**
   * Update server health score
   */
  updateHealth(serverId: string, score: number): void {
    const server = this.servers.get(serverId);
    if (server) {
      server.healthScore = Math.min(1, Math.max(0, score));
    }
  }

  /**
   * Create strategy implementation
   */
  private createStrategy(type: string): LoadBalanceStrategy {
    switch (type) {
      case 'least-connections':
        return {
          name: 'least-connections',
          selectServer: (servers: Server[]) => {
            return servers.reduce((min, s) =>
              s.activeConnections < min.activeConnections ? s : min
            );
          },
        };
      case 'weighted':
        return {
          name: 'weighted',
          selectServer: (servers: Server[]) => {
            const totalWeight = servers.reduce((sum, s) => sum + s.weight, 0);
            let random = Math.random() * totalWeight;
            for (const server of servers) {
              random -= server.weight;
              if (random <= 0) return server;
            }
            return servers[0] || null;
          },
        };
      default:
        return {
          name: 'round-robin',
          selectServer: (servers: Server[]) => {
            const server = servers[this.currentIndex % servers.length];
            this.currentIndex++;
            return server;
          },
        };
    }
  }

  /**
   * Get load balancer statistics
   */
  getStats() {
    const servers = Array.from(this.servers.values());
    return {
      totalServers: servers.length,
      healthyServers: servers.filter(s => s.healthScore > 0.5).length,
      totalConnections: servers.reduce((sum, s) => sum + s.activeConnections, 0),
      strategy: this.strategy.name,
    };
  }
}

export default LoadBalancer;
