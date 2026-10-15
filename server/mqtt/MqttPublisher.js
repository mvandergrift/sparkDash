import mqtt from "mqtt";
import { unitMetricJson } from "../prometheus.js";
import { unitDiscoveryMessages, dashboardDiscoveryMessages, DEFAULT_DISCOVERY_PREFIX } from "./haDiscovery.js";

const LOG_QUIET_MS = 30_000;
const DEFAULT_PREFIX = "sparkdash";

/**
 * Outbound MQTT bridge (opt-in, Settings → MQTT): republishes what sparkDash
 * already holds to a user-configured broker —
 *
 *   {prefix}/events            one JSON EventLog record per accepted event
 *   {prefix}/status/{sparkId}  retained "1"/"0", mirroring sparkdash_up
 *   {prefix}/metrics/{sparkId} retained JSON twin of the unit's Prometheus series
 *   {prefix}/status/dashboard  retained "1" while connected, LWT "0" otherwise
 *   {discovery}/…/config       Home Assistant discovery, when enabled
 *
 * Discovery configs are published retained when a unit's structure changes (a
 * GPU, LLM port, interface or disk appearing or the unit being added or
 * removed) and re-announced on reconnect, so a broker restart self-heals.
 *
 * Publishing never throws and never blocks a poll loop: while the broker is
 * unreachable the client keeps retrying on its own and publishes are skipped
 * until it is back. Best-effort — a long outage drops messages instead of
 * queueing them unboundedly.
 */
export class MqttPublisher {
  /**
   * @param {{ getSettings: () => object, env?: Record<string, string | undefined>, connect?: typeof mqtt.connect, log?: Pick<Console, "warn" | "error"> }} deps
   */
  constructor({ getSettings, env = process.env, connect = mqtt.connect, log = console } = {}) {
    this._getSettings = getSettings;
    this._env = env;
    this._connect = connect;
    this._log = log;
    /** @type {import("mqtt").MqttClient | null} */
    this._client = null;
    /** Broker URL, topic base and username the current client was built with, so settings PUTs can no-op. */
    this._url = null;
    this._base = null;
    this._user = null;
    this._lastQuietLog = 0;
    /** Per-unit discovery configs already at the broker, and whether the dashboard's own was announced this connection. */
    /** @type {Map<string, { json: string, messages: Array<{ topic: string, payload: string }> }>} */
    this._discoveryByUnit = new Map();
    this._discoveryAnnounced = false;
    this._discoveryPrefixInUse = null;
    /** @type {Array<{ topic: string, payload: string }> | null} */
    this._dashboardDiscovery = null;
    /** Last fleet entries seen, so a reconnect can refresh retained state at the broker. */
    this._lastFleet = null;
  }

  /**
   * Apply current settings: connects when enabled, reopens the connection when
   * the broker URL or topic prefix changed, tears down when disabled. Safe to
   * call on every settings PUT — a no-op when nothing relevant changed.
   */
  applySettings() {
    const s = this._getSettings();
    if (!s.mqttDiscovery) this._clearDiscovery();
    if (!s.mqttEnabled || !s.mqttUrl) return this._teardown();
    const base = this._prefix();
    const user = s.mqttUsername || "";
    if (this._client && this._url === s.mqttUrl && this._base === base && this._user === user) return;
    this._teardown();
    this._url = s.mqttUrl;
    this._base = base;
    this._user = user;
    try {
      this._client = this._connect(s.mqttUrl, {
        clientId: `sparkdash-${process.pid}`,
        reconnectPeriod: 5_000,
        connectTimeout: 10_000,
        username: user || undefined,
        // The password never touches settings.json (it is written world-readable);
        // it comes from the SPARKDASH_MQTT_PASSWORD environment variable.
        password: this._env.SPARKDASH_MQTT_PASSWORD || undefined,
        // LWT: the broker flips our availability to 0 if we die without a clean disconnect.
        will: { topic: `${base}/status/dashboard`, payload: "0", qos: 0, retain: true },
      });
    } catch (err) {
      this._client = null;
      this._url = null;
      this._base = null;
      return this._quietLog(`connect failed: ${err?.message}`);
    }
    this._client.on("connect", () => {
      // Fired on every (re)connect; retain keeps the last state across broker restarts.
      this._publish(`${base}/status/dashboard`, "1", { retain: true });
      // A restarted broker lost the retained discovery configs and state.
      this._republishDiscovery();
    });
    this._client.on("error", (err) => this._quietLog(`broker error: ${err?.message}`));
    // "close" / "reconnect" are expected — MQTT.js retries with reconnectPeriod.
  }

  /** Stream one EventLog record. Called from the EventLog's subscriber list. */
  publishEvent(event) {
    const s = this._getSettings();
    if (!s.mqttEnabled) return;
    this._publish(`${this._prefix()}/events`, JSON.stringify(event), { qos: 1 });
  }

  /**
   * Publish the fleet: retained availability plus a metrics payload per unit,
   * from the same entries the /metrics route renders (`metricsEntries()` output
   * — bare snapshots also work, they just lack collection provenance). The
   * metrics payload is the unit's `unitMetricJson()` with a `ts` added; an
   * offline unit gets `{up: 0}` so the metrics topic shows the unit went down,
   * matching the "sparkdash_up 0 and nothing else" rule.
   * @param {Array<object | { snapshot: object, collected?: object, collectedAt?: object } | null | undefined>} entries
   */
  publishFleet(entries) {
    const s = this._getSettings();
    if (!s.mqttEnabled || !Array.isArray(entries)) return;
    this._lastFleet = entries;
    const base = this._prefix();
    for (const entry of entries) {
      const snap = entry && typeof entry === "object" && entry.snapshot ? entry.snapshot : entry;
      if (!snap?.id) continue;
      const id = encodeURIComponent(snap.id);
      const up = snap.online === true;
      this._publish(`${base}/status/${id}`, up ? "1" : "0", { retain: true });
      const json = unitMetricJson(entry);
      const payload = { ts: Date.now(), ...json };
      // Retained, so a freshly (re)started Home Assistant sees the last state
      // immediately; the availability topics say how stale it may be.
      this._publish(`${base}/metrics/${id}`, JSON.stringify(payload), { qos: 0, retain: true });
    }
    if (s.mqttDiscovery) this._syncDiscovery(entries, base, s.mqttDiscoveryPrefix || DEFAULT_DISCOVERY_PREFIX);
  }

  /**
   * Bring the discovery configs at the broker in line with the fleet. An
   * offline unit keeps its entities (its availability topic says unavailable);
   * a unit removed from the fleet gets its configs emptied. Configs are
   * republished only when a unit's structure changes, so the every-tick
   * publishFleet cost is one small comparison.
   */
  _syncDiscovery(entries, base, discoveryPrefix) {
    if (this._discoveryPrefixInUse !== discoveryPrefix) {
      this._clearDiscovery();
      this._discoveryPrefixInUse = discoveryPrefix;
    }
    const seen = new Set();
    for (const entry of entries) {
      const snap = entry && typeof entry === "object" && entry.snapshot ? entry.snapshot : entry;
      if (!snap?.id) continue;
      seen.add(snap.id);
      if (snap.online !== true) continue;
      const messages = unitDiscoveryMessages(snap, { prefix: base, discoveryPrefix });
      const json = JSON.stringify(messages);
      const cached = this._discoveryByUnit.get(snap.id);
      if (cached?.json === json) continue;
      // Publish only what is new or changed; topics that vanished are removed.
      const cachedByTopic = new Map((cached?.messages ?? []).map((m) => [m.topic, m.payload]));
      for (const m of messages) {
        if (cachedByTopic.get(m.topic) !== m.payload) this._publish(m.topic, m.payload, { retain: true, qos: 1 });
      }
      for (const m of cached?.messages ?? []) {
        if (!messages.some((n) => n.topic === m.topic)) this._removeDiscovery(m.topic);
      }
      this._discoveryByUnit.set(snap.id, { messages, json });
    }
    for (const [id, cached] of this._discoveryByUnit) {
      if (seen.has(id)) continue;
      for (const m of cached.messages) this._removeDiscovery(m.topic);
      this._discoveryByUnit.delete(id);
    }
    if (!this._discoveryAnnounced) {
      this._dashboardDiscovery = dashboardDiscoveryMessages({ prefix: base, discoveryPrefix });
      for (const m of this._dashboardDiscovery) this._publish(m.topic, m.payload, { retain: true, qos: 1 });
      this._discoveryAnnounced = true;
    }
  }

  /** Re-announce cached configs after a reconnect (the broker may have lost them). */
  _republishDiscovery() {
    const s = this._getSettings();
    if (!s.mqttEnabled || !s.mqttDiscovery) return;
    const base = this._prefix();
    const discoveryPrefix = s.mqttDiscoveryPrefix || DEFAULT_DISCOVERY_PREFIX;
    if (this._discoveryPrefixInUse !== discoveryPrefix) {
      this._clearDiscovery();
      this._discoveryPrefixInUse = discoveryPrefix;
    }
    for (const { messages } of this._discoveryByUnit.values()) {
      for (const m of messages) this._publish(m.topic, m.payload, { retain: true, qos: 1 });
    }
    this._dashboardDiscovery = dashboardDiscoveryMessages({ prefix: base, discoveryPrefix });
    for (const m of this._dashboardDiscovery) this._publish(m.topic, m.payload, { retain: true, qos: 1 });
    this._discoveryAnnounced = true;
    // Refresh the state the configs point at with the last known fleet.
    if (this._lastFleet) this.publishFleet(this._lastFleet);
  }

  /** An empty retained payload removes a Home Assistant entity. */
  _removeDiscovery(topic) {
    this._publish(topic, "", { retain: true, qos: 1 });
  }

  _clearDiscovery() {
    for (const { messages } of this._discoveryByUnit.values()) {
      for (const m of messages) this._removeDiscovery(m.topic);
    }
    this._discoveryByUnit.clear();
    if (this._dashboardDiscovery) {
      for (const m of this._dashboardDiscovery) this._removeDiscovery(m.topic);
      this._dashboardDiscovery = null;
    }
    this._discoveryAnnounced = false;
  }

  /** Graceful shutdown: flip our own availability to 0, then force-close. */
  stop() {
    if (this._client?.connected) {
      try {
        this._client.publish(`${this._prefix()}/status/dashboard`, "0", { retain: true });
      } catch {
        /* ignore — the broker will keep the stale "1" until the next boot reconnects */
      }
    }
    this._teardown();
  }

  _publish(topic, payload, opts) {
    // Skipping while disconnected keeps a long broker outage from queueing
    // unbounded messages in the client (QoS 0 would be buffered otherwise).
    if (!this._client?.connected) return this._quietLog("broker not connected; publish skipped");
    try {
      this._client.publish(topic, payload, opts);
    } catch (err) {
      this._quietLog(`publish failed: ${err?.message}`);
    }
  }

  _teardown() {
    const client = this._client;
    this._client = null;
    this._url = null;
    this._base = null;
    this._user = null;
    this._discoveryAnnounced = false;
    if (!client) return;
    try {
      client.end(true); // force: don't wait on in-flight QoS acks during teardown
    } catch {
      /* ignore */
    }
  }

  _prefix() {
    const p = this._getSettings().mqttTopicPrefix;
    return typeof p === "string" && p ? p : DEFAULT_PREFIX;
  }

  /** Rate-limited warn: a down broker must not flood the log every poll tick. */
  _quietLog(message) {
    const now = Date.now();
    if (now - this._lastQuietLog < LOG_QUIET_MS) return;
    this._lastQuietLog = now;
    this._log.warn(`[mqtt] ${message}`);
  }
}

export default MqttPublisher;
