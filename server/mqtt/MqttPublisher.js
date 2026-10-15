import mqtt from "mqtt";
import { unitMetricJson } from "../prometheus.js";

const LOG_QUIET_MS = 30_000;
const DEFAULT_PREFIX = "sparkdash";

/**
 * Outbound MQTT bridge (opt-in, Settings → MQTT): republishes what sparkDash
 * already holds to a user-configured broker —
 *
 *   {prefix}/events            one JSON EventLog record per accepted event
 *   {prefix}/status/{sparkId}  retained "1"/"0", mirroring sparkdash_up
 *   {prefix}/metrics/{sparkId} JSON twin of the unit's Prometheus series
 *   {prefix}/status/dashboard  retained "1" while connected, LWT "0" otherwise
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
  }

  /**
   * Apply current settings: connects when enabled, reopens the connection when
   * the broker URL or topic prefix changed, tears down when disabled. Safe to
   * call on every settings PUT — a no-op when nothing relevant changed.
   */
  applySettings() {
    const s = this._getSettings();
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
    const base = this._prefix();
    for (const entry of entries) {
      const snap = entry && typeof entry === "object" && entry.snapshot ? entry.snapshot : entry;
      if (!snap?.id) continue;
      const id = encodeURIComponent(snap.id);
      const up = snap.online === true;
      this._publish(`${base}/status/${id}`, up ? "1" : "0", { retain: true });
      const json = unitMetricJson(entry);
      const payload = { ts: Date.now(), ...json };
      this._publish(`${base}/metrics/${id}`, JSON.stringify(payload), { qos: 0 });
    }
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
