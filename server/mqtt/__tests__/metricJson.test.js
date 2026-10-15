/**
 * unitMetricJson is the MQTT twin of the Prometheus export: the same rules
 * (default-zero detection, availability flags, provenance, MiB → bytes,
 * percent → ratio) in a nested JSON shape instead of text series.
 */
import { test } from "node:test";
import { strict as assert } from "node:assert";
import { unitMetricJson } from "../../prometheus.js";

const MIB = 1024 * 1024;

/** An online unit with one healthy GPU and every collector reporting. */
function fullSnapshot(overrides = {}) {
  return {
    id: "alpha",
    name: "Alpha",
    kind: "spark",
    online: true,
    uptime: 105945,
    llmPorts: [8888],
    metrics: {
      gpu: {
        usage: 59.7,
        temperature: 45,
        power: { draw: 18.4, limit: 110 },
        vram: { used: 2048, total: 12288, available: 10240 },
        gpus: [
          {
            index: 0,
            name: "GB10",
            uuid: "GPU-1234",
            usage: 59.7,
            temperature: 45,
            power: { draw: 18.4, limit: 110 },
            vram: { used: 2048, total: 12288 },
          },
        ],
      },
      cpu: { usage: 23.5, temperature: 41, temperatureSource: "acpitz" },
      ram: { used: 8192, total: 32768 },
      unifiedMemory: { total: 12288, available: 10240 },
      network: { interfaces: [{ name: "enP7s7", rxSpeed: 1024, txSpeed: 2048 }] },
      storage: [{ label: "/", device: "/dev/nvme0n1p5", used: 102400, available: 204800, total: 307200 }],
      llm: [
        {
          port: 8888,
          backend: "llama.cpp",
          modelId: "qwen3-32b",
          available: true,
          generationTps: 41.5,
          prefillTps: 1200.25,
          kvCacheUsage: 0.25,
          requestsRunning: 2,
          requestsWaiting: 1,
          totalOutputTokens: 5000,
          totalPromptTokens: 12000,
          totalCachedTokens: 3000,
        },
      ],
    },
    ...overrides,
  };
}

test("a full snapshot maps every reading, with MiB→bytes and percent→ratio conversions", () => {
  const j = unitMetricJson({ snapshot: fullSnapshot(), collected: {}, collectedAt: { gpu: 1690000000000, llm: 1690000001000 } });
  assert.equal(j.up, 1);
  assert.equal(j.uptime_seconds, 105945);
  assert.deepEqual(j.collector_last_success_seconds, { gpu: 1690000000, llm: 1690000001 });
  assert.deepEqual(j.gpu, [
    {
      index: 0,
      name: "GB10",
      uuid: "GPU-1234",
      utilization_ratio: 0.597,
      temperature_celsius: 45,
      power_watts: 18.4,
      power_limit_watts: 110,
      memory_used_bytes: 2048 * MIB,
      memory_total_bytes: 12288 * MIB,
    },
  ]);
  // A non-host unit's headroom is the unified pool's MemAvailable.
  assert.equal(j.memory_available_bytes, 10240 * MIB);
  assert.deepEqual(j.cpu, { utilization_ratio: 0.235, temperature_celsius: 41, temperature_source: "acpitz" });
  assert.deepEqual(j.ram, { used_bytes: 8192 * MIB, total_bytes: 32768 * MIB });
  assert.deepEqual(j.net, [{ interface: "enP7s7", receive_bytes_per_second: 1024, transmit_bytes_per_second: 2048 }]);
  assert.deepEqual(j.disks, [
    { mount: "/", device: "/dev/nvme0n1p5", used_bytes: 102400 * MIB, available_bytes: 204800 * MIB, total_bytes: 307200 * MIB },
  ]);
  assert.deepEqual(j.llm, [
    {
      port: 8888,
      backend: "llama.cpp",
      model: "qwen3-32b",
      up: 1,
      generation_tokens_per_second: 41.5,
      prefill_tokens_per_second: 1200.25,
      kv_cache_usage_ratio: 0.25,
      requests_running: 2,
      requests_waiting: 1,
      generated_tokens_total: 5000,
      prompt_tokens_total: 12000,
      cached_prompt_tokens_total: 3000,
    },
  ]);
});

test("a host unit's headroom is the GPU's free VRAM", () => {
  const snap = fullSnapshot({ kind: "host" });
  snap.metrics.gpu.vram.available = 2048;
  const j = unitMetricJson({ snapshot: snap, collected: {}, collectedAt: {} });
  assert.equal(j.memory_available_bytes, 2048 * MIB);
});

test("a bare snapshot works — provenance just stays unknown", () => {
  const j = unitMetricJson(fullSnapshot());
  assert.equal(j.up, 1);
  assert.equal(j.ram.total_bytes, 32768 * MIB);
  assert.equal(j.collector_last_success_seconds, undefined);
});

test("an offline unit serializes as {up: 0}, as does an id-less entry", () => {
  assert.deepEqual(unitMetricJson({ snapshot: { id: "x", online: false } }), { up: 0 });
  assert.deepEqual(unitMetricJson(null), { up: 0 });
});

test("the zero-filled GPU object of a failed read is not reported as readings", () => {
  const snap = fullSnapshot();
  snap.metrics.gpu.usage = 0;
  snap.metrics.gpu.temperature = 0;
  snap.metrics.gpu.power = { draw: 0 };
  snap.metrics.gpu.vram = { used: 0, total: 0, available: 0 };
  snap.metrics.gpu.gpus = [];
  delete snap.metrics.unifiedMemory;
  const j = unitMetricJson({ snapshot: snap, collected: {}, collectedAt: {} });
  assert.equal(j.gpu, undefined);
  assert.equal(j.memory_available_bytes, undefined);
  assert.equal(j.up, 1); // the unit itself is fine, only the GPU read failed
});

test("provenance and availability flags gate what is emitted", () => {
  // A failed GPU collection (collected.gpu === false) hides even real-looking data.
  let j = unitMetricJson({ snapshot: fullSnapshot(), collected: { gpu: false }, collectedAt: {} });
  assert.equal(j.gpu, undefined);

  // Availability flags drop a single reading without dropping the device.
  const snap = fullSnapshot();
  snap.metrics.gpu.gpus[0].metricAvailability = { temperature: false, powerLimit: false };
  j = unitMetricJson({ snapshot: snap, collected: {}, collectedAt: {} });
  assert.equal(j.gpu[0].temperature_celsius, undefined);
  assert.equal(j.gpu[0].power_limit_watts, undefined);
  assert.equal(j.gpu[0].utilization_ratio, 0.597);
  assert.equal(j.gpu[0].power_watts, 18.4);
});

test("the llm zero-output rule and an unavailable endpoint are honored", () => {
  const snap = fullSnapshot();
  snap.metrics.llm[0].totalOutputTokens = 0;
  let j = unitMetricJson({ snapshot: snap, collected: {}, collectedAt: {} });
  assert.equal(j.llm[0].generated_tokens_total, undefined);
  assert.equal(j.llm[0].prompt_tokens_total, 12000);

  snap.metrics.llm[0].available = false;
  j = unitMetricJson({ snapshot: snap, collected: {}, collectedAt: {} });
  assert.deepEqual(j.llm[0], { port: 8888, backend: "llama.cpp", model: "qwen3-32b", up: 0 });
});

test("throttle reasons serialize as 0/1 flags", () => {
  const snap = fullSnapshot();
  snap.metrics.gpu.gpus[0].throttle = { reason: "active", thermal: true, powerCap: false, hwSlowdown: false };
  const j = unitMetricJson({ snapshot: snap, collected: {}, collectedAt: {} });
  assert.deepEqual(j.gpu[0].throttled, { thermal: 1, power: 0, hw: 0 });
});
