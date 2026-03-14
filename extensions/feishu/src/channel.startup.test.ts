import type { OpenClawConfig } from "openclaw/plugin-sdk/feishu";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createStartAccountContext } from "../../test-utils/start-account-context.js";

const hoisted = vi.hoisted(() => ({
  monitorFeishuProvider: vi.fn(),
}));

vi.mock("./monitor.js", async () => {
  const actual = await vi.importActual<typeof import("./monitor.js")>("./monitor.js");
  return {
    ...actual,
    monitorFeishuProvider: hoisted.monitorFeishuProvider,
  };
});

import { feishuPlugin } from "./channel.js";

describe("feishuPlugin gateway.startAccount", () => {
  afterEach(() => {
    vi.clearAllMocks();
  });

  it("forwards status patches from monitor startup into gateway runtime status", async () => {
    const cfg = {
      channels: {
        feishu: {
          enabled: true,
          appId: "cli_test",
          appSecret: "secret_test", // pragma: allowlist secret
          connectionMode: "websocket",
        },
      },
    } as OpenClawConfig;

    const account = feishuPlugin.config.resolveAccount(cfg, "default");
    const patches: Array<Record<string, unknown>> = [];
    hoisted.monitorFeishuProvider.mockImplementationOnce(
      async (opts: { statusSink?: (patch: Record<string, unknown>) => void }) => {
        opts.statusSink?.({
          mode: "websocket",
          connected: true,
          lastConnectedAt: 1_234,
          lastEventAt: 1_234,
          lastError: null,
        });
      },
    );

    const abort = new AbortController();
    const ctx = createStartAccountContext({
      account,
      abortSignal: abort.signal,
      statusPatchSink: (next) => patches.push({ ...next }),
    });
    ctx.cfg = cfg;

    await feishuPlugin.gateway!.startAccount!(ctx);

    expect(hoisted.monitorFeishuProvider).toHaveBeenCalledOnce();
    expect(hoisted.monitorFeishuProvider).toHaveBeenCalledWith(
      expect.objectContaining({
        accountId: "default",
        statusSink: expect.any(Function),
      }),
    );
    expect(patches).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ accountId: "default", port: null }),
        expect.objectContaining({
          accountId: "default",
          mode: "websocket",
          connected: true,
          lastConnectedAt: 1_234,
          lastEventAt: 1_234,
          lastError: null,
        }),
      ]),
    );
  });
});
