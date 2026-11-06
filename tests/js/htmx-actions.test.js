// @vitest-environment happy-dom
import { describe, expect, it, vi } from "vitest";

import {
  applyActions,
  errorMessage,
  planActions,
} from "../../src/resonance/static/htmx-actions.js";

const el = (attrs) => {
  const node = document.createElement("button");
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v);
  return node;
};
const ok = { successful: true, xhr: { responseText: "" } };
const failed = (responseText = "") => ({ successful: false, xhr: { responseText } });

describe("planActions", () => {
  it("returns nothing for an element without action attributes", () => {
    expect(planActions(el({ "hx-post": "/x" }), ok)).toEqual([]);
  });

  it("reloads after any request with data-after-request-reload", () => {
    const node = el({ "data-after-request-reload": "" });
    expect(planActions(node, ok)).toEqual([{ type: "reload" }]);
    expect(planActions(node, failed())).toEqual([{ type: "reload" }]);
  });

  it("reloads only on success with data-after-success-reload", () => {
    const node = el({ "data-after-success-reload": "" });
    expect(planActions(node, ok)).toEqual([{ type: "reload" }]);
    expect(planActions(node, failed())).toEqual([]);
  });

  it("redirects only on success with data-after-success-redirect", () => {
    const node = el({ "data-after-success-redirect": "/playlists" });
    expect(planActions(node, ok)).toEqual([{ type: "redirect", url: "/playlists" }]);
    expect(planActions(node, failed())).toEqual([]);
  });

  it("ignores an empty redirect target", () => {
    expect(planActions(el({ "data-after-success-redirect": "" }), ok)).toEqual([]);
  });

  it("refreshes the selector after any request", () => {
    const node = el({ "data-after-request-refresh": "#sync-status" });
    expect(planActions(node, failed())).toEqual([{ type: "refresh", selector: "#sync-status" }]);
  });

  it("refreshes the selector only on success with data-after-success-refresh", () => {
    const node = el({ "data-after-success-refresh": "#sync-status" });
    expect(planActions(node, ok)).toEqual([{ type: "refresh", selector: "#sync-status" }]);
    expect(planActions(node, failed())).toEqual([]);
  });

  it("disables with optional replacement text after any request", () => {
    expect(planActions(el({ "data-after-request-disable": "Syncing..." }), failed())).toEqual([
      { type: "disable", text: "Syncing..." },
    ]);
    expect(planActions(el({ "data-after-request-disable": "" }), ok)).toEqual([
      { type: "disable", text: null },
    ]);
  });

  it("shows an error message only on failure", () => {
    const node = el({
      "data-after-error-show": "#err",
      "data-after-error-fallback": "Upload failed.",
    });
    expect(planActions(node, ok)).toEqual([]);
    expect(planActions(node, failed('{"detail": "Bad CSV"}'))).toEqual([
      { type: "show-error", selector: "#err", message: "Bad CSV" },
    ]);
  });

  it("combines refresh and disable in attribute order of application", () => {
    const node = el({
      "data-after-request-refresh": "#sync-status",
      "data-after-request-disable": "Syncing...",
    });
    expect(planActions(node, ok).map((a) => a.type)).toEqual(["refresh", "disable"]);
  });
});

describe("errorMessage", () => {
  it("uses the JSON detail string when present", () => {
    expect(errorMessage('{"detail": "nope"}', "fallback")).toBe("nope");
  });

  it("falls back on non-JSON, missing, or non-string detail", () => {
    expect(errorMessage("<html>500</html>", "fallback")).toBe("fallback");
    expect(errorMessage('{"other": 1}', "fallback")).toBe("fallback");
    expect(errorMessage('{"detail": [{"msg": "x"}]}', "fallback")).toBe("fallback");
    expect(errorMessage("", "fallback")).toBe("fallback");
  });

  it("defaults the fallback when none is given", () => {
    expect(errorMessage("", null)).toBe("Request failed.");
  });
});

describe("applyActions", () => {
  const env = () => ({
    location: { reload: vi.fn(), assign: vi.fn() },
    htmx: { ajax: vi.fn() },
    document,
  });

  it("reloads and redirects through location", () => {
    const e = env();
    applyActions([{ type: "reload" }, { type: "redirect", url: "/tracks/1" }], el({}), e);
    expect(e.location.reload).toHaveBeenCalledOnce();
    expect(e.location.assign).toHaveBeenCalledWith("/tracks/1");
  });

  it("re-issues the target's hx-get into itself with its hx-swap", () => {
    document.body.innerHTML =
      '<div id="sync-status" hx-get="/partials/sync-status" hx-swap="innerHTML"></div>';
    const e = env();
    applyActions([{ type: "refresh", selector: "#sync-status" }], el({}), e);
    const target = document.getElementById("sync-status");
    expect(e.htmx.ajax).toHaveBeenCalledWith("GET", "/partials/sync-status", {
      target,
      swap: "innerHTML",
    });
  });

  it("skips refresh when the target or its hx-get is missing", () => {
    document.body.innerHTML = '<div id="no-get"></div>';
    const e = env();
    applyActions(
      [
        { type: "refresh", selector: "#missing" },
        { type: "refresh", selector: "#no-get" },
      ],
      el({}),
      e,
    );
    expect(e.htmx.ajax).not.toHaveBeenCalled();
  });

  it("disables the element and replaces its text when given", () => {
    const node = el({});
    node.textContent = "Sync Now";
    applyActions([{ type: "disable", text: "Syncing..." }], node, env());
    expect(node.disabled).toBe(true);
    expect(node.textContent).toBe("Syncing...");

    const keep = el({});
    keep.textContent = "Keep";
    applyActions([{ type: "disable", text: null }], keep, env());
    expect(keep.disabled).toBe(true);
    expect(keep.textContent).toBe("Keep");
  });

  it("writes the error into the target's <mark> and reveals it", () => {
    document.body.innerHTML = '<p id="err" style="display:none"><mark></mark></p>';
    applyActions([{ type: "show-error", selector: "#err", message: "Bad CSV" }], el({}), env());
    const target = document.getElementById("err");
    expect(target.querySelector("mark").textContent).toBe("Bad CSV");
    expect(target.style.display).toBe("block");
  });

  it("writes into the target itself when it has no <mark>", () => {
    document.body.innerHTML = '<p id="err" hidden></p>';
    applyActions([{ type: "show-error", selector: "#err", message: "Oops" }], el({}), env());
    const target = document.getElementById("err");
    expect(target.textContent).toBe("Oops");
    expect(target.hidden).toBe(false);
  });

  it("writes into a [data-error-message] slot, keeping the alert wrapper", () => {
    document.body.innerHTML =
      '<div id="err" hidden><div role="alert"><small data-error-message>old</small></div></div>';
    applyActions([{ type: "show-error", selector: "#err", message: "Nope" }], el({}), env());
    const target = document.getElementById("err");
    expect(target.querySelector("[data-error-message]").textContent).toBe("Nope");
    expect(target.querySelector('[role="alert"]')).not.toBeNull();
    expect(target.hidden).toBe(false);
  });
});

describe("listener", () => {
  it("acts on htmx:afterRequest for the requesting element only", async () => {
    vi.resetModules();
    const assign = vi.fn();
    const { install } = await import("../../src/resonance/static/htmx-actions.js");
    const target = new EventTarget();
    install(target, () => ({ location: { reload: vi.fn(), assign }, htmx: {}, document }));

    const node = el({ "data-after-success-redirect": "/playlists" });
    target.dispatchEvent(new CustomEvent("htmx:afterRequest", { detail: { elt: node, ...ok } }));
    expect(assign).toHaveBeenCalledWith("/playlists");

    const plain = el({});
    target.dispatchEvent(new CustomEvent("htmx:afterRequest", { detail: { elt: plain, ...ok } }));
    expect(assign).toHaveBeenCalledOnce();
  });
});
