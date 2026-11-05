/* Declarative post-request actions for htmx elements. The CSP has no
 * 'unsafe-eval', so hx-on handlers (compiled with Function()) never run; elements
 * declare data-after-* attributes instead and this listener performs them. */

const DEFAULT_ERROR = "Request failed.";

export function errorMessage(responseText, fallback) {
  const fb = fallback || DEFAULT_ERROR;
  try {
    const detail = JSON.parse(responseText).detail;
    return typeof detail === "string" && detail ? detail : fb;
  } catch {
    return fb;
  }
}

export function planActions(elt, detail) {
  const actions = [];
  const attr = (name) => elt.getAttribute(name);
  const has = (name) => elt.hasAttribute(name);
  const ok = detail.successful === true;

  const refresh = attr("data-after-request-refresh");
  if (refresh) actions.push({ type: "refresh", selector: refresh });
  const successRefresh = attr("data-after-success-refresh");
  if (ok && successRefresh) actions.push({ type: "refresh", selector: successRefresh });
  if (has("data-after-request-disable")) {
    actions.push({ type: "disable", text: attr("data-after-request-disable") || null });
  }
  if (has("data-after-request-reload") || (ok && has("data-after-success-reload"))) {
    actions.push({ type: "reload" });
  }
  const redirect = attr("data-after-success-redirect");
  if (ok && redirect) actions.push({ type: "redirect", url: redirect });
  const errorTarget = attr("data-after-error-show");
  if (!ok && errorTarget) {
    actions.push({
      type: "show-error",
      selector: errorTarget,
      message: errorMessage(detail.xhr ? detail.xhr.responseText : "", attr("data-after-error-fallback")),
    });
  }
  return actions;
}

export function applyActions(actions, elt, env) {
  for (const action of actions) {
    if (action.type === "reload") {
      env.location.reload();
    } else if (action.type === "redirect") {
      env.location.assign(action.url);
    } else if (action.type === "refresh") {
      const target = env.document.querySelector(action.selector);
      const url = target && target.getAttribute("hx-get");
      if (url) {
        env.htmx.ajax("GET", url, { target, swap: target.getAttribute("hx-swap") || "innerHTML" });
      }
    } else if (action.type === "disable") {
      if (action.text !== null) elt.textContent = action.text;
      elt.disabled = true;
    } else if (action.type === "show-error") {
      const target = env.document.querySelector(action.selector);
      if (!target) continue;
      const slot =
        target.querySelector("mark") || target.querySelector("[data-error-message]") || target;
      slot.textContent = action.message;
      target.hidden = false;
      target.style.display = "block";
    }
  }
}

export function install(root, getEnv) {
  root.addEventListener("htmx:afterRequest", (event) => {
    const detail = event.detail || {};
    if (!detail.elt || typeof detail.elt.getAttribute !== "function") return;
    const actions = planActions(detail.elt, detail);
    if (actions.length) applyActions(actions, detail.elt, getEnv());
  });
}

if (typeof document !== "undefined") {
  install(document, () => ({ location: window.location, htmx: window.htmx, document }));
}
