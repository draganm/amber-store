// Thin client for the admin JSON API. Every helper throws an Error
// with the server's message on a non-2xx response; a 401 throws
// UnauthorizedError so the app can fall back to the login screen.

export class UnauthorizedError extends Error {
  constructor(message) {
    super(message);
    this.name = 'UnauthorizedError';
  }
}

async function call(method, path, body) {
  const res = await fetch(path, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  if (res.ok) {
    return res.status === 204 ? null : res.json();
  }
  let message = `${res.status} ${res.statusText}`;
  try {
    const data = await res.json();
    if (data.error) message = data.error;
  } catch {
    /* non-JSON error body; keep the status line */
  }
  if (res.status === 401) throw new UnauthorizedError(message);
  throw new Error(message);
}

export const checkSession = () => call('GET', '/admin/api/session');
export const login = (password) => call('POST', '/admin/api/login', { password });
export const logout = () => call('POST', '/admin/api/logout');
export const listKeys = () => call('GET', '/admin/api/keys');
export const addKey = (line, admin) => call('POST', '/admin/api/keys', { line, admin });
export const removeKey = (fingerprint) =>
  call('DELETE', `/admin/api/keys?fingerprint=${encodeURIComponent(fingerprint)}`);

export const listRefs = () => call('GET', '/admin/api/refs');
export const listTree = (refName, path, afterRaw) => {
  const q = new URLSearchParams({ ref: refName, path: path || '' });
  let qs = q.toString();
  // afterRaw is the server's pre-encoded `next` cursor (raw entry-name
  // bytes, percent-encoded); append it verbatim — URLSearchParams would
  // double-encode it.
  if (afterRaw) qs += `&after=${afterRaw}`;
  return call('GET', `/admin/api/tree?${qs}`);
};
// rawURL/archiveURL feed plain <a href> links; the session cookie
// authenticates same-site navigations.
export const rawURL = (refName, path, dl) => {
  const q = new URLSearchParams({ ref: refName, path: path || '' });
  if (dl) q.set('dl', '1');
  return `/admin/api/raw?${q}`;
};
export const archiveURL = (refName, path, format) =>
  `/admin/api/archive?${new URLSearchParams({ ref: refName, path: path || '', format })}`;
