// Hash-route helpers. Every segment is URL-encoded, so a ref name that
// contains '/' travels as a single segment.

export function browseHref(refName, path) {
  let h = `#/refs/${encodeURIComponent(refName)}`;
  if (path) h += '/' + path.split('/').map(encodeURIComponent).join('/');
  return h;
}

// parseRoute reads location.hash: #/keys | #/refs | #/refs/<ref>[/<path>…]
export function parseRoute() {
  const h = window.location.hash.replace(/^#\/?/, '');
  if (h === 'refs') return { page: 'refs' };
  if (h.startsWith('refs/')) {
    const segs = h.slice('refs/'.length).split('/').map(decodeURIComponent);
    return { page: 'browser', refName: segs[0], path: segs.slice(1).join('/') };
  }
  return { page: 'keys' };
}
