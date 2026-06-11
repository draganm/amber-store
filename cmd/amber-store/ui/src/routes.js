// Hash-route helpers. Every segment is URL-encoded, so a ref name that
// contains '/' travels as a single segment.

export function browseHref(refName, path) {
  let h = `#/refs/${encodeURIComponent(refName)}`;
  if (path) h += '/' + path.split('/').map(encodeURIComponent).join('/');
  return h;
}

// parseRoute reads location.hash: #/keys | #/refs | #/refs/<ref>[/<path>…]
export function parseRoute() {
  // location.hash is percent-decoded by some browsers (Firefox); take
  // the raw fragment from href so encoded slashes in ref names survive.
  const raw = window.location.href.split('#')[1] || '';
  const h = raw.replace(/^\/?/, '');
  if (h === 'refs') return { page: 'refs' };
  if (h.startsWith('refs/')) {
    const segs = h.slice('refs/'.length).split('/').map(decodeURIComponent);
    return { page: 'browser', refName: segs[0], path: segs.slice(1).join('/') };
  }
  return { page: 'keys' };
}
