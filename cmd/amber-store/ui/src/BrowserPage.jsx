import { For, Show, createEffect, createSignal, on } from 'solid-js';
import * as api from './api';
import { UnauthorizedError } from './api';
import { browseHref } from './routes';

function fmtSize(n) {
  if (n === undefined || n === null) return '—';
  let v = n;
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i += 1;
  }
  return i === 0 ? `${v} B` : `${v.toFixed(1)} ${units[i]}`;
}
const fmtMode = (m) => (m & 0o7777).toString(8).padStart(4, '0');
const fmtTime = (ns) => (ns ? new Date(ns / 1e6).toLocaleString() : '—');

// One ref's tree: breadcrumbs, ls -l style listing with cursor
// pagination, per-file view/download, per-directory tar downloads.
export default function BrowserPage(props) {
  const [entries, setEntries] = createSignal([]);
  const [stat, setStat] = createSignal(null); // non-dir target: {kind, stat}
  const [next, setNext] = createSignal(''); // server-encoded raw cursor
  const [more, setMore] = createSignal(false);
  const [loading, setLoading] = createSignal(true);
  const [error, setError] = createSignal('');
  const [pageError, setPageError] = createSignal('');

  // Fix 1: stale-response guard — module-instance request token
  let token = 0;

  const load = async (afterRaw) => {
    const t = ++token;
    setLoading(true);
    setError('');
    setPageError('');
    try {
      const res = await api.listTree(props.refName, props.path, afterRaw);
      if (t !== token) return;
      if (res.kind === 'dir') {
        if (afterRaw) {
          setEntries((prev) => prev.concat(res.entries ?? []));
        } else {
          setEntries(res.entries ?? []);
        }
        setMore(res.more);
        setNext(res.next ?? '');
        setStat(null);
      } else {
        setStat(res);
        setEntries([]);
        setMore(false);
        setNext('');
      }
    } catch (err) {
      if (t !== token) return;
      if (err instanceof UnauthorizedError) props.onSignOut();
      // Fix 3: pagination errors go into pageError, not error
      if (afterRaw) {
        setPageError(err.message);
      } else {
        setError(err.message);
      }
    } finally {
      if (t === token) setLoading(false);
    }
  };

  createEffect(on(
    () => [props.refName, props.path],
    () => {
      setEntries([]);
      setStat(null);
      setNext('');
      setPageError('');
      load();
    },
  ));

  const crumbs = () => {
    const segs = props.path ? props.path.split('/') : [];
    const out = [{ label: props.refName, href: browseHref(props.refName, '') }];
    segs.forEach((seg, i) => out.push({
      label: seg,
      href: browseHref(props.refName, segs.slice(0, i + 1).join('/')),
    }));
    return out;
  };

  const childPath = (name) => (props.path ? `${props.path}/${name}` : name);

  return (
    <main class="console container">
      <div class="eyebrow">Reference browser</div>
      <nav class="crumbs">
        <For each={crumbs()}>
          {(c, i) => (
            <>
              <Show when={i() > 0}>
                <span class="crumbs__sep">/</span>
              </Show>
              <a href={c.href}>{c.label}</a>
            </>
          )}
        </For>
      </nav>

      {/* Fix 5: only show archive buttons once we know target is a dir */}
      <Show when={!stat() && !error() && (!loading() || entries().length > 0)}>
        <div class="browser-actions">
          <a class="btn btn--ghost" href={api.archiveURL(props.refName, props.path, 'tar')}>
            Download .tar
          </a>
          <a class="btn btn--ghost" href={api.archiveURL(props.refName, props.path, 'tgz')}>
            Download .tar.gz
          </a>
        </div>
      </Show>

      <Show when={error()}>
        <div class="empty">{error()}</div>
      </Show>

      <Show when={stat()}>
        <div class="tree">
          <div class="tree-row">
            <span class="tree-row__name">{stat().stat?.name || props.refName}</span>
            <span class="badge">{stat().kind}</span>
            {/* Fix 4: 0-byte files show "0 B" not "—" */}
            <span class="tree-row__meta">{fmtSize(stat().kind === 'file' ? (stat().stat?.size ?? 0) : stat().stat?.size)}</span>
            <span class="tree-row__meta" />
            <span class="tree-row__meta" />
            <span class="tree-row__actions">
              <Show when={stat().kind === 'file'}>
                <a class="btn btn--ghost" href={api.rawURL(props.refName, props.path)} target="_blank" rel="noopener">
                  View
                </a>
                <a class="btn btn--ghost" href={api.rawURL(props.refName, props.path, true)}>
                  Download
                </a>
              </Show>
            </span>
          </div>
        </div>
      </Show>

      <Show when={!stat() && !error()}>
        <div class="tree">
          <div class="tree-head">
            <span>name</span>
            <span>kind</span>
            <span>size</span>
            <span>modified</span>
            <span>mode</span>
            <span />
          </div>
          <For
            each={entries()}
            fallback={
              <Show when={!loading()}>
                <div class="empty">Empty directory.</div>
              </Show>
            }
          >
            {(e) => (
              <div class="tree-row">
                <Show
                  when={e.kind === 'dir' && !e.raw_name_invalid}
                  fallback={
                    <span
                      class="tree-row__name"
                      classList={{ 'tree-row__name--invalid': !!e.raw_name_invalid }}
                      title={e.raw_name_invalid ? 'name is not valid UTF-8; shown lossily' : undefined}
                    >
                      {e.name}
                      <Show when={e.kind === 'symlink'}>
                        <span class="tree-row__target"> → {e.target}</span>
                      </Show>
                    </span>
                  }
                >
                  <a class="tree-row__name" href={browseHref(props.refName, childPath(e.name))}>
                    {e.name}/
                  </a>
                </Show>
                <span class="badge">{e.kind}</span>
                {/* Fix 4: 0-byte files show "0 B" not "—" */}
                <span class="tree-row__meta">{e.kind === 'file' ? fmtSize(e.size ?? 0) : '—'}</span>
                <span class="tree-row__meta">{fmtTime(e.mtime)}</span>
                <span class="tree-row__meta tree-row__mode">{fmtMode(e.mode)}</span>
                <span class="tree-row__actions">
                  <Show when={e.kind === 'file' && !e.raw_name_invalid}>
                    <a class="btn btn--ghost" href={api.rawURL(props.refName, childPath(e.name))} target="_blank" rel="noopener">
                      View
                    </a>
                    <a class="btn btn--ghost" href={api.rawURL(props.refName, childPath(e.name), true)}>
                      Download
                    </a>
                  </Show>
                </span>
              </div>
            )}
          </For>
          <Show when={loading()}>
            <div class="empty">Loading…</div>
          </Show>
          {/* Fix 3: show pagination error inline; keep Load more button so user can retry */}
          <Show when={pageError() && !loading()}>
            <div class="help help--error load-more">{pageError()}</div>
          </Show>
          <Show when={more() && !loading()}>
            <button class="btn btn--ghost load-more" onClick={() => load(next())}>
              Load more
            </button>
          </Show>
        </div>
      </Show>
    </main>
  );
}
