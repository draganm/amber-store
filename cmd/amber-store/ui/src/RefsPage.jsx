import { For, Show, createResource, createSignal } from 'solid-js';
import * as api from './api';
import { UnauthorizedError } from './api';
import { browseHref } from './routes';

const fmtDate = (iso) => (iso ? new Date(iso).toLocaleString() : '');

// The refs listing: every reference on the server, filterable by name.
// Directory refs link into the browser; file refs view/download directly.
export default function RefsPage(props) {
  const [refs] = createResource(async () => {
    try {
      const data = await api.listRefs();
      return data.refs ?? [];
    } catch (err) {
      if (err instanceof UnauthorizedError) props.onSignOut();
      throw err;
    }
  });
  const [filter, setFilter] = createSignal('');
  const shown = () => (refs() ?? []).filter((r) => r.name.includes(filter()));

  return (
    <main class="console container">
      <div class="eyebrow">References</div>
      <h1 class="console__title">Everything this server points at.</h1>
      <p class="console__sub">
        Click a reference to browse its tree, view files, or download
        archives.
      </p>

      <input
        class="input refs-filter"
        placeholder="Filter by name…"
        value={filter()}
        onInput={(e) => setFilter(e.currentTarget.value)}
      />

      <Show
        when={!refs.loading || refs()}
        fallback={<div class="empty">Loading refs…</div>}
      >
        <Show when={!refs.error} fallback={<div class="empty">{String(refs.error?.message || refs.error)}</div>}>
          <div class="refs">
            <For
              each={shown()}
              fallback={<div class="empty">No references.</div>}
            >
              {(ref) => (
                <div class="ref-row">
                  <div class="ref-row__main">
                    <Show
                      when={ref.kind === 'dir'}
                      fallback={<span class="ref-row__name">{ref.name}</span>}
                    >
                      <a class="ref-row__name" href={browseHref(ref.name, '')}>
                        {ref.name}
                      </a>
                    </Show>
                    <span class="badge">{ref.kind}</span>
                    <div class="ref-row__meta">
                      {[ref.user, fmtDate(ref.created_at)].filter(Boolean).join(' · ')}
                    </div>
                  </div>
                  <div class="ref-row__actions">
                    <Show when={ref.kind === 'file'}>
                      <a class="btn btn--ghost" href={api.rawURL(ref.name, '')} target="_blank" rel="noopener">
                        View
                      </a>
                      <a class="btn btn--ghost" href={api.rawURL(ref.name, '', true)}>
                        Download
                      </a>
                    </Show>
                    <Show when={ref.kind === 'dir'}>
                      <a class="btn btn--ghost" href={api.archiveURL(ref.name, '', 'tar')}>
                        .tar
                      </a>
                      <a class="btn btn--ghost" href={api.archiveURL(ref.name, '', 'tgz')}>
                        .tar.gz
                      </a>
                    </Show>
                  </div>
                </div>
              )}
            </For>
          </div>
        </Show>
      </Show>
    </main>
  );
}
