// The allowed-keys console: key list, add panel, count footer.
import { For, Show, createResource, createSignal } from 'solid-js';
import * as api from './api';
import { UnauthorizedError } from './api';
import KeyRow from './KeyRow';

export default function KeysPage(props) {
  const [keys, { refetch }] = createResource(async () => {
    try {
      const data = await api.listKeys();
      return data.keys ?? [];
    } catch (err) {
      if (err instanceof UnauthorizedError) props.onSignOut();
      throw err;
    }
  });

  const [line, setLine] = createSignal('');
  const [admin, setAdmin] = createSignal(false);
  const [addError, setAddError] = createSignal('');
  const [busy, setBusy] = createSignal(false);

  const add = async (e) => {
    e.preventDefault();
    if (busy()) return;
    setBusy(true);
    setAddError('');
    try {
      await api.addKey(line().trim(), admin());
      setLine('');
      setAdmin(false);
      refetch();
    } catch (err) {
      if (err instanceof UnauthorizedError) props.onSignOut();
      setAddError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const remove = async (fingerprint) => {
    try {
      await api.removeKey(fingerprint);
      refetch();
    } catch (err) {
      if (err instanceof UnauthorizedError) props.onSignOut();
      else refetch();
    }
  };

  return (
    <>
      <main class="console container">
        <div class="eyebrow">Allowed keys</div>
        <h1 class="console__title">Keys that may talk to this server.</h1>
        <p class="console__sub">
          Changes apply immediately and are written to the server's
          allowed-keys file. Keys marked <code>admin</code> may delete
          references and bypass reference ownership.
        </p>

        <Show
          when={!keys.loading || keys()}
          fallback={<div class="empty">Loading keys…</div>}
        >
          <Show when={!keys.error} fallback={<div class="empty">{String(keys.error?.message || keys.error)}</div>}>
            <div class="keys">
              <For
                each={keys()}
                fallback={
                  <div class="empty">
                    No keys yet. Nothing can talk to this server — add the
                    first key below.
                  </div>
                }
              >
                {(key) => <KeyRow key={key} onRemove={remove} />}
              </For>
            </div>
          </Show>
        </Show>

        <form class="add-panel" onSubmit={add}>
          <h2 class="add-panel__title">Add a key</h2>
          <label class="field-label" for="key-line">
            authorized_keys line
          </label>
          <textarea
            id="key-line"
            class="input input--mono"
            classList={{ 'input--error': !!addError() }}
            placeholder="ssh-ed25519 AAAAC3… backup-host"
            value={line()}
            onInput={(e) => setLine(e.currentTarget.value)}
            rows="3"
          />
          <Show
            when={addError()}
            fallback={
              <div class="help">
                Paste the public key as one line — type, base64 key, and an
                optional comment to name it.
              </div>
            }
          >
            <div class="help help--error">{addError()}</div>
          </Show>
          <div class="add-panel__row">
            <label class="checkbox">
              <input
                type="checkbox"
                checked={admin()}
                onChange={(e) => setAdmin(e.currentTarget.checked)}
              />
              Admin key
              <span class="hint">may delete references</span>
            </label>
            <button
              type="submit"
              class="btn btn--primary"
              disabled={busy() || !line().trim()}
            >
              Add key →
            </button>
          </div>
        </form>
      </main>

      <footer class="console-footer">
        <div class="container console-footer__row">
          <span>AMBER STORE · ADMIN CONSOLE</span>
          <span>{(keys() ?? []).length} KEYS</span>
        </div>
      </footer>
    </>
  );
}
