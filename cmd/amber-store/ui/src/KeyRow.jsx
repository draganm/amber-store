import { Show, createSignal } from 'solid-js';

// One allowed key: comment, type, fingerprint, admin badge, and a
// two-step remove (click → confirm) so a stray click can't drop a key.
export default function KeyRow(props) {
  const [confirming, setConfirming] = createSignal(false);

  return (
    <div class="key-row">
      <div class="key-row__main">
        <div class="key-row__name">
          <Show
            when={props.key.comment}
            fallback={<span class="unnamed">unnamed key</span>}
          >
            {props.key.comment}
          </Show>
          <span class="badge">{props.key.type}</span>
          <Show when={props.key.admin}>
            <span class="badge badge--admin">
              <span class="dot" />
              Admin
            </span>
          </Show>
        </div>
        <div class="key-row__fingerprint" title={props.key.line}>
          {props.key.fingerprint}
        </div>
      </div>
      <div class="key-row__actions">
        <Show
          when={confirming()}
          fallback={
            <button class="btn btn--ghost" onClick={() => setConfirming(true)}>
              Remove
            </button>
          }
        >
          <button class="btn btn--ghost" onClick={() => setConfirming(false)}>
            Keep
          </button>
          <button
            class="btn btn--danger"
            onClick={() => props.onRemove(props.key.fingerprint)}
          >
            Remove key
          </button>
        </Show>
      </div>
    </div>
  );
}
