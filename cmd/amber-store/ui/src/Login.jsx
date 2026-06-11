import { Show, createSignal } from 'solid-js';
import * as api from './api';
import iconWhite from './assets/Icon_White.png';

// Signed-out state: the signature brand hero — black with teal/blue
// glows and the oversized mark bleeding off the right edge.
export default function Login(props) {
  const [password, setPassword] = createSignal('');
  const [error, setError] = createSignal('');
  const [busy, setBusy] = createSignal(false);

  const submit = async (e) => {
    e.preventDefault();
    if (busy()) return;
    setBusy(true);
    setError('');
    try {
      await api.login(password());
      props.onLogin();
    } catch (err) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div class="login">
      <div class="login__panel">
        <div class="login__eyebrow">Amber store · Admin</div>
        <h1 class="login__title">Keys to the store.</h1>
        <p class="login__sub">
          Sign in to manage which SSH keys may talk to this server.
        </p>
        <form onSubmit={submit}>
          <label class="field-label" for="password">
            Admin password
          </label>
          <input
            id="password"
            type="password"
            class="input input--dark"
            classList={{ 'input--error': !!error() }}
            value={password()}
            onInput={(e) => setPassword(e.currentTarget.value)}
            autofocus
          />
          <Show when={error()}>
            <div class="help help--error login__error">{error()}</div>
          </Show>
          <div class="login__actions">
            <button type="submit" class="btn btn--inverse" disabled={busy()}>
              Sign in →
            </button>
          </div>
        </form>
      </div>
      <div class="login__mark" aria-hidden="true">
        <img src={iconWhite} alt="" />
      </div>
    </div>
  );
}
