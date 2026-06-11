import { Show, createSignal, onCleanup } from 'solid-js';
import * as api from './api';
import logoBlack from './assets/Logo_Black.png';
import { parseRoute } from './routes';
import KeysPage from './KeysPage';
import RefsPage from './RefsPage';
import BrowserPage from './BrowserPage';

// Signed-in shell: sticky header with the Keys/Refs nav, hash-routed
// pages underneath. Routes: #/keys, #/refs, #/refs/<ref>[/<path>…].
export default function Console(props) {
  const [route, setRoute] = createSignal(parseRoute());
  const onHash = () => setRoute(parseRoute());
  window.addEventListener('hashchange', onHash);
  onCleanup(() => window.removeEventListener('hashchange', onHash));

  const signOut = async () => {
    try {
      await api.logout();
    } finally {
      props.onSignOut();
    }
  };

  return (
    <>
      <header class="site-header">
        <div class="container site-header__row">
          <div class="site-header__brand">
            <img src={logoBlack} alt="Fables for Robots" />
            <span class="site-header__app">AMBER STORE · ADMIN</span>
          </div>
          <nav class="site-nav">
            <a
              href="#/keys"
              classList={{ 'site-nav__link': true, 'site-nav__link--active': route().page === 'keys' }}
            >
              Keys
            </a>
            <a
              href="#/refs"
              classList={{ 'site-nav__link': true, 'site-nav__link--active': route().page !== 'keys' }}
            >
              Refs
            </a>
          </nav>
          <button class="btn btn--ghost" onClick={signOut}>
            Sign out
          </button>
        </div>
      </header>

      <Show when={route().page === 'keys'}>
        <KeysPage onSignOut={props.onSignOut} />
      </Show>
      <Show when={route().page === 'refs'}>
        <RefsPage onSignOut={props.onSignOut} />
      </Show>
      <Show when={route().page === 'browser'}>
        <BrowserPage
          refName={route().refName}
          path={route().path}
          onSignOut={props.onSignOut}
        />
      </Show>
    </>
  );
}
